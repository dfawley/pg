use std::{any::TypeId, fmt::Debug};

mod intercept;
pub use intercept::*;
mod channel;
pub use channel::*;
mod server;
pub use server::*;

#[derive(Clone, Debug)]
pub struct Status {
    pub code: i32,
    pub _msg: String,
}

impl Status {
    pub fn ok() -> Self {
        Status {
            code: 0,
            _msg: String::new(),
        }
    }
    pub fn unknown(msg: impl ToString) -> Self {
        Status {
            code: 2,
            _msg: msg.to_string(),
        }
    }
}

pub struct Metadata;

#[derive(Clone, Debug)]
pub struct Headers {}
#[derive(Clone, Debug)]
pub struct Trailers {
    pub status: Status,
    // TODO: include Metadata.
}

#[allow(unused)]
pub trait SendMessage: Send + Sync {
    fn encode(&self) -> Vec<Vec<u8>>;

    #[doc(hidden)]
    fn _ptr_for(&self, id: TypeId) -> Option<*const ()> {
        None
    }
}

#[allow(unused)]
pub trait RecvMessage: Send + Sync {
    fn decode(&mut self, data: Vec<Vec<u8>>);

    #[doc(hidden)]
    fn _ptr_for(&mut self, id: TypeId) -> Option<*mut ()> {
        None
    }
}

/// A MessageType describes what underlying message is inside a SendMessage or
/// RecvMessage so that it can be downcast, e.g. by interceptors.  It allows for
/// safe downcasting to views containing a lifetime.
pub trait MessageType {
    /// The message view's type, which may have a lifetime.
    type Target<'a>;
    /// The 'static TypeId of the message view.
    fn type_id() -> TypeId;
}

impl dyn SendMessage + '_ {
    /// Downcasts the SendMessage to T::Target if the SendMessage contains a T.
    pub fn downcast_ref<T: MessageType>(&self) -> Option<&T::Target<'_>> {
        if let Some(ptr) = self._ptr_for(T::type_id()) {
            unsafe { Some(&*(ptr as *mut T::Target<'_>)) }
        } else {
            None
        }
    }
}

impl dyn RecvMessage + '_ {
    /// Downcasts the RecvMessage to T::Target if the RecvMessage contains a T.
    pub fn downcast_mut<T: MessageType>(&mut self) -> Option<&mut T::Target<'_>> {
        if let Some(ptr) = self._ptr_for(T::type_id()) {
            unsafe { Some(&mut *(ptr as *mut T::Target<'_>)) }
        } else {
            None
        }
    }
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_and_close closes the send side of the stream.
#[trait_variant::make(Send)]
pub trait ClientSendStream: Send {
    /// Sends msg on the stream.  If Err(()) is returned, the message could not
    /// be delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    async fn send_msg(&mut self, msg: &dyn SendMessage) -> Result<(), ()>;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    async fn send_and_close(self, msg: &dyn SendMessage);
}

/// ResponseStreamItem represents an item in a response stream (either server
/// sending or client receiving).
///
/// A response stream must always contain items exactly as follows:
///
/// [Headers *Message] Trailers *StreamClosed
///
/// That is: optionaly, a Headers value and any number of Message values
/// (including zero), followed by a required Trailers value.  A response stream
/// should never be used after Trailers, but reads should return StreamClosed if
/// it is.
#[derive(Debug)]
pub enum ResponseStreamItem<M> {
    /// Indicates the headers for the stream.
    Headers(Headers),
    /// Indicates a message was received on the stream; the message provided to
    /// RecvStream::next will be populated.
    Message(M),
    /// Indicates trailers were received on the stream and includes the trailers.
    Trailers(Trailers),
    /// Indicates the response stream was closed.  Trailers must have been
    /// provided before this value may be used.
    StreamClosed,
}

pub type ClientResponseStreamItem = ResponseStreamItem<()>;
pub type ServerResponseStreamItem<'a> = ResponseStreamItem<&'a dyn SendMessage>;

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
#[trait_variant::make(Send)]
pub trait ClientRecvStream: Send {
    /// Returns the next item on the response stream, or None if the stream has
    /// finished.  If the item is Message, then RecvMessage has received the
    /// contents of a message.
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem;
}

/// Call begins the dispatching of an RPC.
#[trait_variant::make(Send)]
pub trait Call: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    async fn call(
        &self,
        method: String,
        args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream);
}

/// CallOnce is like Call, but consumes the receiver to limit it to a single
/// call.  This is particularly relevant for pairing with CallInterceptorOnce
/// usages.  It is blanket implemented on Call references.
#[trait_variant::make(Send)]
pub trait CallOnce: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    async fn call_once(
        self,
        method: String,
        args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream);
}

impl<C: Call> CallOnce for &C {
    async fn call_once(
        self,
        method: String,
        args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream) {
        <C as Call>::call(self, method, args).await
    }
}

impl<C: Call> Call for &C {
    async fn call(
        &self,
        method: String,
        args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream) {
        <C as Call>::call(self, method, args).await
    }
}

/// CallInterceptor allows intercepting a call.
#[trait_variant::make(Send)]
pub trait CallInterceptor: Send + Sync {
    /// Starts a call.  Implementations should generally use next to create and
    /// start a call whose streams are optionally wrapped before being returned.
    async fn call(
        &self,
        method: String,
        args: Args,
        next: &impl Call,
    ) -> (impl ClientSendStream, impl ClientRecvStream);
}

/// CallInterceptorOnce allows intercepting a call one time only.
#[trait_variant::make(Send)]
pub trait CallInterceptorOnce: Send + Sync {
    async fn call_once(
        self,
        method: String,
        args: Args,
        next: impl CallOnce,
    ) -> (impl ClientSendStream, impl ClientRecvStream);
}

enum RecvStreamState {
    AwaitingHeaders,
    AwaitingMessagesOrTrailers,
    AwaitingTrailers,
    Done,
}

/// RecvStreamValidator wraps a RecvStream and enforces proper RecvStream
/// semantics on it so that protocol validation does not need to be handled by
/// the consumer.
pub struct RecvStreamValidator<R> {
    recv_stream: R,
    state: RecvStreamState,
    unary_response: bool,
}

impl<R> RecvStreamValidator<R>
where
    R: ClientRecvStream,
{
    pub fn new(recv_stream: R, unary_response: bool) -> Self {
        Self {
            recv_stream,
            state: RecvStreamState::AwaitingHeaders,
            unary_response,
        }
    }

    /// Sets the state to Done and produces a synthesized trailer item
    /// containing the error message.
    fn error(&mut self, s: impl ToString) -> ClientResponseStreamItem {
        self.state = RecvStreamState::Done;
        ResponseStreamItem::Trailers(Trailers {
            status: Status {
                code: 13, // TODO: Internal? TBD
                _msg: s.to_string(),
            },
        })
    }
}

impl<R> ClientRecvStream for RecvStreamValidator<R>
where
    R: ClientRecvStream,
{
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
        // Never call the underlying RecvStream if done.
        if matches!(self.state, RecvStreamState::Done) {
            return ResponseStreamItem::StreamClosed;
        }

        let item = self.recv_stream.next(msg).await;

        match item {
            ResponseStreamItem::Headers(_) => {
                if matches!(self.state, RecvStreamState::AwaitingHeaders) {
                    self.state = RecvStreamState::AwaitingMessagesOrTrailers;
                    item
                } else {
                    self.error("stream received multiple headers")
                }
            }
            ResponseStreamItem::Message(_) => {
                if matches!(self.state, RecvStreamState::AwaitingMessagesOrTrailers) {
                    if self.unary_response {
                        self.state = RecvStreamState::AwaitingTrailers;
                    }
                    item
                } else if matches!(self.state, RecvStreamState::AwaitingTrailers) {
                    self.error("unary stream produced multiple messages")
                } else {
                    self.error("stream produced messages without headers")
                }
            }
            ResponseStreamItem::Trailers(t) => {
                if self.unary_response
                    && matches!(self.state, RecvStreamState::AwaitingMessagesOrTrailers)
                    && t.status.code == 0
                {
                    return self.error("unary stream produced zero messages");
                }
                // Always return a trailers result immediately - it is valid any
                // time but sets the stream's state to Done.
                self.state = RecvStreamState::Done;
                ResponseStreamItem::Trailers(t)
            }
            ResponseStreamItem::StreamClosed => {
                // Trailers were never received or we would be Done.
                self.error("stream ended without trailers")
            }
        }
    }
}
