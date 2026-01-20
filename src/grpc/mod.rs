use std::{any::TypeId, fmt::Debug};

mod intercept;
pub use intercept::*;
mod channel;
pub use channel::*;

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
}

#[derive(Clone, Debug)]
pub struct Headers {}
pub struct Trailers {
    pub status: Status,
}

#[derive(Clone, Default)]
pub struct Channel;

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
pub trait SendStream: Send {
    /// Sends msg on the stream.  If Err(()) is returned, the message could not
    /// be delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    async fn send_msg(&mut self, msg: &dyn SendMessage) -> Result<(), ()>;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    async fn send_and_close(self, msg: &dyn SendMessage);
}

pub enum RecvStreamItem {
    Headers(Headers),
    Message,
    Trailers(Trailers),
    StreamClosed,
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
///
/// A RecvStream should always return the items exactly as follows:
///
/// [Headers *Message] Trailers *StreamClosed
///
/// That is: optionaly, a Headers value and any number of Message values
/// (including zero), followed by a required Trailers value.  A RecvStream
/// should not be used after next has returned Trailers, but it should return
/// StreamClosed if it is.
#[trait_variant::make(Send)]
pub trait RecvStream: Send {
    /// Returns the next item on the response stream, or None if the stream has
    /// finished.  If the item is Message, then RecvMessage has received the
    /// contents of a message.
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> RecvStreamItem;
}

/// Call begins the dispatching of an RPC.
#[trait_variant::make(Send)]
pub trait Call: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    async fn call(&self, method: String, args: Args) -> (impl SendStream, impl RecvStream);
}

/// CallOnce is like Call, but consumes the receiver to limit it to a single
/// call.  This is particularly relevant for pairing with CallInterceptorOnce
/// usages.  It is blanket implemented on Call references.
#[trait_variant::make(Send)]
pub trait CallOnce: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    async fn call_once(self, method: String, args: Args) -> (impl SendStream, impl RecvStream);
}

impl<C: Call> CallOnce for &C {
    async fn call_once(self, method: String, args: Args) -> (impl SendStream, impl RecvStream) {
        <C as Call>::call(self, method, args).await
    }
}

impl<C: Call> Call for &C {
    async fn call(&self, method: String, args: Args) -> (impl SendStream, impl RecvStream) {
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
    ) -> (impl SendStream, impl RecvStream);
}

/// CallInterceptorOnce allows intercepting a call one time only.
#[trait_variant::make(Send)]
pub trait CallInterceptorOnce: Send + Sync {
    async fn call_once(
        self,
        method: String,
        args: Args,
        next: impl CallOnce,
    ) -> (impl SendStream, impl RecvStream);
}
