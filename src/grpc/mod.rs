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
pub trait SendStream: Send {
    /// Sends msg on the stream.  If Err(()) is returned, the message could not
    /// be delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    fn send_msg<'a>(
        &'a mut self,
        msg: &'a dyn SendMessage,
    ) -> impl Future<Output = Result<(), ()>> + Send + 'a;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    // TODO - This should consume self, but that fails without the new trait
    // solver.
    fn send_and_close<'a>(
        &'a mut self,
        msg: &'a dyn SendMessage,
    ) -> impl Future<Output = ()> + Send + 'a;
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
pub trait RecvStream: Send {
    /// Returns the next item on the response stream, or None if the stream has
    /// finished.  If the item is Message, then RecvMessage has received the
    /// contents of a message.
    fn next<'a>(
        &'a mut self,
        msg: &'a mut dyn RecvMessage,
    ) -> impl Future<Output = RecvStreamItem> + Send + 'a;
}

/// Call begins the dispatching of an RPC.
pub trait Call: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    fn call(
        &self,
        method: String,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

/// CallOnce is like Call, but consumes the receiver to limit it to a single
/// call.  This is particularly relevant for pairing with CallInterceptorOnce
/// usages.  It is blanket implemented on Call references.
pub trait CallOnce: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    fn call_once(
        self,
        method: String,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

impl<C: Call> CallOnce for &C {
    fn call_once(
        self,
        method: String,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send {
        <C as Call>::call(self, method, args)
    }
}

impl<C: Call> Call for &C {
    fn call(
        &self,
        method: String,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send {
        <C as Call>::call(self, method, args)
    }
}

/// CallInterceptor allows intercepting a call.
pub trait CallInterceptor: Send + Sync {
    /// Starts a call.  Implementations should generally use next to create and
    /// start a call whose streams are optionally wrapped before being returned.
    fn call(
        &self,
        method: String,
        args: Args,
        next: &impl Call,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

/// CallInterceptorOnce allows intercepting a call one time only.
pub trait CallInterceptorOnce: Send + Sync {
    fn call_once(
        self,
        method: String,
        args: Args,
        next: impl CallOnce,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}
