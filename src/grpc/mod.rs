use std::fmt::Debug;

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

#[derive(Debug)]
#[allow(unused)]
pub enum MethodType {
    Unary,
    ClientStream,
    ServerStream,
    BidiStream,
}

#[derive(Debug)]
pub struct MethodDescriptor {
    pub method_name: String,
    pub method_type: MethodType,
}

#[allow(unused)]
pub trait SendMessage: Send + Sync {
    fn encode(&self) -> Vec<Vec<u8>>;
}

#[allow(unused)]
pub trait RecvMessage: Send + Sync {
    fn decode(&mut self, data: Vec<Vec<u8>>);
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_and_close results in a signal the server can use
/// to determine the client is done sending requests.
pub trait SendStream: Send + Sync + 'static {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    fn send_msg<'a>(
        &'a mut self,
        msg: &'a dyn SendMessage,
    ) -> impl Future<Output = bool> + Send + 'a;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    // TODO - This should consume self, but that fails without the new trait
    // solver.
    fn send_and_close<'a>(
        &'a mut self,
        msg: &'a dyn SendMessage,
    ) -> impl Future<Output = ()> + Send + 'a;
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
pub trait RecvStream: Send + Sync + 'static {
    /// Returns the response stream's headers, or None if a trailers-only
    /// response is received.
    fn headers(&mut self) -> impl Future<Output = Option<Headers>> + Send;

    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
    fn next_msg<'a>(
        &'a mut self,
        msg: &'a mut dyn RecvMessage,
    ) -> impl Future<Output = bool> + Send + 'a;

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    fn trailers(self) -> impl Future<Output = Trailers> + Send;
}

/// Call begins the dispatching of an RPC.
pub trait Call: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    fn call(
        &self,
        descriptor: MethodDescriptor,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

/// CallOnce is like Call, but consumes the receiver to limit it to a single
/// call.  This is particularly relevant for pairing with CallInterceptorOnce
/// usages.  It is blanket implemented on Callable references.
pub trait CallOnce: Send + Sync {
    /// Starts a call, returning the send and receive streams to interact with
    /// the RPC.  The returned future may block until sufficient resources are
    /// available to allow the call to start.
    fn call(
        self,
        descriptor: MethodDescriptor,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

impl<C: Call> CallOnce for &C {
    fn call(
        self,
        descriptor: MethodDescriptor,
        args: Args,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send {
        <C as Call>::call(self, descriptor, args)
    }
}

/// CallInterceptor allows intercepting a call.
pub trait CallInterceptor: Send + Sync {
    /// Starts a call.  Implementations should generally use next to create and
    /// start a call whose streams are optionally wrapped before being returned.
    fn call<C: CallOnce>(
        &self,
        descriptor: MethodDescriptor,
        args: Args,
        next: C,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

/// CallInterceptorOnce allows intercepting a call one time only.
pub trait CallInterceptorOnce: Send + Sync {
    fn call<C: CallOnce>(
        self,
        descriptor: MethodDescriptor,
        args: Args,
        next: C,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send;
}

impl<CI: CallInterceptor> CallInterceptorOnce for &CI {
    fn call<C: CallOnce>(
        self,
        descriptor: MethodDescriptor,
        args: Args,
        next: C,
    ) -> impl Future<Output = (impl SendStream, impl RecvStream)> + Send {
        <CI as CallInterceptor>::call(self, descriptor, args, next)
    }
}
