use std::any::Any;
use std::fmt::Debug;

mod dynadapt;
mod intercept;
pub use dynadapt::*;
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
pub enum MethodType {
    Unary,
    ClientStream,
    ServerStream,
    BidiStream,
}

pub struct MethodDescriptor<E, D> {
    pub method_name: String,
    pub message_encoder: E,
    pub message_decoder: D,
    pub method_type: MethodType,
}

pub trait Encoder: Any + Send + Sync + Clone + 'static {
    type View<'a>: Send + Sync;

    fn encode(&self, item: Self::View<'_>) -> Vec<Vec<u8>>;
}

pub trait Decoder: Any + Send + Sync + Clone + 'static {
    type Mut<'a>: Send + Sync;

    fn decode(&self, data: Vec<Vec<u8>>, item: Self::Mut<'_>);
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_final_message results in a signal the server can
/// use to determine the client is done sending.
pub trait SendStream<E: Encoder>: Send + Sync + 'static {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> impl Future<Output = bool> + Send + 'a;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    // TODO - This should consume self, but that fails without the new trait
    // solver.  Also it's not dyn compatible if it takes self.
    fn send_and_close<'a>(&'a mut self, msg: E::View<'a>) -> impl Future<Output = ()> + Send + 'a;
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
pub trait RecvStream<D: Decoder>: Send + Sync + 'static {
    /// Returns the response stream's headers, or None if a trailers-only
    /// response is received.
    fn headers(&mut self) -> impl Future<Output = Option<Headers>> + Send;

    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
    fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> impl Future<Output = bool> + Send + 'a;

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    fn trailers(self) -> impl Future<Output = Trailers> + Send;
}

pub trait Callable: Send + Sync {
    fn call<'a, E: Encoder, D: Decoder>(&'a self) -> impl Call<E, D> + 'a;
}

pub trait CallableOnce: Send + Sync {
    fn call<E: Encoder, D: Decoder>(self) -> impl Call<E, D>;
}

pub trait Call<E: Encoder, D: Decoder>: Send + Sync {
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send;
}

impl<'a, C: Callable> CallableOnce for &'a C {
    fn call<E: Encoder, D: Decoder>(self) -> impl Call<E, D> {
        <C as Callable>::call(self)
    }
}

pub trait CallInterceptor: Send + Sync {
    fn start<C: CallableOnce, E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
        next: C,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send;
}

pub trait CallInterceptorOnce: Send + Sync {
    fn start<C: CallableOnce, E: Encoder, D: Decoder>(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
        next: C,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send;
}
