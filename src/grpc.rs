use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time;

use async_trait::async_trait;

use crate::gencode::pb::*;

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

#[async_trait]
trait DynSendStream<E: Encoder>: Send + Sync + 'static {
    async fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> bool;
    // TODO - This should consume self, but then SendStream::send_and_close
    // can't be impl'd for DynSendStream because would be consumed.
    async fn send_and_close<'a>(&'a mut self, msg: E::View<'a>);
}

pub struct BoxSendStream<E>(Box<dyn DynSendStream<E>>);

#[async_trait]
impl<E: Encoder, T: SendStream<E>> DynSendStream<E> for T {
    async fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> bool {
        SendStream::send_msg(self, msg).await
    }
    async fn send_and_close<'a>(&'a mut self, msg: E::View<'a>) {
        SendStream::send_and_close(self, msg).await;
    }
}

impl<E: Encoder> SendStream<E> for BoxSendStream<E> {
    fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> impl Future<Output = bool> + Send + 'a {
        self.0.send_msg(msg)
    }
    fn send_and_close<'a>(&'a mut self, msg: E::View<'a>) -> impl Future<Output = ()> + Send + 'a {
        self.0.send_and_close(msg)
    }
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

#[async_trait]
trait DynRecvStream<D: Decoder>: Send + Sync {
    async fn headers(&mut self) -> Option<Headers>;
    async fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> bool;
    async fn trailers(self: Box<Self>) -> Trailers;
}

pub struct BoxRecvStream<D>(Box<dyn DynRecvStream<D>>);

#[async_trait]
impl<D: Decoder, T: RecvStream<D>> DynRecvStream<D> for T {
    async fn headers(&mut self) -> Option<Headers> {
        RecvStream::headers(self).await
    }
    async fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> bool {
        RecvStream::next_msg(self, msg).await
    }
    async fn trailers(self: Box<Self>) -> Trailers {
        RecvStream::trailers(*self).await
    }
}

impl<D: Decoder> RecvStream<D> for BoxRecvStream<D> {
    fn headers(&mut self) -> impl Future<Output = Option<Headers>> + Send {
        self.0.headers()
    }

    fn next_msg<'a>(
        &'a mut self,
        msg: <D as Decoder>::Mut<'a>,
    ) -> impl Future<Output = bool> + Send + 'a {
        self.0.next_msg(msg)
    }

    fn trailers(self) -> impl Future<Output = Trailers> + Send {
        Box::new(self.0).trailers()
    }
}

pub trait Callable: Send + Sync {
    fn call<E: Encoder, D: Decoder>(&self) -> impl Call<E, D>;
}

pub trait Call<E: Encoder, D: Decoder>: Send + Sync {
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send;
}

pub trait CallInterceptor: Send + Sync {
    fn start<C: Callable, E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
        next: &C,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send;
}

#[derive(Clone)]
pub struct Interceptor<C: Clone, I: Clone> {
    callable: C,
    call_interceptor: I,
}

impl<C: Clone, I: Clone> Interceptor<C, I> {
    pub fn new(callable: C, call_interceptor: I) -> Self {
        Self {
            callable,
            call_interceptor,
        }
    }
}

struct InterceptorCall<'a, C: Clone, I: Clone> {
    interceptor: &'a Interceptor<C, I>,
}

impl<'a, C: Callable + Clone, I: CallInterceptor + Clone, E: Encoder, D: Decoder> Call<E, D>
    for InterceptorCall<'a, C, I>
{
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.interceptor
            .call_interceptor
            .start(descriptor, args, &self.interceptor.callable)
    }
}

impl<C: Callable + Clone, I: CallInterceptor + Clone> Callable for Interceptor<C, I> {
    fn call<E: Encoder, D: Decoder>(&self) -> impl Call<E, D> {
        InterceptorCall { interceptor: self }
    }
}

#[async_trait]
trait DynCall<E: Encoder, D: Decoder>: Send + Sync {
    async fn start(
        self: Box<Self>,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> (BoxSendStream<E>, BoxRecvStream<D>);
}

#[async_trait]
impl<T: Call<E, D>, E: Encoder, D: Decoder> DynCall<E, D> for T {
    async fn start(
        self: Box<Self>,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> (BoxSendStream<E>, BoxRecvStream<D>) {
        let (tx, rx) = (*self).start(descriptor, args).await;
        (BoxSendStream(Box::new(tx)), BoxRecvStream(Box::new(rx)))
    }
}

pub struct BoxCall<E: Encoder, D: Decoder>(Box<dyn DynCall<E, D>>);

impl<E: Encoder, D: Decoder> Call<E, D> for BoxCall<E, D> {
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.0.start(descriptor, args)
    }
}

pub struct ChannelCall;

impl<E: Encoder, D: Decoder> Call<E, D> for ChannelCall {
    async fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        _args: Args,
    ) -> (impl SendStream<E>, impl RecvStream<D>) {
        println!(
            "starting call for {:?} ({:?})",
            descriptor.method_name, descriptor.method_type
        );
        (
            ChannelSendStream {
                _e: descriptor.message_encoder.clone(),
            },
            ChannelRecvStream {
                _d: descriptor.message_decoder.clone(),
                cnt: Mutex::new(0),
            },
        )
    }
}

impl Callable for Channel {
    fn call<E: Encoder, D: Decoder>(&self) -> impl Call<E, D> {
        ChannelCall {}
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

pub struct ChannelSendStream<T> {
    _e: T,
}

impl<T: Encoder> SendStream<T> for ChannelSendStream<T> {
    async fn send_msg<'a>(&'a mut self, _msg: T::View<'a>) -> bool {
        true // false on error sending
    }
    async fn send_and_close<'a>(&'a mut self, _msg: T::View<'a>) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream<T> {
    cnt: Mutex<i32>,
    _d: T,
}

impl<T: Decoder> RecvStream<T> for ChannelRecvStream<T> {
    async fn headers(&mut self) -> Option<Headers> {
        Some(Headers {})
    }

    async fn next_msg<'a>(&'a mut self, mut msg: T::Mut<'a>) -> bool {
        let mut cnt = self.cnt.lock().unwrap();
        let msg: &mut MyResponseMut =
            unsafe { &mut *(&mut msg as *mut T::Mut<'_> as *mut MyResponseMut) };
        if *cnt == 3 {
            return false;
        }
        *cnt += 1;
        msg.set_result(*cnt);
        true
    }

    async fn trailers(self) -> Trailers {
        Trailers {
            status: Status::ok(),
        }
    }
}
