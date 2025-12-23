use async_trait::async_trait;

use super::*;

#[async_trait]
pub(crate) trait DynSendStream<E: Encoder>: Send + Sync + 'static {
    async fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> bool;
    // TODO - This should consume self, but then SendStream::send_and_close
    // can't be impl'd for DynSendStream because would be consumed.
    async fn send_and_close<'a>(&'a mut self, msg: E::View<'a>);
}

pub struct BoxSendStream<E>(pub(crate) Box<dyn DynSendStream<E>>);

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

#[async_trait]
pub(crate) trait DynRecvStream<D: Decoder>: Send + Sync {
    async fn headers(&mut self) -> Option<Headers>;
    async fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> bool;
    async fn trailers(self: Box<Self>) -> Trailers;
}

pub struct BoxRecvStream<D>(pub(crate) Box<dyn DynRecvStream<D>>);

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

#[async_trait]
pub(crate) trait DynCall<E: Encoder, D: Decoder>: Send + Sync {
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

pub struct BoxCall<'a, E: Encoder, D: Decoder>(Box<dyn DynCall<E, D> + 'a>);

impl<'a, E: Encoder, D: Decoder> BoxCall<'a, E, D> {
    pub fn new<T: Call<E, D> + 'a>(call: T) -> Self {
        Self(Box::new(call))
    }
}

impl<'a, E: Encoder, D: Decoder> Call<E, D> for BoxCall<'a, E, D> {
    fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (impl SendStream<E>, impl RecvStream<D>)> + Send {
        self.0.start(descriptor, args)
    }
}
