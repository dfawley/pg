use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, stream::select};
use protobuf::{AsMut, AsView, ClearAndParse, Message, Proxied, Serialize};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;
use tokio::task;

use crate::grpc::{
    Args, Callable, Decoder, Encoder, MethodDescriptor, RecvStream, SendStream, Status,
};

pub struct UnaryCall<'a, C, Req, Res, ReqMsgView> {
    channel: &'a C,
    desc: MethodDescriptor<ProtoEncoder<Req>, ProtoDecoder<Res>>,
    req: ReqMsgView,
    args: Args,
}

impl<'a, C, Req, Res, ReqMsgView> UnaryCall<'a, C, Req, Res, ReqMsgView>
where
    C: Callable,
    Req: Message + 'static,
    Res: Message + 'static,
    ReqMsgView: AsView<Proxied = Req> + Send + 'a,
    for<'b> Req::View<'b>: Send + Serialize,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    pub fn new(
        channel: &'a C,
        desc: MethodDescriptor<ProtoEncoder<Req>, ProtoDecoder<Res>>,
        req: ReqMsgView,
    ) -> Self {
        Self {
            channel,
            req,
            desc,
            args: Default::default(),
        }
    }

    pub async fn with_response_message<ResMsgMut>(self, res: &mut ResMsgMut) -> Status
    where
        ResMsgMut: AsMut<MutProxied = Res>,
    {
        let (tx, mut rx) = self.channel.call(self.desc, self.args).await;
        tx.send_and_close(self.req.as_view()).await;
        rx.next_msg(res.as_mut()).await;
        rx.trailers().await.status
    }

    async fn invoke(self) -> Result<Res, Status> {
        let (tx, mut rx) = self.channel.call(self.desc, self.args).await;

        tx.send_and_close(self.req.as_view()).await;

        let mut res = Res::default();
        rx.next_msg(res.as_mut()).await;

        let status = rx.trailers().await.status;
        if status.code != 0 {
            Err(status)
        } else {
            Ok(res)
        }
    }
}

impl<'a, C, Req, Res, ReqMsgView> IntoFuture for UnaryCall<'a, C, Req, Res, ReqMsgView>
where
    C: Callable,
    Req: Message + 'static,
    Res: Message + 'static,
    ReqMsgView: AsView<Proxied = Req> + Send + 'a,
    for<'b> Req::View<'b>: Send + Serialize,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.invoke())
    }
}

pub struct BidiCall<'a, C, ReqStream: Stream, Res> {
    channel: &'a C,
    desc: MethodDescriptor<ProtoEncoder<ReqStream::Item>, ProtoDecoder<Res>>,
    req_stream: ReqStream,
    args: Args,
}

impl<'a, C, ReqStream: Stream, Res> BidiCall<'a, C, ReqStream, Res> {
    pub fn new(
        channel: &'a C,
        desc: MethodDescriptor<ProtoEncoder<ReqStream::Item>, ProtoDecoder<Res>>,
        req: ReqStream,
    ) -> Self {
        Self {
            channel,
            req_stream: req,
            desc,
            args: Default::default(),
        }
    }
}

impl<'a, C, ReqStream, Res> IntoFuture for BidiCall<'a, C, ReqStream, Res>
where
    C: Callable,
    ReqStream: Unpin + Stream + Send + 'a,
    ReqStream::Item: Message + Send + Sync + 'static,
    for<'b> <ReqStream::Item as Proxied>::View<'b>: Send + Serialize,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    type Output = Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send + 'a>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let (mut tx, mut rx) = self.channel.call(self.desc, self.args).await;

            // Create a stream for sending data.  Yields None after every
            // message to cause the receiver stream to be polled.
            let sender = stream! {
                while let Some(req) = self.req_stream.next().await {
                    if !tx.send_msg(req.as_view()).await {
                        return;
                    }
                    yield None;
                }
            };

            // Create a stream for receiving data.  Yields Ok(response) or
            // Err(trailers).  Wrapped in a Some to be combined with the
            // sender stream.
            let receiver = stream! {
                loop {
                    let mut res = Res::default();
                    if rx.next_msg(res.as_mut()).await {
                        yield Ok(res);
                    } else {
                        yield Err(rx.trailers().await.status);
                        return;
                    }
                }
            }
            .map(Some);

            // Filter out sender stream None values and propagate the receiver
            // stream only.
            Box::pin(select(sender, receiver).filter_map(|item| async move { item }))
                as Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send + 'a>>
        })
    }
}
mod private {
    pub(crate) trait Sealed {}
}

pub trait CallArgs: private::Sealed {
    fn args_mut(&mut self) -> &mut Args;
}

impl<'a, C, Req, Res, ReqMsg> private::Sealed for UnaryCall<'a, C, Req, Res, ReqMsg> {}
impl<'a, C, Req, Res, ReqMsg> CallArgs for UnaryCall<'a, C, Req, Res, ReqMsg> {
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}

impl<'a, C, ReqStream: Stream, Res> private::Sealed for BidiCall<'a, C, ReqStream, Res> {}
impl<'a, C, ReqStream: Stream, Res> CallArgs for BidiCall<'a, C, ReqStream, Res> {
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}

pub trait SharedCall: private::Sealed {
    fn with_timeout(self, timeout: Duration) -> Self;
}

impl<T: CallArgs> SharedCall for T {
    fn with_timeout(mut self, t: Duration) -> Self {
        self.args_mut().timeout = t;
        self
    }
}

#[derive(Clone)]
pub struct ProtoEncoder<M>(PhantomData<M>);

impl<M> ProtoEncoder<M> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Encoder for ProtoEncoder<M>
where
    M: Message + 'static,
    for<'a> M::View<'a>: Send + Serialize,
{
    type View<'a> = M::View<'a>;

    fn encode<'a>(&self, item: Self::View<'a>) -> Vec<Vec<u8>> {
        vec![item.serialize().unwrap()]
    }
}

#[derive(Clone)]
pub struct ProtoDecoder<M>(PhantomData<M>);

impl<M> ProtoDecoder<M> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Decoder for ProtoDecoder<M>
where
    M: Message + 'static,
    for<'a> M::Mut<'a>: Send + ClearAndParse,
{
    type Mut<'a> = M::Mut<'a>;

    fn decode<'a>(&self, data: Vec<Vec<u8>>, mut item: Self::Mut<'a>) {
        item.clear_and_parse(data.as_slice()[0].as_slice()).unwrap();
    }
}
