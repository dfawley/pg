use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::once;
use protobuf::{
    AsMut, AsView, ClearAndParse, Message, MessageMut, MutProxied, Proxied, Serialize, View,
};
use std::pin::Pin;
use std::time::Duration;
use std::{fmt::Debug, marker::PhantomData};
use tokio::task;

use crate::grpc::{Args, Callable, Channel, Decoder, Encoder, MethodDescriptor, Status};

pub struct UnaryCall<'a, C, Req, Res, ReqMsg> {
    channel: &'a C,
    desc: &'a MethodDescriptor<ProtoEncoder<Req>, ProtoDecoder<Res>>,
    req: ReqMsg,
    args: Args,
}

impl<'a, C, Req, Res, ReqMsgView> UnaryCall<'a, C, Req, Res, ReqMsgView>
where
    C: Callable,
    Req: Message + 'static,
    Res: Message + 'static,
    ReqMsgView: AsView<Proxied = Req> + 'a,
    for<'b> Req::View<'b>: Send + Serialize,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    pub fn new(
        channel: &'a C,
        desc: &'a MethodDescriptor<ProtoEncoder<Req>, ProtoDecoder<Res>>,
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
        let (tx, rx) = self.channel.call(self.desc, self.args).await;
        tx.send_final_msg(&self.req.as_view()).await;
        rx.next_msg(&mut res.as_mut()).await;
        rx.trailers().await
    }
}

impl<'a, C, Req, Res, ReqMsg> IntoFuture for UnaryCall<'a, C, Req, Res, ReqMsg>
where
    C: Callable,
    Req: Message + 'static,
    Res: Message + 'static,
    ReqMsg: AsView<Proxied = Req> + Send + 'a,
    for<'b> Req::View<'b>: Send + Serialize,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (tx, rx) = self.channel.call(self.desc, self.args).await;

            tx.send_final_msg(&self.req.as_view()).await;
            let mut res = Res::default();
            rx.next_msg(&mut res.as_mut()).await;
            let status = rx.trailers().await;
            if status.code != 0 {
                Err(status)
            } else {
                Ok(res)
            }
        })
    }
}

pub struct BidiCall<'a, C, ReqStream: Stream, Res> {
    channel: &'a C,
    desc: &'a MethodDescriptor<ProtoEncoder<ReqStream::Item>, ProtoDecoder<Res>>,
    req_stream: ReqStream,
    args: Args,
}

impl<'a, C, ReqStream: Stream, Res> BidiCall<'a, C, ReqStream, Res>
where
    ReqStream: Unpin + Stream + Send + 'static,
    ReqStream::Item: Sync + Send + AsView + 'static,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    pub fn new(
        channel: &'a C,
        desc: &'a MethodDescriptor<ProtoEncoder<ReqStream::Item>, ProtoDecoder<Res>>,
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

impl<'a, C, ReqStream: Stream, Res> IntoFuture for BidiCall<'a, C, ReqStream, Res>
where
    C: Callable,
    ReqStream: Unpin + Stream + Send + 'static,
    ReqStream::Item: Message + Send + Sync + 'static,
    for<'b> <ReqStream::Item as Proxied>::View<'b>: Send + Serialize,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
{
    type Output = Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // 1. self is moved into this async block (owning the request data).
            // 2. self.req.as_view() creates a view pointing to that data.
            // 3. The stream consumes that view.
            let (tx, rx) = self.channel.call(self.desc, self.args).await;

            task::spawn(async move {
                while let Some(req) = self.req_stream.next().await {
                    if !tx.send_msg(&req.as_view()).await {
                        return;
                    }
                }
            });
            Box::pin(stream! {
                loop {
                    let mut res = Res::default();
                    if rx.next_msg(&mut res.as_mut()).await {
                        yield Ok(res);
                    } else {
                        yield Err(rx.trailers().await);
                        return;
                    }
                }
            }) as Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send>>
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

    fn encode<'a>(&self, item: &Self::View<'a>) -> Vec<Vec<u8>> {
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

    fn decode<'a>(&self, data: &[&[u8]], item: &mut Self::Mut<'a>) {
        item.clear_and_parse(data[0]).unwrap();
    }
}
