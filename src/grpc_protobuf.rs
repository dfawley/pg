use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::once;
use protobuf::{AsMut, AsView, ClearAndParse, Message, MutProxied, Proxied, Serialize};
use std::pin::Pin;
use std::time::Duration;
use std::{fmt::Debug, marker::PhantomData};
use tokio::task;

use crate::grpc::{Args, Channel, Decoder, Encoder, MethodDescriptor, Status};

mod private {
    pub(crate) trait Sealed {}
}

pub struct UnaryCall<'a, Enc: Encoder, Dec: Decoder, Req = <Enc as Encoder>::View<'a>>
where
    Req: AsView<Proxied = Enc::Message>,
{
    channel: &'a Channel,
    desc: &'a MethodDescriptor<'a, Enc, Dec>,
    req: Req,
    args: Args,
}

impl<'a, Enc, Dec, Req> UnaryCall<'a, Enc, Dec, Req>
where
    Enc: Encoder,
    Dec: Decoder,
    Req: AsView<Proxied = Enc::Message>,
    for<'b> Enc::Message: Proxied<View<'b> = Enc::View<'b>>,
{
    pub fn new(channel: &'a Channel, desc: &'a MethodDescriptor<Enc, Dec>, req: Req) -> Self {
        Self {
            channel,
            req,
            desc,
            args: Default::default(),
        }
    }

    pub async fn with_response_message<Msg>(self, res: &mut Msg) -> Status
    where
        Msg: AsMut<MutProxied = Dec::Message>,
        Dec::Message: MutProxied + 'static,
        for<'b> Dec::Message: MutProxied<Mut<'b> = Dec::MutView<'b>>,
    {
        let (tx, rx) = self.channel.call(self.desc, &self.args).await;
        tx.send_final_msg(&self.req.as_view()).await;
        rx.next_msg(&mut res.as_mut()).await;
        rx.trailers().await
    }
}

impl<'a, Enc, Dec, Req> IntoFuture for UnaryCall<'a, Enc, Dec, Req>
where
    Enc: Encoder,
    Dec: Decoder,
    Req: AsView<Proxied = Enc::Message> + Send + 'a,
    for<'b> Enc::Message: Proxied<View<'b> = Enc::View<'b>>,
    for<'b> Dec::Message: MutProxied<Mut<'b> = Dec::MutView<'b>> + Default,
{
    type Output = Result<Dec::Message, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (tx, rx) = self.channel.call(self.desc, &self.args).await;

            tx.send_final_msg(&self.req.as_view()).await;
            let mut res = Dec::Message::default();
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

/*
pub struct BidiCall<'a, Req, Res> {
    channel: &'a Channel,
    req: Req,
    args: Args,
    _d1: PhantomData<Res>,
}

impl<'a, Req, Res> BidiCall<'a, Req, Res>
where
    Req: Unpin + Stream + Send + 'static,
    Req::Item: Sync + Send + AsView + 'a,
{
    pub fn new(channel: &'a Channel, req: Req) -> Self {
        Self {
            channel,
            req,
            args: Default::default(),
            _d1: PhantomData,
        }
    }
}

impl<'a, Req, Res> IntoFuture for BidiCall<'a, Req, Res>
where
    Res: Sync + Send + Debug + Default + 'static,
    Req: Unpin + Stream + Send + 'static,
    Req::Item: Sync + Send + AsView + 'a,
{
    type Output = Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send>>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            // 1. self is moved into this async block (owning the request data).
            // 2. self.req.as_view() creates a view pointing to that data.
            // 3. The stream consumes that view.
            let (tx, rx) = self.channel.call(&self.args).await;

            task::spawn(async move {
                while let Some(req) = self.req.next().await {
                    if !tx.send_msg(req).await {
                        return;
                    }
                }
            });
            Box::pin(stream! {
                loop {
                    let mut res = Res::default();
                    if rx.next_msg(&mut res).await {
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
*/
pub trait CallArgs: private::Sealed {
    fn args_mut(&mut self) -> &mut Args;
}

impl<'a, Enc: Encoder, Dec: Decoder, Req> private::Sealed for UnaryCall<'a, Enc, Dec, Req> where
    Req: AsView<Proxied = Enc::Message>
{
}
impl<'a, Enc: Encoder, Dec: Decoder, Req: AsView<Proxied = Enc::Message>> CallArgs
    for UnaryCall<'a, Enc, Dec, Req>
{
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}
/*
impl<'a, Req, Res> private::Sealed for BidiCall<'a, Req, Res> {}
impl<'a, Req, Res> CallArgs for BidiCall<'a, Req, Res> {
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}
*/
pub trait SharedCall: private::Sealed {
    fn with_timeout(self, timeout: Duration) -> Self;
}

impl<T: CallArgs> SharedCall for T {
    fn with_timeout(mut self, t: Duration) -> Self {
        self.args_mut().timeout = t;
        self
    }
}

pub struct ProtoEncoder<M>(PhantomData<M>);

impl<M> ProtoEncoder<M> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Encoder for ProtoEncoder<M>
where
    M: Proxied + Send + Sync + 'static,
    for<'a> M::View<'a>: Send + Serialize,
{
    type Message = M;
    type View<'a> = M::View<'a>;

    fn encode<'a>(&self, item: &Self::View<'a>) -> Vec<Vec<u8>> {
        vec![item.serialize().unwrap()]
    }
}

pub struct ProtoDecoder<M>(PhantomData<M>);

impl<M> ProtoDecoder<M> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }
}

impl<M> Decoder for ProtoDecoder<M>
where
    M: MutProxied + Send + Sync + 'static,
    for<'a> M::Mut<'a>: Send + ClearAndParse,
{
    type Message = M;
    type MutView<'a> = M::Mut<'a>;

    fn decode<'a>(&self, data: &[&[u8]], item: &mut Self::MutView<'a>) {
        item.clear_and_parse(&data[0]).unwrap();
    }
}
