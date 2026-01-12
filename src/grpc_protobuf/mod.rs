use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, stream::select};
use protobuf::{
    AsMut, AsView, ClearAndParse, Message, MessageMut, MessageView, MutProxied, Proxied, Serialize,
    ViewProxy,
};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use crate::grpc::{
    Args, CallInterceptorOnce, CallOnce, CallOnceExt as _, MethodDescriptor, RecvMessage,
    RecvStream, SendMessage, SendStream, Status,
};

pub struct UnaryCall<'a, C, Res, ReqMsgView> {
    channel: C,
    desc: MethodDescriptor,
    req: ReqMsgView,
    args: Args,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, Res, ReqMsgView> UnaryCall<'a, C, Res, ReqMsgView>
where
    C: CallOnce,
    Res: Message + 'static,
    ReqMsgView: AsView + Send + Sync + 'a,
{
    pub fn new(channel: C, desc: MethodDescriptor, req: ReqMsgView) -> Self {
        Self {
            channel,
            req,
            desc,
            args: Default::default(),
            _phantom: PhantomData,
        }
    }

    pub fn with_interceptor<I: CallInterceptorOnce>(
        self,
        interceptor: I,
    ) -> UnaryCall<'a, impl CallOnce, Res, ReqMsgView> {
        UnaryCall {
            channel: self.channel.with_interceptor(interceptor),
            desc: self.desc,
            req: self.req,
            args: self.args,
            _phantom: PhantomData,
        }
    }

    pub async fn with_response_message<ResMsgMut>(self, res: &mut ResMsgMut) -> Status
    where
        ResMsgMut: AsMut<MutProxied = Res> + Send + Sync,
        for<'b> Res::Mut<'b>: Send + ClearAndParse,
        for<'b> <Res as MutProxied>::Mut<'b>: MessageMut<'b>,
        for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>:
            MessageView<'b> + Send + Sync,
    {
        let (mut tx, mut rx) = self.channel.call(self.desc, self.args).await;
        tx.send_and_close(&ProtoMessageView(self.req.as_view(), PhantomData) as &dyn SendMessage)
            .await;
        rx.next_msg(&mut ProtoMessageMut(res.as_mut()) as &mut dyn RecvMessage)
            .await;
        rx.trailers().await.status
    }
}

impl<'a, C, Res, ReqMsgView> IntoFuture for UnaryCall<'a, C, Res, ReqMsgView>
where
    C: CallOnce + 'a,
    Res: Message + 'static,
    ReqMsgView: AsView + Send + Sync + 'a,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
    for<'b> <Res as MutProxied>::Mut<'b>: MessageMut<'b>,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b> + Send + Sync,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (mut tx, mut rx) = self.channel.call(self.desc, self.args).await;
            tx.send_and_close(
                &ProtoMessageView(self.req.as_view(), PhantomData) as &dyn SendMessage
            )
            .await;

            let mut res = Res::default();
            rx.next_msg(&mut ProtoMessageMut(res.as_mut()) as &mut dyn RecvMessage)
                .await;

            let status = rx.trailers().await.status;
            if status.code != 0 {
                Err(status)
            } else {
                Ok(res)
            }
        })
    }
}

pub struct BidiCall<'a, C, ReqStream: Stream, Res> {
    channel: C,
    desc: MethodDescriptor,
    req_stream: ReqStream,
    args: Args,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, ReqStream: Stream, Res> BidiCall<'a, C, ReqStream, Res> {
    pub fn new(channel: C, desc: MethodDescriptor, req: ReqStream) -> Self {
        Self {
            channel,
            req_stream: req,
            desc,
            args: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<'a, C, ReqStream, Res> IntoFuture for BidiCall<'a, C, ReqStream, Res>
where
    C: CallOnce + 'a,
    ReqStream: Unpin + Stream + Send + 'a,
    ReqStream::Item: Message + Send + Sync + 'static,
    for<'b> <ReqStream::Item as Proxied>::View<'b>: Send + Serialize,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: Send + ClearAndParse,
    for<'b> <<ReqStream as Stream>::Item as Proxied>::View<'b>: MessageView<'b>,
    for<'b> <Res as MutProxied>::Mut<'b>: MessageMut<'b>,
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
                    if !tx.send_msg(&ProtoMessageView(req.as_view(),PhantomData) as &dyn SendMessage).await {
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
                    if rx.next_msg(&mut ProtoMessageMut(res.as_mut()) as &mut dyn RecvMessage).await {
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

impl<'a, C, Res, ReqMsg> private::Sealed for UnaryCall<'a, C, Res, ReqMsg> {}
impl<'a, C, Res, ReqMsg> CallArgs for UnaryCall<'a, C, Res, ReqMsg> {
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

pub struct ProtoMessageView<'a, V>(V, PhantomData<&'a ()>);

impl<'a, V: ViewProxy<'a>> SendMessage for ProtoMessageView<'a, V>
where
    V: Serialize + Send + Sync,
{
    fn encode(&self) -> Vec<Vec<u8>> {
        vec![self.0.serialize().unwrap()]
    }
}

pub struct ProtoMessageMut<M>(pub M);

impl<M> RecvMessage for ProtoMessageMut<M>
where
    M: Send + Sync + ClearAndParse,
{
    fn decode(&mut self, data: Vec<Vec<u8>>) {
        self.0
            .clear_and_parse(data.as_slice()[0].as_slice())
            .unwrap();
    }
}
