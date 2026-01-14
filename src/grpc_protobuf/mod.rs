use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, stream::select};
use protobuf::{
    AsMut, AsView, ClearAndParse, Message, MessageMut, MessageView, MutProxied, MutProxy, Proxied,
    Serialize,
};
use std::pin::Pin;
use std::time::Duration;
use std::{any::TypeId, marker::PhantomData};

use crate::grpc::{
    Args, CallInterceptorOnce, CallOnce, CallOnceExt as _, MethodDescriptor, RecvMessage,
    RecvStream, RecvStreamItem, SendMessage, SendStream, Status,
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
        ResMsgMut: AsMut<MutProxied = Res>,
        for<'b> Res::Mut<'b>: ClearAndParse + Send,
        <ReqMsgView as AsView>::Proxied: Message + 'static,
        for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
    {
        let (mut tx, mut rx) = self.channel.call(self.desc, self.args).await;
        let v: &(dyn SendMessage + '_) =
            &ProtoMessageView::<ReqMsgView::Proxied>(self.req.as_view(), PhantomData);
        tx.send_and_close(v).await;
        let mut msg = ProtoMessageMut::<Res>(res.as_mut(), PhantomData);
        loop {
            let i = rx.next(&mut msg).await.unwrap();
            if let RecvStreamItem::Trailers(t) = i {
                return t.status;
            }
        }
    }
}

impl<'a, C, Res, ReqMsgView> IntoFuture for UnaryCall<'a, C, Res, ReqMsgView>
where
    C: CallOnce + 'a,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: ClearAndParse + Send,
    ReqMsgView: AsView + Send + Sync + 'a,
    <ReqMsgView as AsView>::Proxied: Message + 'static,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let (mut tx, mut rx) = self.channel.call(self.desc, self.args).await;
            tx.send_and_close(&ProtoMessageView::<ReqMsgView::Proxied>(
                self.req.as_view(),
                PhantomData,
            ))
            .await;

            let mut res = Res::default();
            let mut msg = ProtoMessageMut::<Res>(res.as_mut(), PhantomData);
            loop {
                let i = rx.next(&mut msg).await.unwrap();
                if let RecvStreamItem::Trailers(t) = i {
                    if t.status.code == 0 {
                        drop(msg);
                        return Ok(res);
                    }
                    return Err(t.status);
                }
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
    ReqStream::Item: Message + 'static,
    for<'b> <ReqStream::Item as Proxied>::View<'b>: MessageView<'b>,
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: Send + MessageMut<'b>,
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
                    if !tx.send_msg(&ProtoMessageView::<ReqStream::Item>(req.as_view(), PhantomData)).await {
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
                    let i = rx.next(&mut ProtoMessageMut::<Res>(res.as_mut(), PhantomData)).await;
                    if let Some(RecvStreamItem::Message) = i {
                        yield Ok(res);
                    } else if let Some(RecvStreamItem::Trailers(t)) = i {
                        yield Err(t.status);
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

pub struct ProtoMessageView<'a, M: Message + 'static>(
    pub <M as Proxied>::View<'a>,
    pub PhantomData<&'a ()>,
);

impl<'a, M> SendMessage for ProtoMessageView<'a, M>
where
    M: Message + 'static,
    <M as Proxied>::View<'a>: Serialize + Send + Sync,
{
    fn encode(&self) -> Vec<Vec<u8>> {
        vec![self.0.serialize().unwrap()]
    }

    fn msg_ptr(&self) -> *const () {
        &self.0 as *const _ as *const ()
    }

    fn msg_type_id(&self) -> TypeId {
        TypeId::of::<M::View<'static>>()
    }
}

pub struct ProtoMessageMut<'a, M: Message + 'static>(
    pub <M as MutProxied>::Mut<'a>,
    pub PhantomData<&'a ()>,
);

impl<'a, M> RecvMessage for ProtoMessageMut<'a, M>
where
    M: MutProxied + Message + 'static,
    <M as MutProxied>::Mut<'a>: MutProxy<'a> + Send + Sync + ClearAndParse,
{
    fn decode(&mut self, data: Vec<Vec<u8>>) {
        self.0
            .clear_and_parse(data.as_slice()[0].as_slice())
            .unwrap();
    }

    fn msg_ptr(&self) -> *const () {
        &self.0 as *const _ as *const ()
    }

    fn msg_type_id(&self) -> TypeId {
        TypeId::of::<M::Mut<'static>>()
    }
}

pub fn downcast_proto_view<'a, T>(msg: &'a dyn SendMessage) -> Option<&'a T::View<'a>>
where
    T: Message + 'static,
{
    if msg.msg_type_id() == TypeId::of::<T::View<'static>>() {
        let inner_msg = unsafe { &*(msg.msg_ptr() as *const T::View<'a>) };
        Some(inner_msg)
    } else {
        None
    }
}

pub fn downcast_proto_mut<'a, T>(msg: &'a mut dyn RecvMessage) -> Option<&'a mut T::Mut<'a>>
where
    T: Message + 'static,
{
    if msg.msg_type_id() == TypeId::of::<T::Mut<'static>>() {
        let inner_msg = unsafe { &mut *(msg.msg_ptr() as *mut T::Mut<'a>) };
        Some(inner_msg)
    } else {
        None
    }
}
