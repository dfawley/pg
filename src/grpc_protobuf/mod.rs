use async_stream::stream;
use futures_core::Stream;
use futures_util::{StreamExt, stream::select};
use protobuf::{
    AsMut, AsView, ClearAndParse, Message, MessageView, MutProxied, Proxied, Serialize,
};
use std::pin::Pin;
use std::time::Duration;
use std::{any::TypeId, marker::PhantomData};

use crate::grpc::{
    Args, CallInterceptorOnce, CallOnce, CallOnceExt as _, MessageType, RecvMessage, RecvStream,
    RecvStreamItem, SendMessage, SendStream, Status,
};

pub struct UnaryCallBuilder<'a, C, Res, ReqMsgView> {
    channel: C,
    method: String,
    req: ReqMsgView,
    args: Args,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, Res, ReqMsgView> UnaryCallBuilder<'a, C, Res, ReqMsgView>
where
    C: CallOnce,
{
    pub fn new(channel: C, method: String, req: ReqMsgView) -> Self {
        Self {
            channel,
            req,
            method,
            args: Default::default(),
            _phantom: PhantomData,
        }
    }

    pub fn with_interceptor<I: CallInterceptorOnce>(
        self,
        interceptor: I,
    ) -> UnaryCallBuilder<'a, impl CallOnce, Res, ReqMsgView> {
        UnaryCallBuilder {
            channel: self.channel.with_interceptor(interceptor),
            method: self.method,
            req: self.req,
            args: self.args,
            _phantom: PhantomData,
        }
    }

    pub async fn with_response_message(self, res: &mut impl AsMut<MutProxied = Res>) -> Status
    where
        // ReqMsgView is a proto message view. (Ideally we could just require
        // "AsView" and protobuf would automatically include the rest.)
        ReqMsgView: AsView + Send + Sync + 'a,
        <ReqMsgView as AsView>::Proxied: Message + 'static,
        for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
        // Res is a proto message. (Ideally we could just require "Message" and
        // protobuf would automatically include the rest.)
        Res: Message + 'static,
        for<'b> Res::Mut<'b>: ClearAndParse + Send,
    {
        let (tx, mut rx) = self.channel.call_once(self.method, self.args).await;
        let req = &ProtoSendMessage::from_view(&self.req);
        tx.send_and_close(req).await;
        let mut res = ProtoRecvMessage::from_mut(res);
        loop {
            let i = rx.next(&mut res).await;
            if let RecvStreamItem::Trailers(t) = i {
                return t.status;
            }
        }
    }
}

impl<'a, C, Res, ReqMsgView> IntoFuture for UnaryCallBuilder<'a, C, Res, ReqMsgView>
where
    // We can make one call on C.
    C: CallOnce + 'a,
    // ReqMsgView is a proto message view. (Ideally we could just require
    // "AsView" and protobuf would automatically include the rest.)
    ReqMsgView: AsView + Send + Sync + 'a,
    <ReqMsgView as AsView>::Proxied: Message + 'static,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.)
    Res: Message + 'static,
    for<'b> Res::Mut<'b>: ClearAndParse + Send,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let mut res = Res::default();
            let status = self.with_response_message(&mut res).await;
            if status.code == 0 {
                Ok(res)
            } else {
                Err(status)
            }
        })
    }
}

pub struct BidiCallBuilder<'a, C, ReqStream: Stream, Res> {
    channel: C,
    method: String,
    req_stream: ReqStream,
    args: Args,
    _phantom: PhantomData<&'a Res>,
}

impl<'a, C, ReqStream: Stream, Res> BidiCallBuilder<'a, C, ReqStream, Res> {
    pub fn new(channel: C, method: String, req: ReqStream) -> Self {
        Self {
            channel,
            req_stream: req,
            method,
            args: Default::default(),
            _phantom: Default::default(),
        }
    }
}

pub type ResponseStream<'a, Res> = Pin<Box<dyn Stream<Item = Result<Res, Status>> + Send + 'a>>;

impl<'a, C, ReqStream, Res> IntoFuture for BidiCallBuilder<'a, C, ReqStream, Res>
where
    // We can make one call on C.
    C: CallOnce + 'a,
    // ReqStream is a stream of proto messages. (Ideally we could just require
    // "Item: Message" and protobuf would automatically include the rest.)
    ReqStream: Unpin + Stream + Send + 'a,
    ReqStream::Item: Message + 'static,
    for<'b> <ReqStream::Item as Proxied>::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.)
    Res: Message + ClearAndParse + 'static,
    for<'b> Res::Mut<'b>: ClearAndParse + Send,
{
    type Output = ResponseStream<'a, Res>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            let (mut tx, mut rx) = self.channel.call_once(self.method, self.args).await;

            // Create a stream for sending data.  Yields None after every
            // message to cause the receiver stream to be polled.
            let sender = stream! {
                while let Some(req) = self.req_stream.next().await {
                    if tx.send_msg(&ProtoSendMessage::from_view(&req)).await.is_err() {
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
                    let i = rx.next(&mut ProtoRecvMessage::from_mut(&mut res)).await;
                    if let RecvStreamItem::Message = i {
                        yield Ok(res);
                    } else if let RecvStreamItem::Trailers(t) = i {
                        yield Err(t.status);
                        return;
                    }
                }
            }
            .map(Some);

            // Filter out sender stream None values and propagate the receiver
            // stream only.
            Box::pin(select(sender, receiver).filter_map(|item| async move { item }))
                as ResponseStream<'_, Res>
        })
    }
}
mod private {
    pub(crate) trait Sealed {}
}

pub trait CallArgs: private::Sealed {
    fn args_mut(&mut self) -> &mut Args;
}

impl<'a, C, Res, ReqMsg> private::Sealed for UnaryCallBuilder<'a, C, Res, ReqMsg> {}
impl<'a, C, Res, ReqMsg> CallArgs for UnaryCallBuilder<'a, C, Res, ReqMsg> {
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}

impl<'a, C, ReqStream: Stream, Res> private::Sealed for BidiCallBuilder<'a, C, ReqStream, Res> {}
impl<'a, C, ReqStream: Stream, Res> CallArgs for BidiCallBuilder<'a, C, ReqStream, Res> {
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

pub struct ProtoSendMessage<'a, V: Proxied + 'static>(V::View<'a>);

impl<'a, V: Proxied> ProtoSendMessage<'a, V> {
    pub fn from_view(provider: &'a impl AsView<Proxied = V>) -> Self {
        Self(provider.as_view())
    }
}

impl<'a, V> SendMessage for ProtoSendMessage<'a, V>
where
    V: Proxied + 'static,
    V::View<'a>: Serialize + Send + Sync,
{
    fn encode(&self) -> Vec<Vec<u8>> {
        vec![self.0.serialize().unwrap()]
    }

    fn _ptr_for(&self, id: TypeId) -> Option<*const ()> {
        if id != TypeId::of::<V::View<'static>>() {
            return None;
        }
        Some(&self.0 as *const _ as *const ())
    }
}

impl<'a, V: Proxied> MessageType for ProtoSendMessage<'a, V> {
    type Target<'b> = V::View<'b>;
    fn type_id() -> TypeId {
        TypeId::of::<V::View<'static>>()
    }
}

pub struct ProtoRecvMessage<'a, M: MutProxied + 'static>(M::Mut<'a>);

impl<'a, M: MutProxied> ProtoRecvMessage<'a, M> {
    pub fn from_mut(provider: &'a mut impl AsMut<MutProxied = M>) -> Self {
        Self(provider.as_mut())
    }
}

impl<'a, M> RecvMessage for ProtoRecvMessage<'a, M>
where
    M: MutProxied + 'static,
    M::Mut<'a>: Send + Sync + ClearAndParse,
{
    fn decode(&mut self, data: Vec<Vec<u8>>) {
        self.0
            .clear_and_parse(data.as_slice()[0].as_slice())
            .unwrap();
    }

    fn _ptr_for(&mut self, id: TypeId) -> Option<*mut ()> {
        if id != TypeId::of::<M::Mut<'static>>() {
            return None;
        }
        Some(&mut self.0 as *mut _ as *mut ())
    }
}

impl<'a, M: Message> MessageType for ProtoRecvMessage<'a, M> {
    type Target<'b> = M::Mut<'b>;
    fn type_id() -> TypeId {
        TypeId::of::<M::Mut<'static>>()
    }
}
