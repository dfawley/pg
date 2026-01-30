use futures::Sink;
use futures::sink::unfold;
use protobuf::{
    AsMut, AsView, ClearAndParse, Message, MessageMut, MessageView, MutProxied, Proxied, Serialize,
};
use std::pin::Pin;
use std::time::Duration;
use std::{any::TypeId, marker::PhantomData};

use crate::grpc::{
    Args, CallInterceptorOnce, CallOnce, CallOnceExt as _, ClientRecvStream,
    ClientResponseStreamItem, ClientSendStream, MessageType, RecvMessage, RecvStreamValidator,
    ResponseStreamItem, SendMessage, SendMsgOptions, Status,
};

mod server;
pub use server::*;

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
    pub fn new(channel: C, method: impl ToString, req: ReqMsgView) -> Self {
        Self {
            channel,
            req,
            method: method.to_string(),
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
        <ReqMsgView as AsView>::Proxied: Message,
        for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
        // Res is a proto message. (Ideally we could just require "Message" and
        // protobuf would automatically include the rest.)
        Res: Message,
        for<'b> Res::Mut<'b>: ClearAndParse + Send + Sync,
    {
        let (mut tx, rx) = self.channel.call_once(self.method, self.args);
        let mut rx = RecvStreamValidator::new(rx, true);
        let req = &ProtoSendMessage::from_view(&self.req);
        let _ = tx
            .send_msg(
                req,
                SendMsgOptions {
                    final_msg: true,
                    ..Default::default()
                },
            )
            .await;
        let mut res = ProtoRecvMessage::from_mut(res);
        loop {
            let i = rx.next(&mut res).await;
            if let ResponseStreamItem::Trailers(t) = i {
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
    <ReqMsgView as AsView>::Proxied: Message,
    for<'b> <<ReqMsgView as AsView>::Proxied as Proxied>::View<'b>: MessageView<'b>,
    // Res is a proto message. (Ideally we could just require "Message" and
    // protobuf would automatically include the rest.)
    Res: Message,
    for<'b> Res::Mut<'b>: ClearAndParse + Send + Sync,
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

pub struct GrpcStreamingResponse<M, Rx> {
    rx: RecvStreamValidator<Rx>,
    status: Option<Status>,
    _phantom: PhantomData<M>,
}

impl<M, Rx> GrpcStreamingResponse<M, Rx>
where
    Rx: ClientRecvStream,
    M: Message,
    for<'b> M::Mut<'b>: MessageMut<'b>,
{
    fn new(rx: RecvStreamValidator<Rx>) -> Self {
        Self {
            rx,
            status: None,
            _phantom: PhantomData,
        }
    }

    pub async fn next(&mut self) -> Option<M> {
        let mut res = M::default();
        let mut res_view = ProtoRecvMessage::from_mut(&mut res);
        loop {
            let i = self.rx.next(&mut res_view).await;
            match i {
                ResponseStreamItem::Headers(_) => continue,
                ResponseStreamItem::Message(_) => {
                    drop(res_view);
                    return Some(res);
                }
                ResponseStreamItem::Trailers(trailers) => {
                    self.status = Some(trailers.status);
                    return None;
                }
                ResponseStreamItem::StreamClosed => return None,
            }
        }
    }

    pub async fn status(mut self) -> Status {
        if let Some(status) = self.status.take() {
            // We encountered a status while handling a call to next.
            status
        } else {
            // Drain the stream until we find trailers.
            let mut nop_msg = NopRecvMessage;
            loop {
                let i = self.rx.next(&mut nop_msg).await;
                if let ClientResponseStreamItem::Trailers(t) = i {
                    return t.status;
                }
            }
        }
    }
}

struct NopRecvMessage;

impl RecvMessage for NopRecvMessage {
    fn decode(&mut self, _data: Vec<Vec<u8>>) {}
}

pub struct BidiCallBuilder<C, Req, Res> {
    channel: C,
    method: String,
    args: Args,
    _phantom: PhantomData<(Req, Res)>,
}

impl<C, Req, Res> BidiCallBuilder<C, Req, Res> {
    pub fn new(channel: C, method: impl ToString) -> Self {
        Self {
            channel,
            method: method.to_string(),
            args: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<C, Req, Res> BidiCallBuilder<C, Req, Res>
where
    // We can make one call on C.
    C: CallOnce,
    Req: Message,
    for<'b> Req::View<'b>: MessageView<'b>,
    Res: Message + ClearAndParse,
    for<'b> Res::Mut<'b>: MessageMut<'b>,
{
    pub fn start(
        self,
    ) -> (
        impl Sink<Req, Error = ()>,
        GrpcStreamingResponse<Res, impl ClientRecvStream>,
    ) {
        let (tx, rx) = self.channel.call_once(self.method, self.args);
        let rx = RecvStreamValidator::new(rx, false);
        let req_sink = unfold(tx, |mut tx, req| async move {
            tx.send_msg(
                &ProtoSendMessage::from_view(&req),
                SendMsgOptions::default(),
            )
            .await?;
            Ok(tx)
        });
        let res_stream = GrpcStreamingResponse::new(rx);
        (req_sink, res_stream)
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

impl<C, Req, Res> private::Sealed for BidiCallBuilder<C, Req, Res> {}
impl<C, Req, Res> CallArgs for BidiCallBuilder<C, Req, Res> {
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

pub struct ProtoSendMessage<'a, V: Proxied>(V::View<'a>);

impl<'a, V: Proxied> ProtoSendMessage<'a, V> {
    pub fn from_view(provider: &'a impl AsView<Proxied = V>) -> Self {
        Self(provider.as_view())
    }
}

impl<'a, V> SendMessage for ProtoSendMessage<'a, V>
where
    V: Proxied,
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

pub struct ProtoRecvMessage<'a, M: MutProxied>(M::Mut<'a>);

impl<'a, M: MutProxied> ProtoRecvMessage<'a, M> {
    pub fn from_mut(provider: &'a mut impl AsMut<MutProxied = M>) -> Self {
        Self(provider.as_mut())
    }
}

impl<'a, M> RecvMessage for ProtoRecvMessage<'a, M>
where
    M: MutProxied,
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
