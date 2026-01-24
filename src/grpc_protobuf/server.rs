use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::join, sink::unfold};
use futures_core::Stream;
use futures_sink::Sink;
use protobuf::{AsMut, AsView, Message, MessageMut, MessageView, MutProxied, Proxied};

use crate::{
    grpc::{
        Handle, Headers, ResponseStreamItem, ServerRecvStream, ServerSendStream, ServerStatus,
        Trailers,
    },
    grpc_protobuf::{ProtoRecvMessage, ProtoSendMessage},
};

#[trait_variant::make(Send)]
pub trait UnaryHandler: Send + Sync
where
    for<'a> <Self::Req as MutProxied>::Mut<'a>: MessageMut<'a> + Send + Sync,
    for<'a> <Self::Res as Proxied>::View<'a>: MessageView<'a> + Send + Sync,
{
    type Req: Message + 'static;
    type Res: Message + 'static;
    fn unary(
        &self,
        req: <Self::Req as Proxied>::View<'_>,
        res: <Self::Res as MutProxied>::Mut<'_>,
    ) -> impl Future<Output = Result<(), ServerStatus>> + Send;
}

struct ForceSend<F>(F);

impl<F: Future> Future for ForceSend<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            let inner = self.map_unchecked_mut(|s| &mut s.0);
            inner.poll(cx)
        }
    }
}

unsafe impl<F> Send for ForceSend<F> {}
unsafe impl<F> Sync for ForceSend<F> {}

impl<T: UnaryHandler> Handle for T {
    fn handle(
        &self,
        _method: String,
        _headers: Headers,
        mut tx: impl ServerSendStream,
        mut rx: impl ServerRecvStream,
    ) -> impl Future<Output = ()> + Send {
        ForceSend(async move {
            let mut req = T::Req::default();
            rx.next(&mut ProtoRecvMessage::from_mut(&mut req)).await;
            let mut res = T::Res::default();
            let status = self.unary(req.as_view(), res.as_mut()).await;
            if let Err(ServerStatus(status)) = status {
                tx.send(ResponseStreamItem::Trailers(Trailers { status }))
                    .await;
                return;
            }
            let sm = ProtoSendMessage::from_view(&res);
            tx.send(ResponseStreamItem::Message(&sm)).await;
        })
    }
}

#[macro_export]
macro_rules! register_unary {
    (
        $server:expr,
        $path:expr,
        $service:expr,
        $method:ident,
        $Req:ty,
        $Res:ty
    ) => {{
        struct MethodShim<S>(S);

        impl<S> UnaryHandler for MethodShim<S>
        where
            S: std::ops::Deref + Send + Sync + 'static,
            S::Target: MyService,
        {
            type Req = $Req;
            type Res = $Res;

            async fn unary(
                &self,
                req: <Self::Req as Proxied>::View<'_>,
                res: <Self::Res as MutProxied>::Mut<'_>,
            ) -> Result<(), ServerStatus> {
                self.0.$method(req, res).await
            }
        }
        $server.register($path, MethodShim($service.clone()));
    }};
}

#[trait_variant::make(Send)]
pub trait BidiHandler: Send + Sync
where
    for<'a> <Self::Req as MutProxied>::Mut<'a>: MessageMut<'a> + Send + Sync,
    for<'a> <Self::Res as Proxied>::View<'a>: MessageView<'a> + Send + Sync,
{
    type Req: Message + 'static;
    type Res: Message + 'static;
    fn stream(
        &self,
        req_stream: impl Stream<Item = Self::Req> + Send,
        res: impl Sink<Self::Res> + Send,
    ) -> impl Future<Output = Result<(), ServerStatus>> + Send;
}

pub struct BidiHandle<B>(pub B);

impl<B: BidiHandler> Handle for BidiHandle<B> {
    fn handle(
        &self,
        _method: String,
        _headers: Headers,
        tx: impl ServerSendStream,
        mut rx: impl ServerRecvStream,
    ) -> impl Future<Output = ()> + Send {
        ForceSend(async move {
            let send_fut = async move {
                loop {
                    let mut req = B::Req::default();
                    rx.next(&mut ProtoRecvMessage::from_mut(&mut req)).await;
                }
            };
            let recv_fut = async move {
                let resps = unfold(tx, |mut tx, res: B::Res| async move {
                    let msg = &ProtoSendMessage::from_view(&res);
                    tx.send(ResponseStreamItem::Message(msg)).await;
                    Ok::<_, ()>(tx)
                });
            };

            join(send_fut, recv_fut).await;
        })
    }
}

#[macro_export]
macro_rules! register_bidi {
    (
        $server:expr,
        $path:expr,
        $service:expr,
        $method:ident,
        $Req:ty,
        $Res:ty
    ) => {{
        struct MethodShim<S>(S);

        impl<S> BidiHandler for MethodShim<S>
        where
            S: std::ops::Deref + Send + Sync + 'static,
            S::Target: MyService,
        {
            type Req = $Req;
            type Res = $Res;

            async fn stream(
                &self,
                req_stream: impl Stream<Item = Self::Req> + Send,
                res: impl Sink<Self::Res> + Send,
            ) -> Result<(), ServerStatus> {
                self.0.$method(req_stream, res).await
            }
        }
        $server.register($path, BidiHandle(MethodShim($service.clone())));
    }};
}
