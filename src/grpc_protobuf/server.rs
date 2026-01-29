use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_stream::stream;
use futures_core::Stream;
use futures_sink::Sink;
use protobuf::{AsMut, AsView, Message, MessageMut, MessageView, MutProxied, Proxied};
use tokio::{select, sync::mpsc};
use tokio_util::sync::PollSender;

use crate::{
    grpc::{
        Handle, Headers, ResponseStreamItem, ServerRecvStream, ServerSendStream, ServerStatus,
        Status, Trailers,
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
    ) -> impl Future<Output = ServerStatus> + Send;
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
    fn handle<'a>(
        &'a self,
        _method: String,
        _headers: Headers,
        mut tx: impl ServerSendStream + 'a,
        mut rx: impl ServerRecvStream + 'a,
    ) -> impl Future<Output = ()> + Send {
        ForceSend(async move {
            let mut req = T::Req::default();
            rx.next(&mut ProtoRecvMessage::from_mut(&mut req))
                .await
                .unwrap();
            let mut res = T::Res::default();
            let ServerStatus(status) = self.unary(req.as_view(), res.as_mut()).await;
            if status.code != 0 {
                tx.send(ResponseStreamItem::Trailers(Trailers { status }))
                    .await;
                return;
            }
            let sm = ProtoSendMessage::from_view(&res);
            tx.send(ResponseStreamItem::Message(&sm)).await;
            tx.send(ResponseStreamItem::Trailers(Trailers {
                status: Status::ok(),
            }))
            .await;
        })
    }
}

#[allow(clippy::crate_in_macro_def)]
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
            ) -> ServerStatus {
                self.0.$method(req, res).await
            }
        }
        crate::grpc::RegisterMethod::register_method($server, $path, MethodShim($service.clone()));
    }};
}

#[trait_variant::make(Send)]
pub trait BidiHandler: Send + Sync
where
    for<'a> <Self::Req as MutProxied>::Mut<'a>: MessageMut<'a> + Send + Sync,
    for<'a> <Self::Res as Proxied>::View<'a>: MessageView<'a> + Send + Sync,
{
    type Req: Message + Send + Sync;
    type Res: Message + Proxied + Send + Sync;
    fn stream<'a>(
        &'a self,
        req_stream: impl Stream<Item = Self::Req> + Send + 'a,
        res: impl Sink<Self::Res> + Send + 'a,
    ) -> impl Future<Output = ServerStatus> + Send + 'a;
}

pub struct BidiHandle<B>(pub B);

impl<B: BidiHandler> Handle for BidiHandle<B> {
    async fn handle<'a>(
        &'a self,
        _method: String,
        _headers: Headers,
        mut tx: impl ServerSendStream + 'a,
        mut rx: impl ServerRecvStream + 'a,
    ) {
        let request_stream = stream! {
            loop {
                let mut req = B::Req::default();
                {
                     let wrapper = &mut ProtoRecvMessage::from_mut(&mut req);
                     if rx.next(wrapper).await.is_err() {
                         return;
                     }
                }
                yield req;
            }
        };

        let (sink_tx, sink_rx) = mpsc::channel::<B::Res>(1);
        let mut sink_rx = Some(sink_rx);
        let user_sink = PollSender::new(sink_tx);
        let mut handler_fut = Box::pin(self.0.stream(request_stream, user_sink));

        let status = loop {
            select! {
                ServerStatus(status) = &mut handler_fut => {
                    break status;
                }
                maybe_msg =
                    sink_rx.as_mut().unwrap().recv(), if sink_rx.is_some() => {
                    if let Some(msg) = maybe_msg {
                        tx.send(ResponseStreamItem::Message(
                            &ProtoSendMessage::<B::Res>::from_view(&msg.as_view())),
                        ).await;
                    } else {
                        sink_rx = None;
                    }
                }
            }
        };
        // When the handler future exits, drain the handler's sink.
        if let Some(mut rx) = sink_rx {
            while let Some(msg) = rx.recv().await {
                tx.send(ResponseStreamItem::Message(
                    &ProtoSendMessage::<B::Res>::from_view(&msg.as_view()),
                ))
                .await;
            }
        }
        tx.send(ResponseStreamItem::Trailers(Trailers { status }))
            .await;
    }
}

#[allow(clippy::crate_in_macro_def)]
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

            async fn stream<'a>(
                &'a self,
                req_stream: impl Stream<Item = Self::Req> + Send + 'a,
                res: impl Sink<Self::Res> + Send + 'a,
            ) -> ServerStatus {
                self.0.$method(req_stream, res).await
            }
        }
        crate::grpc::RegisterMethod::register_method(
            $server,
            $path,
            BidiHandle(MethodShim($service.clone())),
        );
    }};
}
