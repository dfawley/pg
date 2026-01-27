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
        Handle, Headers, ResponseStreamItem, SendMessage, ServerRecvStream, ServerSendStream,
        ServerStatus, Status, Trailers,
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
            rx.next(&mut ProtoRecvMessage::from_mut(&mut req))
                .await
                .unwrap();
            let mut res = T::Res::default();
            let status = self.unary(req.as_view(), res.as_mut()).await;
            if let Err(ServerStatus(status)) = status {
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
            ) -> Result<(), ServerStatus> {
                self.0.$method(req, res).await
            }
        }
        crate::grpc::Register::register($server, $path, MethodShim($service.clone()));
    }};
}

#[trait_variant::make(Send)]
pub trait BidiHandler: Send + Sync
where
    for<'a> <Self::Req as MutProxied>::Mut<'a>: MessageMut<'a> + Send + Sync,
    for<'a> <Self::Res as Proxied>::View<'a>: MessageView<'a> + Send + Sync,
{
    type Req: Message + Send + Sync + 'static;
    type Res: Message + Proxied + Send + Sync + 'static;
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
        mut tx: impl ServerSendStream,
        mut rx: impl ServerRecvStream,
    ) -> impl Future<Output = ()> + Send {
        async move {
            let request_stream = stream! {
                loop {
                    let mut req = B::Req::default();
                    {
                         let mut wrapper = &mut ProtoRecvMessage::from_mut(&mut req);
                         if rx.next(wrapper).await.is_err() {
                             return;
                         }
                    }
                    yield req;
                }
            };

            let (user_tx, rx_inner) = mpsc::channel::<B::Res>(1);
            let mut channel_rx = Some(rx_inner);
            let user_sink = PollSender::new(user_tx);
            let mut handler_fut = Box::pin(self.0.stream(request_stream, user_sink));

            let status = loop {
                select! {
                    result = &mut handler_fut => {
                        if let Some(mut rx) = channel_rx {
                            while let Ok(msg) = rx.try_recv() {
                                tx.send(ResponseStreamItem::Message(&ProtoSendMessage::from_view(&msg.as_view()) as &dyn SendMessage)).await;
                            }
                        }

                        let status = match result {
                            Ok(()) => Status::ok(),
                            Err(ss) => ss.0,
                        };
                        break status;
                    }

                    maybe_msg =
                        channel_rx.as_mut().expect("guard").recv()
                    , if channel_rx.is_some() => {
                        match maybe_msg {
                            Some(msg) => {
                                tx.send(ResponseStreamItem::Message(&ProtoSendMessage::from_view(&msg.as_view()))).await;
                            }
                            None => {
                                channel_rx = None;
                            }
                        }
                    }
                }
            };
            tx.send(ResponseStreamItem::Trailers(Trailers { status }))
                .await;
        }
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

            async fn stream(
                &self,
                req_stream: impl Stream<Item = Self::Req> + Send,
                res: impl Sink<Self::Res> + Send,
            ) -> Result<(), ServerStatus> {
                self.0.$method(req_stream, res).await
            }
        }
        crate::grpc::Register::register($server, $path, BidiHandle(MethodShim($service.clone())));
    }};
}
