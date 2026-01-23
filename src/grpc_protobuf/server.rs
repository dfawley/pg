use std::{
    pin::Pin,
    task::{Context, Poll},
};

use async_stream::stream;
use futures::sink::unfold;
use futures_core::Stream;
use futures_sink::Sink;
use protobuf::{AsMut, AsView, Message, MessageMut, MessageView, MutProxied, Proxied};

use crate::{
    grpc::{Handle, Headers, ServerRecvStream, ServerSendStream, ServerStatus, Status},
    grpc_protobuf::{ProtoRecvMessage, ProtoSendMessage},
};

#[trait_variant::make(Send)]
pub trait UnaryHandler: Send
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
        tx: impl ServerSendStream,
        mut rx: impl ServerRecvStream,
    ) -> impl Future<Output = Result<(), ServerStatus>> + Send {
        ForceSend(async move {
            let mut req = T::Req::default();
            if rx
                .next(&mut ProtoRecvMessage::from_mut(&mut req))
                .await
                .is_err()
            {
                return Err(ServerStatus(Status {
                    code: 13,
                    _msg: "error receiving request message".to_string(),
                }));
            }

            let mut res = T::Res::default();
            self.unary(req.as_view(), res.as_mut()).await?;
            let sm = ProtoSendMessage::from_view(&res);
            tx.enqueue_final_msg(&sm).await;
            Ok(())
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
        mut tx: impl ServerSendStream,
        mut rx: impl ServerRecvStream,
    ) -> impl Future<Output = Result<(), ServerStatus>> + Send {
        ForceSend(async move {
            let reqs = stream! {
                loop {
                    let mut req = B::Req::default();
                    if rx.next(&mut ProtoRecvMessage::from_mut(&mut req)).await.is_err() {
                        return;
                    }
                    yield req;
                }
            };

            let resps = unfold(tx, |mut tx, res: B::Res| async move {
                if tx
                    .send_msg(&ProtoSendMessage::from_view(&res))
                    .await
                    .is_err()
                {
                    Err(())
                } else {
                    Ok(tx)
                }
            });

            self.0.stream(reqs, resps).await
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
