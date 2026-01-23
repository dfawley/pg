use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::Stream;
use futures_sink::Sink;
use protobuf::{AsMut, AsView, Message, MessageMut, MessageView, MutProxied, Proxied};

use crate::{
    grpc::{Handle, Headers, ServerRecvStream, ServerSendStream, ServerStatus, Status},
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
