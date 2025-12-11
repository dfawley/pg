use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::stream::once;
use protobuf::AsView;
use std::pin::Pin;
use std::time::Duration;
use std::{fmt::Debug, marker::PhantomData};
use tokio::task;

use crate::grpc::{Args, Channel, Status};

mod private {
    pub(crate) trait Sealed {}
}

pub struct UnaryCall<'a, Req, Res, ResView> {
    channel: &'a Channel,
    req: Req,
    args: Args,
    _d1: PhantomData<Res>,
    _d2: PhantomData<ResView>,
}

impl<'a, Req, Res, ResView> UnaryCall<'a, Req, Res, ResView>
where
    Req: Sync + Send + AsView + 'a,
    Res: Default + Send,
    ResView: Send + 'a,
{
    pub fn new(channel: &'a Channel, req: Req) -> Self {
        Self {
            channel,
            req,
            args: Default::default(),
            _d1: PhantomData,
            _d2: PhantomData,
        }
    }

    pub async fn with_response_message(self, res: &mut ResView) -> Status {
        let (tx, rx) = self.channel.call(&self.args).await;
        tx.send_final_msg(self.req.as_view());
        // TODO: cardinality violation checks.
        rx.next_msg(res).await;
        rx.trailers().await
    }
}

impl<'a, Req, Res, ResView> IntoFuture for UnaryCall<'a, Req, Res, ResView>
where
    Res: Sync + Send + Debug + Default + 'a,
    Req: Sync + Send + AsView + 'a,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            // 1. self is moved into this async block (owning the request data).
            // 2. self.req.as_view() creates a view pointing to that data.
            // 3. The stream consumes that view.
            let (tx, rx) = self.channel.call(&self.args).await;
            tx.send_final_msg(self.req.as_view());
            let mut res = Res::default();
            // TODO: cardinality violation checks.
            rx.next_msg(&mut res).await;
            let status = rx.trailers().await;
            if status.code != 0 {
                Err(status)
            } else {
                Ok(res)
            }
        })
    }
}

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

pub trait CallArgs: private::Sealed {
    fn args_mut(&mut self) -> &mut Args;
}

impl<'a, Req, Res, ResView> private::Sealed for UnaryCall<'a, Req, Res, ResView> {}
impl<'a, Req, Res, ResView> CallArgs for UnaryCall<'a, Req, Res, ResView> {
    fn args_mut(&mut self) -> &mut Args {
        &mut self.args
    }
}

impl<'a, Req, Res> private::Sealed for BidiCall<'a, Req, Res> {}
impl<'a, Req, Res> CallArgs for BidiCall<'a, Req, Res> {
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
