use futures_util::stream::once;
use protobuf::AsView;
use std::pin::Pin;
use std::time::Duration;
use std::{fmt::Debug, marker::PhantomData};

use crate::grpc::{Args, Channel, Status};

mod private {
    pub(crate) trait Sealed {}
}

pub trait SharedCall: private::Sealed {
    fn with_timeout(self, timeout: Duration) -> Self;
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
        let stream = self
            .channel
            .call(once(async { self.req.as_view() }), &self.args)
            .await;
        stream.next_msg(res).await
    }
}

impl<'a, Req, Res, ResView> private::Sealed for UnaryCall<'a, Req, Res, ResView> {}

impl<'a, Req, Res, ResView> SharedCall for UnaryCall<'a, Req, Res, ResView> {
    fn with_timeout(mut self, t: Duration) -> Self {
        self.args.timeout = t;
        self
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
            let stream = self
                .channel
                .call(once(async { self.req.as_view() }), &self.args)
                .await;

            let mut res = Res::default();
            let status = stream.next_msg(&mut res).await;

            if status.code != 0 {
                return Err(status);
            }
            Ok(res)
        })
    }
}

/*
pub struct BidiCall<'a, Req, Res> {
    channel: &'a Channel,
    req: Req,
    args: Args,
    _d1: PhantomData<Res>,
}

impl<'a, Req, Res> BidiCall<'a, Req, Res>
where
    Req: RequestMessage + 'a,
    Res: Default + Send,
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

impl<'a, Req, Res> private::Sealed for BidiCall<'a, Req, Res> {}

impl<'a, Req, Res> SharedCall for BidiCall<'a, Req, Res> {
    fn with_timeout(mut self, t: time::Duration) -> Self {
        self.args.timeout = t;
        self
    }
}

impl<'a, Req, Res> IntoFuture for BidiCall<'a, Req, Res>
where
    Res: Sync + Send + Debug + Default + 'a,
    Req: RequestMessage + 'a,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            // 1. self is moved into this async block (owning the request data).
            // 2. self.req.as_view() creates a view pointing to that data.
            // 3. The stream consumes that view.
            let stream = self
                .channel
                .call(once(async { self.req.as_view() }), &self.args);

            let mut res = Res::default();
            let status = stream.next_msg(&mut res).await;

            if status.code != 0 {
                return Err(status);
            }
            Ok(res)
        })
    }
}
*/
