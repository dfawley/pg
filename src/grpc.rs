use std::fmt::Debug;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time;

use std::time::Duration;

use futures_core::Stream;
use futures_util::stream::once;

use crate::gencode::pb::*;

mod private {
    pub(crate) trait Sealed {}
}

pub trait SharedCall: private::Sealed {
    fn with_timeout(self, timeout: Duration) -> Self;
}
/*
    pub struct BidiCall<'a, Req, Res> {
        channel: &'a Channel,
        req: Req,
        args: Args,
        _d: PhantomData<Res>,
    }

    impl<'a, Req: Send + 'a, Res: Send + 'a> BidiCall<'a, Req, Res> {
        pub fn new(channel: &'a Channel, req: Req) -> Self {
            Self {
                channel,
                req,
                args: Default::default(),
                _d: PhantomData,
            }
        }

        pub async fn with_response_receiver(self, res: impl Fn(Res)) -> Status {
            self.channel
                .call(&self.req, &res, &self.args)
                .await
                .err()
                .unwrap_or(Status::ok())
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
        Res: Sync + Send + Default + 'a,
        Req: Sync + Send + 'a,
    {
        type Output = Result<Res, Status>;
        type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

        fn into_future(self) -> Self::IntoFuture {
            Box::pin(async move {
                let res = Res::default();
                self.channel.call(&self.req, &res, &self.args).await?;
                Ok(res)
            })
        }
    }
*/
pub struct UnaryCall<'a, Req, Res, ResView> {
    channel: &'a Channel,
    req: Req,
    args: Args,
    _d1: PhantomData<Res>,
    _d2: PhantomData<ResView>,
}

impl<'a, Req: Send + 'a, Res: Default + Send, ResView: Send + 'a> UnaryCall<'a, Req, Res, ResView> {
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
        let stream = self.channel.call(once(async { &self.req }), &self.args);
        stream.next_msg(res).await
    }
}

impl<'a, Req, Res, ResView> private::Sealed for UnaryCall<'a, Req, Res, ResView> {}

impl<'a, Req, Res, ResView> SharedCall for UnaryCall<'a, Req, Res, ResView> {
    fn with_timeout(mut self, t: time::Duration) -> Self {
        self.args.timeout = t;
        self
    }
}

impl<'a, Req, Res, ResView> IntoFuture for UnaryCall<'a, Req, Res, ResView>
where
    Res: Sync + Send + Debug + Default + 'a,
    Req: Sync + Send + 'a,
{
    type Output = Result<Res, Status>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let stream = self.channel.call(once(async { &self.req }), &self.args);
            let mut res = Res::default();
            let status = stream.next_msg(&mut res).await;
            if status.code != 0 {
                return Err(status);
            }
            Ok(res)
        })
    }
}

#[derive(Clone, Debug)]
#[allow(unused)]
pub struct Status {
    code: i32,
    msg: String,
}

impl Status {
    pub fn ok() -> Self {
        Status {
            code: 0,
            msg: String::new(),
        }
    }
}

#[derive(Clone, Default)]
pub struct Channel {
    pub _code: Arc<i32>,
}

impl Channel {
    fn call<Req, Res>(&self, _req: impl Stream<Item = Req>, _args: &Args) -> RecvStream<Res> {
        RecvStream {
            _d: PhantomData,
            cnt: Mutex::new(0),
        }
    }
}

#[derive(Debug, Default)]
pub struct Args {
    timeout: time::Duration,
}

#[derive(Default)]
struct RecvStream<T> {
    cnt: Mutex<i32>,
    _d: PhantomData<T>,
}

impl<T> RecvStream<T> {
    async fn next_msg(&self, msg: &mut T) -> Status {
        let mut cnt = self.cnt.lock().unwrap();
        let msg: &mut MyResponseMut = unsafe { &mut *(msg as *mut T as *mut MyResponseMut) };
        *cnt += 1;
        msg.set_result(*cnt);
        Status::ok()
    }
}
