use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time;

use futures_core::Stream;

use crate::gencode::pb::*;

#[derive(Clone, Debug)]
pub struct Status {
    pub code: i32,
    pub msg: String,
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
    pub async fn call<Req, Res>(
        &self,
        _req: impl Stream<Item = Req>,
        _args: &Args,
    ) -> RecvStream<Res> {
        RecvStream {
            _d: PhantomData,
            cnt: Mutex::new(0),
        }
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

#[derive(Default)]
pub struct RecvStream<T> {
    cnt: Mutex<i32>,
    _d: PhantomData<T>,
}

impl<T> RecvStream<T> {
    pub async fn next_msg(&self, msg: &mut T) -> bool {
        let mut cnt = self.cnt.lock().unwrap();
        let msg: &mut MyResponseMut = unsafe { &mut *(msg as *mut T as *mut MyResponseMut) };
        if *cnt == 3 {
            return false;
        }
        *cnt += 1;
        msg.set_result(*cnt);
        true
    }

    pub async fn status(self) -> Status /* Should have all RPC trailers/etc */ {
        Status::ok()
    }
}
