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
    pub async fn call<Req, Res>(&self, _args: &Args) -> (SendStream<Req>, RecvStream<Res>) {
        (
            SendStream { _d: PhantomData },
            RecvStream {
                _d: PhantomData,
                cnt: Mutex::new(0),
            },
        )
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_final_message results in a signal the server can
/// use to determine the client is done sending.
pub struct SendStream<T> {
    _d: PhantomData<T>,
}

impl<T> SendStream<T> {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    pub async fn send_msg(&self, msg: T) -> bool {
        true // false on error sending
    }
    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    pub async fn send_final_msg(self, msg: T) {
        // Error doesn't matter when sending final message.
    }
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
#[derive(Default)]
pub struct RecvStream<T> {
    cnt: Mutex<i32>,
    _d: PhantomData<T>,
}

impl<T> RecvStream<T> {
    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
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

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    pub async fn trailers(self) -> Status /* Should have all RPC trailers/etc */ {
        Status::ok()
    }
}
