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

#[derive(Debug)]
pub enum MethodType {
    Unary,
    ClientStream,
    ServerStream,
    BidiStream,
}

pub trait Encoder: Send + Sync + Clone {
    type View<'a>: Send + Sync;

    fn encode<'a>(&self, item: &Self::View<'a>) -> Vec<Vec<u8>>;
}

pub trait Decoder: Send + Sync + Clone {
    type Mut<'a>: Send + Sync;

    fn decode<'a>(&self, data: &[&[u8]], item: &mut Self::Mut<'a>);
}

pub struct MethodDescriptor<Sender, Receiver> {
    pub method_name: String,
    pub message_encoder: Sender,
    pub message_decoder: Receiver,
    pub method_type: MethodType,
}

pub trait Callable: Send + Sync {
    fn call<Sender: Encoder, Receiver: Decoder>(
        &self,
        descriptor: &MethodDescriptor<Sender, Receiver>,
        args: Args,
    ) -> impl std::future::Future<Output = (SendStream<Sender>, RecvStream<Receiver>)> + Send;
}

impl Callable for Channel {
    fn call<Sender: Encoder, Receiver: Decoder>(
        &self,
        descriptor: &MethodDescriptor<Sender, Receiver>,
        _args: Args,
    ) -> impl std::future::Future<Output = (SendStream<Sender>, RecvStream<Receiver>)> + Send {
        println!(
            "starting call for {:?} ({:?})",
            descriptor.method_name, descriptor.method_type
        );
        std::future::ready((
            SendStream {
                _e: descriptor.message_encoder.clone(),
            },
            RecvStream {
                _d: descriptor.message_decoder.clone(),
                cnt: Mutex::new(0),
            },
        ))
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
    _e: T,
}

impl<'call, T: Encoder> SendStream<T> {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    pub async fn send_msg(&self, msg: &T::View<'_>) -> bool {
        true // false on error sending
    }
    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    pub async fn send_final_msg(self, msg: &T::View<'_>) {
        // Error doesn't matter when sending final message.
    }
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
pub struct RecvStream<T> {
    cnt: Mutex<i32>,
    _d: T,
}

impl<T: Decoder> RecvStream<T> {
    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
    pub async fn next_msg(&self, msg: &mut T::Mut<'_>) -> bool {
        let mut cnt = self.cnt.lock().unwrap();
        let msg: &mut MyResponseMut =
            unsafe { &mut *(msg as *mut T::Mut<'_> as *mut MyResponseMut) };
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
