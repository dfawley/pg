use std::fmt::Debug;
use std::future::ready;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time;

use async_trait::async_trait;
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
pub struct Channel {}

#[derive(Debug)]
pub enum MethodType {
    Unary,
    ClientStream,
    ServerStream,
    BidiStream,
}

pub trait Encoder: Send + Sync + Clone + 'static {
    type View<'a>: Send + Sync;

    fn encode(&self, item: &Self::View<'_>) -> Vec<Vec<u8>>;
}

pub trait Decoder: Send + Sync + Clone + 'static {
    type Mut<'a>: Send + Sync;

    fn decode(&self, data: &[&[u8]], item: &mut Self::Mut<'_>);
}

pub struct MethodDescriptor<Sender, Receiver> {
    pub method_name: String,
    pub message_encoder: Sender,
    pub message_decoder: Receiver,
    pub method_type: MethodType,
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_final_message results in a signal the server can
/// use to determine the client is done sending.
#[async_trait]
pub trait SendStream<Sender: Encoder>: Send + Sync + 'static {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    async fn send_msg(&self, msg: &Sender::View<'_>) -> bool;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    async fn send_final_msg(self, msg: &Sender::View<'_>);
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
#[async_trait]
pub trait RecvStream<Receiver: Decoder>: Send + Sync + 'static {
    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
    async fn next_msg(&self, msg: &mut Receiver::Mut<'_>) -> bool;

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    async fn trailers(self) -> Status; /* Should have all RPC trailers/etc */
}

#[async_trait]
pub trait Callable: Send + Sync {
    type SendStream<S: Encoder>: SendStream<S>;
    type RecvStream<R: Decoder>: RecvStream<R>;

    async fn call<S: Encoder, R: Decoder>(
        &self,
        descriptor: &MethodDescriptor<S, R>,
        args: Args,
    ) -> (Self::SendStream<S>, Self::RecvStream<R>);
}

#[async_trait]
impl Callable for Channel {
    type SendStream<S: Encoder> = ChannelSendStream<S>;
    type RecvStream<R: Decoder> = ChannelRecvStream<R>;

    async fn call<S: Encoder, R: Decoder>(
        &self,
        descriptor: &MethodDescriptor<S, R>,
        _args: Args,
    ) -> (Self::SendStream<S>, Self::RecvStream<R>) {
        println!(
            "starting call for {:?} ({:?})",
            descriptor.method_name, descriptor.method_type
        );
        (
            ChannelSendStream {
                _e: descriptor.message_encoder.clone(),
            },
            ChannelRecvStream {
                _d: descriptor.message_decoder.clone(),
                cnt: Mutex::new(0),
            },
        )
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

pub struct ChannelSendStream<T> {
    _e: T,
}

#[async_trait]
impl<T: Encoder> SendStream<T> for ChannelSendStream<T> {
    async fn send_msg(&self, msg: &T::View<'_>) -> bool {
        true // false on error sending
    }
    async fn send_final_msg(self, msg: &T::View<'_>) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream<T> {
    cnt: Mutex<i32>,
    _d: T,
}

#[async_trait]
impl<T: Decoder> RecvStream<T> for ChannelRecvStream<T> {
    async fn next_msg(&self, msg: &mut T::Mut<'_>) -> bool {
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

    async fn trailers(self) -> Status {
        Status::ok()
    }
}
