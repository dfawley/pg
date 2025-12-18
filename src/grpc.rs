use std::fmt::Debug;
use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;

#[derive(Clone, Debug)]
pub struct Status {
    pub code: i32,
    pub _msg: String,
}

impl Status {
    pub fn ok() -> Self {
        Status {
            code: 0,
            _msg: String::new(),
        }
    }
}

pub struct Headers {}
pub struct Trailers {
    pub status: Status,
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

pub struct MethodDescriptor<E, D> {
    pub method_name: String,
    pub message_encoder: E,
    pub message_decoder: D,
    pub method_type: MethodType,
}

pub trait Encoder: Send + Sync + Clone + 'static {
    type View<'a>: Send + Sync;

    fn encode(&self, item: Self::View<'_>) -> Vec<Vec<u8>>;
}

pub trait Decoder: Send + Sync + Clone + 'static {
    type Mut<'a>: Send + Sync;

    fn decode(&self, data: Vec<Vec<u8>>, item: Self::Mut<'_>);
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_final_message results in a signal the server can
/// use to determine the client is done sending.
pub trait SendStream<E: Encoder>: Send + Sync + 'static {
    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    fn send_msg<'a>(&'a self, msg: E::View<'a>) -> impl Future<Output = bool> + Send + 'a;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    fn send_and_close<'a>(self, msg: E::View<'a>) -> impl Future<Output = ()> + Send + 'a;
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
pub trait RecvStream<D: Decoder>: Send + Sync + 'static {
    /// Returns the response stream's headers, or None if a trailers-only
    /// response is received.
    fn headers(&mut self) -> impl Future<Output = Option<Headers>> + Send;

    /// Receives the next message on the stream into msg.  If false is returned,
    /// msg is unmodified, the stream has finished, and trailers should be
    /// called to receive the trailers from the stream.
    fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> impl Future<Output = bool> + Send + 'a;

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    fn trailers(self) -> impl Future<Output = Trailers> + Send;
}

pub trait Callable: Send + Sync {
    type SendStream<E: Encoder>: SendStream<E>;
    type RecvStream<D: Decoder>: RecvStream<D>;

    fn call<E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> impl Future<Output = (Self::SendStream<E>, Self::RecvStream<D>)> + Send;
}

impl Callable for Channel {
    type SendStream<E: Encoder> = ChannelSendStream<E>;
    type RecvStream<D: Decoder> = ChannelRecvStream<D>;

    async fn call<E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        _args: Args,
    ) -> (Self::SendStream<E>, Self::RecvStream<D>) {
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

impl<T: Encoder> SendStream<T> for ChannelSendStream<T> {
    async fn send_msg<'a>(&'a self, _msg: T::View<'a>) -> bool {
        true // false on error sending
    }
    async fn send_and_close(self, _msg: T::View<'_>) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream<T> {
    cnt: Mutex<i32>,
    _d: T,
}

impl<T: Decoder> RecvStream<T> for ChannelRecvStream<T> {
    async fn headers(&mut self) -> Option<Headers> {
        Some(Headers {})
    }

    async fn next_msg<'a>(&'a mut self, mut msg: T::Mut<'a>) -> bool {
        let mut cnt = self.cnt.lock().unwrap();
        let msg: &mut MyResponseMut =
            unsafe { &mut *(&mut msg as *mut T::Mut<'_> as *mut MyResponseMut) };
        if *cnt == 3 {
            return false;
        }
        *cnt += 1;
        msg.set_result(*cnt);
        true
    }

    async fn trailers(self) -> Trailers {
        Trailers {
            status: Status::ok(),
        }
    }
}
