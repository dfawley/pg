use std::fmt::Debug;
use std::sync::Mutex;
use std::time;

use async_trait::async_trait;

use crate::gencode::pb::*;
use protobuf::Serialize; // temporary for faking stream behavior

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

#[derive(Debug, Clone, Copy)]
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
    type Item<'a>: Send + Sync;

    fn encode(&mut self, item: Self::Item<'_>) -> Vec<Vec<u8>>;
}

pub trait Decoder: Send + Sync {
    type Item;

    fn decode(&mut self, data: Vec<Vec<u8>>) -> Self::Item;
}

/// SendStream represents the sending side of a client stream.  Dropping the
/// SendStream or calling send_final_message results in a signal the server can
/// use to determine the client is done sending.
#[async_trait]
pub trait SendStream: Send + Sync {
    type Message<'a>: Send + Sync;

    /// Sends msg on the stream.  If false is returned, the message could not be
    /// delivered because the stream was closed.  Future calls to SendStream
    /// will do nothing.
    async fn send_msg(&mut self, msg: Self::Message<'_>) -> bool;

    /// Sends msg on the stream and indicates the client has no further messages
    /// to send.
    async fn send_and_close(self, msg: Self::Message<'_>);
}

/// RecvStream represents the receiving side of a client stream.  Dropping the
/// RecvStream results in early RPC cancellation if the server has not already
/// terminated the stream first.
#[async_trait]
pub trait RecvStream: Send + Sync {
    type Message;

    /// Returns the response stream's headers, or None if a trailers-only
    /// response is received.
    async fn headers(&mut self) -> Option<Headers>;

    /// Receives the next message on the stream and returns it.  If None is
    /// returned, the stream has finished, and trailers should be called to
    /// receive the trailers from the stream.
    async fn next_msg(&mut self) -> Option<Self::Message>;

    /// Returns the trailers for the stream, consuming the stream and any
    /// unreceived messages preceding the trailers.
    async fn trailers(self) -> Trailers;
}

#[async_trait]
pub trait Callable: Send + Sync {
    type SendStream<E: Encoder>: for<'a> SendStream<Message<'a> = E::Item<'a>>;
    type RecvStream<D: Decoder>: RecvStream<Message = D::Item>;

    async fn call<E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> (Self::SendStream<E>, Self::RecvStream<D>);
}

#[async_trait]
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
                _e: descriptor.message_encoder,
            },
            ChannelRecvStream {
                _d: descriptor.message_decoder,
                cnt: Mutex::new(0),
            },
        )
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

pub struct ChannelSendStream<E> {
    _e: E,
}

#[async_trait]
impl<E: Encoder> SendStream for ChannelSendStream<E> {
    type Message<'a> = E::Item<'a>;

    async fn send_msg(&mut self, _msg: Self::Message<'_>) -> bool {
        true // false on error sending
    }
    async fn send_and_close(self, _msg: Self::Message<'_>) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream<D> {
    cnt: Mutex<i32>,
    _d: D,
}

#[async_trait]
impl<D: Decoder> RecvStream for ChannelRecvStream<D> {
    type Message = D::Item;

    async fn headers(&mut self) -> Option<Headers> {
        Some(Headers {})
    }

    async fn next_msg(&mut self) -> Option<Self::Message> {
        let mut cnt = self.cnt.lock().unwrap();
        if *cnt == 3 {
            return None;
        }
        *cnt += 1;

        let mut resp = MyResponse::default();
        resp.set_result(*cnt);
        let bytes = resp.serialize().expect("failed to serialize mock response");
        let raw_data = vec![bytes];
        Some(self._d.decode(raw_data))
    }

    async fn trailers(self) -> Trailers {
        Trailers {
            status: Status::ok(),
        }
    }
}

#[async_trait]
pub trait Interceptor: Send + Sync {
    type CallSendStream<C: Callable, E: Encoder>: for<'a> SendStream<Message<'a> = E::Item<'a>>;
    type CallRecvStream<C: Callable, D: Decoder>: RecvStream<Message = D::Item>;

    async fn intercept<E: Encoder, D: Decoder, C: Callable>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
        next: &C,
    ) -> (Self::CallSendStream<C, E>, Self::CallRecvStream<C, D>);
}

pub struct InterceptedChannel<C, I> {
    pub inner: C,
    pub interceptor: I,
}

#[async_trait]
impl<C: Callable, I: Interceptor> Callable for InterceptedChannel<C, I> {
    type SendStream<E: Encoder> = I::CallSendStream<C, E>;
    type RecvStream<D: Decoder> = I::CallRecvStream<C, D>;

    async fn call<E: Encoder, D: Decoder>(
        &self,
        descriptor: MethodDescriptor<E, D>,
        args: Args,
    ) -> (Self::SendStream<E>, Self::RecvStream<D>) {
        self.interceptor
            .intercept(descriptor, args, &self.inner)
            .await
    }
}
