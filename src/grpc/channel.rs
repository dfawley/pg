use std::marker::PhantomData;
use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;

use super::*;

pub struct ChannelCall<'a, E, D> {
    _chan: &'a Channel,
    _d: PhantomData<(E, D)>,
}

impl<'a, E: Encoder, D: Decoder> Call<E, D> for ChannelCall<'a, E, D> {
    async fn start(
        self,
        descriptor: MethodDescriptor<E, D>,
        _args: Args,
    ) -> (impl SendStream<E>, impl RecvStream<D>) {
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

impl Callable for Channel {
    fn call<'a, E: Encoder, D: Decoder>(&'a self) -> impl Call<E, D> + 'a {
        // BoxCall is unnecessary but it erases types.
        ChannelCall::<E, D> {
            _chan: self,
            _d: PhantomData,
        }
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
    async fn send_msg<'a>(&'a mut self, _msg: T::View<'a>) -> bool {
        true // false on error sending
    }
    async fn send_and_close<'a>(&'a mut self, _msg: T::View<'a>) {
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
