use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;
use crate::grpc_protobuf::ProtoMessageMut;

use super::*;

impl Call for Channel {
    async fn call(
        &self,
        descriptor: MethodDescriptor,
        _args: Args,
    ) -> (impl SendStream, impl RecvStream) {
        println!(
            "starting call for {:?} ({:?})",
            descriptor.method_name, descriptor.method_type
        );
        (
            ChannelSendStream {},
            ChannelRecvStream { cnt: Mutex::new(0) },
        )
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

pub struct ChannelSendStream {}

impl SendStream for ChannelSendStream {
    async fn send_msg<'a>(&'a mut self, _msg: &'a dyn SendMessage) -> bool {
        true // false on error sending
    }
    async fn send_and_close<'a>(&'a mut self, _msg: &'a dyn SendMessage) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream {
    cnt: Mutex<i32>,
}

impl RecvStream for ChannelRecvStream {
    async fn headers(&mut self) -> Option<Headers> {
        Some(Headers {})
    }

    async fn next_msg<'a>(&'a mut self, msg: &'a mut dyn RecvMessage) -> bool {
        let mut cnt = self.cnt.lock().unwrap();

        unsafe {
            let wrapper_ptr = msg as *mut dyn RecvMessage as *mut ProtoMessageMut<MyResponseMut>;
            let wrapper = &mut *wrapper_ptr;
            let inner_msg = &mut wrapper.0;
            if *cnt == 3 {
                return false;
            }
            *cnt += 1;
            inner_msg.set_result(*cnt);
        }
        true
    }

    async fn trailers(self) -> Trailers {
        Trailers {
            status: Status::ok(),
        }
    }
}
