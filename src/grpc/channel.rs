use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;
use crate::grpc_protobuf::downcast_proto_mut;

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
            ChannelRecvStream {
                state: Some(RecvStreamItem::Headers(Headers {})),
                cnt: Default::default(),
            },
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
    state: Option<RecvStreamItem>,
    cnt: Mutex<i32>,
}

impl RecvStream for ChannelRecvStream {
    async fn next<'a>(&'a mut self, msg: &'a mut dyn RecvMessage) -> Option<RecvStreamItem> {
        let ret = self.state.take();
        match ret {
            Some(RecvStreamItem::Headers(_)) => {
                self.state = Some(RecvStreamItem::Message);
            }
            Some(RecvStreamItem::Message) => {
                let mut cnt = self.cnt.lock().unwrap();
                if let Some(inner_msg) = downcast_proto_mut::<MyResponse>(msg) {
                    if *cnt == 2 {
                        // Last message; next time return trailers.
                        self.state = Some(RecvStreamItem::Trailers(Trailers {
                            status: Status::ok(),
                        }));
                    } else {
                        self.state = Some(RecvStreamItem::Message);
                    }
                    *cnt += 1;
                    inner_msg.set_result(*cnt);
                } else {
                    panic!();
                }
            }
            _ => {}
        }
        ret
    }
}
