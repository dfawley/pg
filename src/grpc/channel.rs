use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;
use crate::grpc_protobuf::ProtoRecvMessage;

use super::*;

impl Call for Channel {
    async fn call(&self, method: String, _args: Args) -> (impl ClientSendStream, impl ClientRecvStream) {
        println!("starting call for {method}");
        (
            ChannelSendStream {},
            ChannelRecvStream {
                state: Some(ClientRecvStreamItem::Headers(Headers {})),
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

impl ClientSendStream for ChannelSendStream {
    async fn send_msg(&mut self, _msg: &dyn SendMessage) -> Result<(), ()> {
        Ok(()) // Err(()) on error sending
    }
    async fn send_and_close(self, _msg: &dyn SendMessage) {
        // Error doesn't matter when sending final message.
    }
}

pub struct ChannelRecvStream {
    state: Option<ClientRecvStreamItem>,
    cnt: Mutex<i32>,
}

impl ClientRecvStream for ChannelRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientRecvStreamItem {
        let ret = self.state.take();
        match ret {
            Some(ClientRecvStreamItem::Headers(_)) => {
                self.state = Some(ClientRecvStreamItem::Message);
            }
            Some(ClientRecvStreamItem::Message) => {
                let mut cnt = self.cnt.lock().unwrap();
                if let Some(inner_msg) = msg.downcast_mut::<ProtoRecvMessage<MyResponse>>() {
                    if *cnt == 2 {
                        // Last message; next time return trailers.
                        self.state = Some(ClientRecvStreamItem::Trailers(Trailers {
                            status: Status::ok(),
                        }));
                    } else {
                        self.state = Some(ClientRecvStreamItem::Message);
                    }
                    *cnt += 1;
                    inner_msg.set_result(*cnt);
                } else {
                    panic!();
                }
            }
            _ => {}
        }
        if let Some(ret) = ret {
            ret
        } else {
            ClientRecvStreamItem::StreamClosed
        }
    }
}
