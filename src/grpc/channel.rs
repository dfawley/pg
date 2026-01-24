use std::sync::Mutex;
use std::time;

use crate::gencode::pb::*;
use crate::grpc_protobuf::ProtoRecvMessage;

use super::*;

#[derive(Clone, Default)]
pub struct Channel {
    pub server: Server,
}

impl Call for Channel {
    async fn call(
        &self,
        method: String,
        _args: Args,
    ) -> (impl ClientSendStream, impl ClientRecvStream) {
        println!("starting call for {method}");
        (
            ChannelSendStream {},
            ChannelRecvStream {
                state: Some(ResponseStreamItem::Headers(Headers {})),
                cnt: Mutex::new(0),
            },
        )
        /*
        let (tx1, rx1) = mpsc::channel(1);
        let (tx2, rx2) = mpsc::channel(1);
        task::spawn(async move {
            let handler = self.server.handlers.lock().unwrap().get(&method).unwrap();
            let res = handler
                .grpc_handle(
                    method,
                    Headers {},
                    GrpcServerSendStream { tx2 },
                    GrpcServerRecvStream { rx1 },
                )
                .await;
            let _ = tx2.send(res);
        });
        (ChannelSendStream { tx1 }, ChannelRecvStream { rx2 })
        */
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
    state: Option<ClientResponseStreamItem>,
    cnt: Mutex<i32>,
}

impl ClientRecvStream for ChannelRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
        let ret = self.state.take();
        match ret {
            Some(ResponseStreamItem::Headers(_)) => {
                self.state = Some(ResponseStreamItem::Message(()));
            }
            Some(ResponseStreamItem::Message(_)) => {
                let mut cnt = self.cnt.lock().unwrap();
                if let Some(inner_msg) = msg.downcast_mut::<ProtoRecvMessage<MyResponse>>() {
                    if *cnt == 2 {
                        // Last message; next time return trailers.
                        self.state = Some(ResponseStreamItem::Trailers(Trailers {
                            status: Status::ok(),
                        }));
                    } else {
                        self.state = Some(ResponseStreamItem::Message(()));
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
            ResponseStreamItem::StreamClosed
        }
    }
}
