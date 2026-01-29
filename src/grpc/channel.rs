use std::time;

use tokio::sync::mpsc;
use tokio::task;

use super::*;

#[derive(Clone, Default)]
pub struct Channel {
    pub server: Server,
}

impl Call for Channel {
    fn call(&self, method: String, _args: Args) -> (impl ClientSendStream, impl ClientRecvStream) {
        let (tx1, rx1) = mpsc::channel(1);
        let (tx2, rx2) = mpsc::channel(1);
        let handler = self.server.handlers.lock().unwrap().get(&method).cloned();
        if let Some(handler) = handler {
            task::spawn(async move {
                handler
                    .grpc_handle(
                        method,
                        Headers {},
                        GrpcServerSendStream::new(tx2),
                        GrpcServerRecvStream { rx: rx1 },
                    )
                    .await;
            });
        } else {
            // (Can't block since there is a buffer of 1.)
            let _ = tx2.blocking_send(ResponseStreamItem::Trailers(Trailers {
                status: Status {
                    code: 13,
                    _msg: "method not registered on server".to_string(),
                },
            }));
        }
        (ChannelSendStream { tx: tx1 }, ChannelRecvStream { rx: rx2 })
    }
}

#[derive(Debug, Default)]
pub struct Args {
    pub timeout: time::Duration,
}

pub struct ChannelSendStream {
    tx: mpsc::Sender<Vec<u8>>,
}

impl ClientSendStream for ChannelSendStream {
    async fn send_msg(&mut self, msg: &dyn SendMessage) -> Result<(), ()> {
        let _ = self.tx.send(msg.encode().pop().unwrap()).await;
        Ok(())
    }
    async fn send_and_close(mut self, msg: &dyn SendMessage) {
        let _ = self.send_msg(msg).await;
    }
}

pub struct ChannelRecvStream {
    rx: mpsc::Receiver<ResponseStreamItem<Vec<u8>>>,
}

impl ClientRecvStream for ChannelRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
        let i = self.rx.recv().await;
        if i.is_none() {
            return ClientResponseStreamItem::StreamClosed;
        }
        let i = i.unwrap();
        match i {
            ResponseStreamItem::Message(m) => {
                msg.decode(vec![m]);
                ClientResponseStreamItem::Message(())
            }
            ResponseStreamItem::Headers(headers) => ResponseStreamItem::Headers(headers),
            ResponseStreamItem::Trailers(trailers) => ResponseStreamItem::Trailers(trailers),
            ResponseStreamItem::StreamClosed => ResponseStreamItem::StreamClosed,
        }
    }
}
