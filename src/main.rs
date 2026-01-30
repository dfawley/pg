mod gencode;
mod grpc;
mod grpc_protobuf;
mod server;

use protobuf::proto;
use std::time::Duration;

use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::Call;
use grpc::CallExt as _;
use grpc::Channel;
use grpc::Server;
use grpc_protobuf::SharedCall;

use server::MyServiceImpl;

// User-defined interceptors.
use header_reader::*;
use interceptor::*;

#[tokio::main]
async fn main() {
    let mut server = Server::default();
    server.register(MyServiceImpl {});

    let channel = Channel { server };
    let client = MyServiceClientStub::new(channel.clone());
    unary(&client).await;
    bidi(&client).await;
    headers_example(&client).await;

    println!("Adding interceptors...");
    let wrap_chan = channel
        .with_interceptor(FailStatusInterceptor {})
        .with_interceptor(PrintReqInterceptor {});

    let client = MyServiceClientStub::new(wrap_chan);
    unary(&client).await;
    bidi(&client).await;
    headers_example(&client).await;
}

async fn unary(client: &MyServiceClientStub<impl Call>) {
    {
        // Using an owned message for request and response:
        let res = client.unary_call(proto!(MyRequest { query: 1 })).await;
        println!("Owned response: {:?}", res.map(|r| r.result()));
    }

    {
        // Using a view for the request and response:
        let req = proto!(MyRequest { query: 2 });
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(req.as_view())
            .with_response_message(&mut resp)
            .await;
        println!("Mut response: {:?} / {:?}", resp.result(), status);
    }

    {
        // Owned response with timeout:
        let res = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .await;
        println!("Owned response with timeout: {:?}", res.map(|r| r.result()));
    }

    {
        // View response with timeout:
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .with_response_message(&mut resp.as_mut())
            .await;
        println!(
            "Mut response with timeout: {:?} / {:?}",
            resp.result(),
            status
        );
    }

    {
        // Two calls joined:
        let f1 = client.unary_call(proto!(MyRequest { query: 8 }));
        let f2 = client
            .unary_call(proto!(MyRequest { query: 9 }))
            .with_timeout(Duration::from_secs(2));
        let (a, b) = tokio::join!(f1, f2);
        println!(
            "Joined: {:?}, {:?}",
            a.map(|r| r.result()),
            b.map(|r| r.result())
        );
    }
    println!();
}

use futures::SinkExt;
use std::pin::pin;
async fn bidi(client: &MyServiceClientStub<impl Call>) {
    {
        let (tx, mut rx) = client.streaming_call().start();
        // Perform sends and receives concurrently.
        //
        // Note that the danger in doing it serially:
        //
        // tx.send().await;
        // tx.send().await; ... x 100
        // rx.next().await; ...
        //
        // is that the send stream could become blocked.  If the server is
        // also attempting to send a hundred responses before reading the
        // first request, that would result in a deadlock.
        let requests = async move {
            let mut tx = pin!(tx);
            if tx.send(proto!(MyRequest { query: 10 })).await.is_err() {
                return;
            }
            let _ = tx.send(proto!(MyRequest { query: 20 })).await;
        };
        let responses = async move {
            while let Some(res) = rx.next().await {
                println!("stream: {:?}", res.result());
            }
            println!("stream status: {:?}", rx.status().await);
        };
        tokio::join!(requests, responses);
    }
    println!();
}

async fn headers_example(client: &MyServiceClientStub<impl Call>) {
    {
        let (header_reader_interceptor, headers_rx) = HeaderReader::new();
        let res = client
            .unary_call(proto!(MyRequest { query: 1 }))
            .with_interceptor(header_reader_interceptor)
            .await;
        match headers_rx.await {
            Ok(v) => println!("saw headers: {:?}", v),
            Err(_) => println!("RPC finished as trailers-only"),
        }
        println!("Response: {:?}", res);
    }

    println!();
}

mod header_reader {
    use tokio::sync::oneshot::{self, Receiver, Sender};

    use crate::grpc::*;

    pub struct HeaderReader {
        tx: Sender<Headers>,
    }

    impl HeaderReader {
        pub fn new() -> (Self, Receiver<Headers>) {
            let (tx, rx) = oneshot::channel();
            (Self { tx }, rx)
        }
    }

    impl CallInterceptorOnce for HeaderReader {
        fn call_once(
            self,
            method: String,
            args: Args,
            next: impl CallOnce,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, delegate) = next.call_once(method, args);
            (
                tx,
                HeaderReaderRecvStream {
                    tx: Some(self.tx),
                    delegate,
                },
            )
        }
    }

    pub struct HeaderReaderRecvStream<Delegate> {
        tx: Option<Sender<Headers>>,
        delegate: Delegate,
    }

    impl<Delegate: ClientRecvStream> ClientRecvStream for HeaderReaderRecvStream<Delegate> {
        async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
            let i = self.delegate.next(msg).await;
            if let ResponseStreamItem::Headers(h) = &i
                && let Some(tx) = self.tx.take()
            {
                let _ = tx.send(h.clone());
            }
            i
        }
    }
}

mod interceptor {
    use crate::{gencode::pb::MyRequest, grpc::*, grpc_protobuf::ProtoSendMessage};

    pub struct FailStatusInterceptor {}

    impl CallInterceptor for FailStatusInterceptor {
        fn call(
            &self,
            method: String,
            args: Args,
            next: &impl Call,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, rx) = next.call(method, args);
            (tx, FailingRecvStreamInterceptor { delegate: rx })
        }
    }

    struct FailingRecvStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<Delegate: ClientRecvStream> ClientRecvStream for FailingRecvStreamInterceptor<Delegate> {
        async fn next(&mut self, msg: &mut dyn RecvMessage) -> ClientResponseStreamItem {
            let i = self.delegate.next(msg).await;
            if let ResponseStreamItem::Trailers(mut t) = i {
                t.status.code = 3;
                return ResponseStreamItem::Trailers(t);
            }
            i
        }
    }

    pub struct PrintReqInterceptor {}

    impl CallInterceptor for PrintReqInterceptor {
        fn call(
            &self,
            method: String,
            args: Args,
            next: &impl Call,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, rx) = next.call(method, args);
            (PrintReqSendStreamInterceptor { delegate: tx }, rx)
        }
    }

    pub struct PrintReqSendStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<Delegate: ClientSendStream> ClientSendStream for PrintReqSendStreamInterceptor<Delegate> {
        async fn send_msg(
            &mut self,
            msg: &dyn SendMessage,
            opts: SendMsgOptions,
        ) -> Result<(), ()> {
            if let Some(req) = msg.downcast_ref::<ProtoSendMessage<MyRequest>>() {
                println!("Saw request query value: {}", req.query());
            }
            self.delegate.send_msg(msg, opts).await
        }
    }
}
