mod gencode;
mod grpc;
mod grpc_protobuf;
mod server;

use async_stream::stream;
use futures_util::StreamExt as _;
use protobuf::proto;
use std::time::Duration;

use gencode::MyServiceClientStub;
use gencode::pb::*;
use gencode::server::register_my_service;
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
    register_my_service(&mut server, MyServiceImpl {});
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

async fn bidi(client: &MyServiceClientStub<impl Call>) {
    {
        let requests = Box::pin(stream! {
            yield proto!(MyRequest { query: 10 });
            yield proto!(MyRequest { query: 20 });
        });
        let mut res = client.streaming_call(requests).await;
        let mut saw_err = false;
        while let Some(res) = res.next().await {
            saw_err |= res.is_err();
            println!("stream: {:?}", res.map(|r| r.result()));
        }
        if !saw_err {
            println!("stream terminated successfully")
        }
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
        async fn call_once(
            self,
            method: String,
            args: Args,
            next: impl CallOnce,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, delegate) = next.call_once(method, args).await;
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
        async fn call(
            &self,
            method: String,
            args: Args,
            next: &impl Call,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, rx) = next.call(method, args).await;
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
        async fn call(
            &self,
            method: String,
            args: Args,
            next: &impl Call,
        ) -> (impl ClientSendStream, impl ClientRecvStream) {
            let (tx, rx) = next.call(method, args).await;
            (PrintReqSendStreamInterceptor { delegate: tx }, rx)
        }
    }

    pub struct PrintReqSendStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<Delegate: ClientSendStream> PrintReqSendStreamInterceptor<Delegate> {
        fn send_common(&self, msg: &dyn SendMessage) {
            if let Some(req) = msg.downcast_ref::<ProtoSendMessage<MyRequest>>() {
                println!("Saw request query value: {}", req.query());
            }
        }
    }

    impl<Delegate: ClientSendStream> ClientSendStream for PrintReqSendStreamInterceptor<Delegate> {
        async fn send_msg(&mut self, msg: &dyn SendMessage) -> Result<(), ()> {
            self.send_common(msg);
            self.delegate.send_msg(msg).await
        }

        async fn send_and_close(self, msg: &dyn SendMessage) {
            self.send_common(msg);
            self.delegate.send_and_close(msg).await
        }
    }
}
