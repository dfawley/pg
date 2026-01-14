mod gencode;
mod grpc;
mod grpc_protobuf;

use async_stream::stream;
use futures_util::StreamExt as _;
use protobuf::proto;
use std::time::Duration;

use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::Call;
use grpc::CallExt as _;
use grpc::Channel;
use grpc_protobuf::SharedCall;

// User-defined interceptors.
use header_reader::*;
use interceptor::*;

#[tokio::main]
async fn main() {
    let channel = Channel {};
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

async fn unary<C: Call>(client: &MyServiceClientStub<C>) {
    {
        // Using an owned message for request and response:
        let res = client.unary_call(proto!(MyRequest { query: 1 })).await;
        println!("Owned response: {:?}", res);
    }

    {
        // Using a view for the request and response:
        let req = proto!(MyRequest { query: 2 });
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(req.as_view())
            .with_response_message(&mut resp)
            .await;
        println!("Mut response: {:?} / {:?}", resp, status);
    }

    {
        // Owned response with timeout:
        let res = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .await;
        println!("Owned response with timeout: {:?}", res);
    }

    {
        // View response with timeout:
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .with_response_message(&mut resp.as_mut())
            .await;
        println!("Mut response with timeout: {:?} / {:?}", resp, status);
    }

    {
        // Two calls joined:
        let f1 = client.unary_call(proto!(MyRequest { query: 8 }));
        let f2 = client
            .unary_call(proto!(MyRequest { query: 9 }))
            .with_timeout(Duration::from_secs(2));
        let (a, b) = tokio::join!(f1, f2);
        println!("Joined: {:?}, {:?}", a, b);
    }
    println!();
}

async fn bidi<C: Call>(client: &MyServiceClientStub<C>) {
    {
        let requests = Box::pin(stream! {
            yield proto!(MyRequest { query: 10 });
            yield proto!(MyRequest { query: 20 });
        });
        let mut res = client.streaming_call(requests).await;
        while let Some(res) = res.next().await {
            println!("stream: {:?}", res);
        }
    }
    println!();
}

async fn headers_example<C: Call>(client: &MyServiceClientStub<C>) {
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
        async fn call<C: CallOnce>(
            self,
            descriptor: MethodDescriptor,
            args: Args,
            next: C,
        ) -> (impl SendStream, impl RecvStream) {
            let (tx, delegate) = next.call(descriptor, args).await;
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

    impl<Delegate: RecvStream> RecvStream for HeaderReaderRecvStream<Delegate> {
        async fn next<'a>(&'a mut self, msg: &'a mut dyn RecvMessage) -> Option<RecvStreamItem> {
            let i = self.delegate.next(msg).await;
            if let Some(RecvStreamItem::Headers(h)) = &i
                && let Some(tx) = self.tx.take()
            {
                let _ = tx.send(h.clone());
            }
            i
        }
    }
}

mod interceptor {
    use crate::{gencode::pb::MyRequest, grpc::*, grpc_protobuf::downcast_proto_view};

    pub struct FailStatusInterceptor {}

    impl CallInterceptor for FailStatusInterceptor {
        async fn call<C: CallOnce>(
            &self,
            descriptor: MethodDescriptor,
            args: Args,
            next: C,
        ) -> (impl SendStream, impl RecvStream) {
            let (tx, rx) = next.call(descriptor, args).await;
            (tx, FailingRecvStreamInterceptor { delegate: rx })
        }
    }

    struct FailingRecvStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<Delegate: RecvStream> RecvStream for FailingRecvStreamInterceptor<Delegate> {
        async fn next<'a>(&'a mut self, msg: &'a mut dyn RecvMessage) -> Option<RecvStreamItem> {
            let i = self.delegate.next(msg).await;
            if let Some(RecvStreamItem::Trailers(mut t)) = i {
                t.status.code = 3;
                return Some(RecvStreamItem::Trailers(t));
            }
            i
        }
    }

    pub struct PrintReqInterceptor {}

    impl CallInterceptor for PrintReqInterceptor {
        async fn call<C: CallOnce>(
            &self,
            descriptor: MethodDescriptor,
            args: Args,
            next: C,
        ) -> (impl SendStream, impl RecvStream) {
            let (tx, rx) = next.call(descriptor, args).await;
            (PrintReqSendStreamInterceptor { delegate: tx }, rx)
        }
    }

    pub struct PrintReqSendStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<Delegate: SendStream> PrintReqSendStreamInterceptor<Delegate> {
        fn send_common(&self, msg: &dyn SendMessage) {
            if let Some(req) = downcast_proto_view::<MyRequest>(msg) {
                println!("Saw request query value: {}", req.query());
            }
        }
    }

    impl<Delegate: SendStream> SendStream for PrintReqSendStreamInterceptor<Delegate> {
        async fn send_msg<'a>(&'a mut self, msg: &'a dyn SendMessage) -> bool {
            self.send_common(msg);
            self.delegate.send_msg(msg).await
        }

        async fn send_and_close<'a>(&'a mut self, msg: &'a dyn SendMessage) {
            self.send_common(msg);
            self.delegate.send_and_close(msg).await
        }
    }
}
