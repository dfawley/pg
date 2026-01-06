#![allow(unused)]

mod gencode;
mod grpc;

use async_stream::stream;
use futures_util::StreamExt;
use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::Callable;
use grpc::Channel;
use grpc::protobuf::SharedCall;
use protobuf::proto;
use std::time::Duration;

use grpc::CallableInterceptor;

#[tokio::main]
async fn main() {
    let channel = Channel {};
    let client = MyServiceClientStub::new(channel.clone());
    unary(client.clone()).await;
    bidi(client.clone()).await;
    headers_example(client.clone()).await;

    let wrap_chan = CallableInterceptor::new(channel, interceptor::FailAllInterceptCall {});
    let wrap_chan = CallableInterceptor::new(wrap_chan, interceptor::PrintReqInterceptor {});

    let client = MyServiceClientStub::new(wrap_chan);
    unary(client.clone()).await;
    bidi(client.clone()).await;
}

async fn bidi<C: Callable>(client: MyServiceClientStub<C>) {
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
}

async fn unary<C: Callable>(client: MyServiceClientStub<C>) {
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
}

async fn headers_example<C: Callable>(client: MyServiceClientStub<C>) {
    {
        let (i, rx) = header_reader::HeaderReader::new();
        let res = client
            .unary_call(proto!(MyRequest { query: 1 }))
            .with_interceptor(i)
            .await;
        match rx.await {
            Ok(v) => println!("saw headers: {:?}", v),
            Err(_) => println!("RPC finished as trailers-only"),
        }
        println!("Response: {:?}", res);
    }
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
        async fn start<C: CallableOnce, E: Encoder, D: Decoder>(
            self,
            descriptor: MethodDescriptor<E, D>,
            args: Args,
            next: C,
        ) -> (impl SendStream<E>, impl RecvStream<D>) {
            let (tx, delegate) = next.call().start(descriptor, args).await;
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

    impl<D: Decoder, Delegate: RecvStream<D>> RecvStream<D> for HeaderReaderRecvStream<Delegate> {
        async fn headers(&mut self) -> Option<Headers> {
            let headers = self.delegate.headers().await;
            if let Some(tx) = self.tx.take()
                && let Some(h) = headers.clone()
            {
                tx.send(h).unwrap();
            }
            headers
        }
        async fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> bool {
            RecvStream::headers(self).await;
            self.delegate.next_msg(msg).await
        }
        async fn trailers(mut self) -> Trailers {
            RecvStream::headers(&mut self).await;
            self.delegate.trailers().await
        }
    }
}

mod interceptor {
    use std::any::TypeId;
    use std::marker::PhantomData;

    use crate::gencode::pb::MyRequest;
    use crate::gencode::pb::MyRequestView;
    use crate::grpc::protobuf::ProtoEncoder;
    use crate::grpc::*;

    // Note: must have Clone so corresponding wrapped channel can impl Clone.
    #[derive(Clone)]
    pub struct FailAllInterceptCall {}

    impl CallInterceptor for FailAllInterceptCall {
        async fn start<C: CallableOnce, E: Encoder, D: Decoder>(
            &self,
            descriptor: MethodDescriptor<E, D>,
            args: Args,
            next: C,
        ) -> (impl SendStream<E>, impl RecvStream<D>) {
            let (tx, rx) = next.call().start(descriptor, args).await;
            (tx, FailingRecvStreamInterceptor { delegate: rx })
        }
    }

    pub struct FailingRecvStreamInterceptor<Delegate> {
        delegate: Delegate,
    }

    impl<D: Decoder, Delegate: RecvStream<D>> RecvStream<D> for FailingRecvStreamInterceptor<Delegate> {
        async fn headers(&mut self) -> Option<Headers> {
            self.delegate.headers().await
        }
        async fn next_msg<'a>(&'a mut self, msg: D::Mut<'a>) -> bool {
            self.delegate.next_msg(msg).await
        }
        async fn trailers(self) -> Trailers {
            let mut trailers = self.delegate.trailers().await;
            trailers.status.code = 3;
            trailers
        }
    }

    #[derive(Clone)]
    pub struct PrintReqInterceptor {}

    impl CallInterceptor for PrintReqInterceptor {
        async fn start<C: CallableOnce, E: Encoder, D: Decoder>(
            &self,
            descriptor: MethodDescriptor<E, D>,
            args: Args,
            next: C,
        ) -> (impl SendStream<E>, impl RecvStream<D>) {
            let (tx, rx) = next.call().start(descriptor, args).await;
            (
                PrintReqSendStreamInterceptor {
                    delegate: tx,
                    encoder_type: PhantomData,
                },
                rx,
            )
        }
    }

    pub struct PrintReqSendStreamInterceptor<E, Delegate> {
        delegate: Delegate,
        encoder_type: PhantomData<E>,
    }

    impl<E: Encoder, Delegate: SendStream<E>> PrintReqSendStreamInterceptor<E, Delegate> {
        fn send_common(&self, msg: &E::View<'_>) {
            if TypeId::of::<E>() == TypeId::of::<ProtoEncoder<MyRequest>>() {
                // Print a field to show message inspection.
                let req: &MyRequestView =
                    unsafe { &*(msg as *const E::View<'_> as *const MyRequestView) };
                println!("Saw request query value: {}", req.query());
            }
        }
    }

    impl<E: Encoder, Delegate: SendStream<E>> SendStream<E>
        for PrintReqSendStreamInterceptor<E, Delegate>
    {
        async fn send_msg<'a>(&'a mut self, msg: E::View<'a>) -> bool {
            self.send_common(&msg);
            self.delegate.send_msg(msg).await
        }

        async fn send_and_close<'a>(&'a mut self, msg: E::View<'a>) {
            self.send_common(&msg);
            self.delegate.send_and_close(msg).await
        }
    }
}
