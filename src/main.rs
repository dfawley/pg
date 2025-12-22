#![allow(unused)]

mod gencode;
mod grpc;
mod grpc_protobuf;

use async_stream::stream;
use futures_util::StreamExt;
use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::Callable;
use grpc::Channel;
use grpc_protobuf::SharedCall;
use protobuf::proto;
use std::time::Duration;

use crate::grpc::Interceptor;

#[tokio::main]
async fn main() {
    let channel = Channel::default();
    let client = MyServiceClientStub::new(channel.clone());
    unary(client.clone()).await;
    bidi(client.clone()).await;

    let wrap_chan = Interceptor::new(channel, interceptor::FailAllInterceptCall {});
    let wrap_chan = Interceptor::new(wrap_chan, interceptor::PrintReqInterceptor {});

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

mod interceptor {
    use std::any::TypeId;
    use std::marker::PhantomData;

    use crate::gencode::pb::MyRequest;
    use crate::gencode::pb::MyRequestView;
    use crate::grpc::Args;
    use crate::grpc::Call;
    use crate::grpc::CallInterceptor;
    use crate::grpc::Callable;
    use crate::grpc::Decoder;
    use crate::grpc::Encoder;
    use crate::grpc::Headers;
    use crate::grpc::MethodDescriptor;
    use crate::grpc::RecvStream;
    use crate::grpc::SendStream;
    use crate::grpc::Trailers;
    use crate::grpc_protobuf::ProtoEncoder;

    #[derive(Clone)]
    pub struct FailAllInterceptCall {}

    impl CallInterceptor for FailAllInterceptCall {
        async fn start<C: Callable, E: Encoder, D: Decoder>(
            &self,
            descriptor: MethodDescriptor<E, D>,
            args: Args,
            next: &C,
        ) -> (impl SendStream<E>, impl RecvStream<D>) {
            let encoder_type_id = TypeId::of::<ProtoEncoder<MyRequest>>();
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
        async fn start<C: Callable, E: Encoder, D: Decoder>(
            &self,
            descriptor: MethodDescriptor<E, D>,
            args: Args,
            next: &C,
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
