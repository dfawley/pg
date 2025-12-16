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

#[tokio::main]
async fn main() {
    let channel = Channel::default();
    let client = MyServiceClientStub::new(channel.clone());
    unary(client.clone()).await;
    bidi(client.clone()).await;

    let wrap_chan = interceptor::CallInterceptor { inner: channel };
    let client = MyServiceClientStub::new(wrap_chan);
    unary(client.clone()).await;
    bidi(client.clone()).await;
}

async fn bidi<C: Callable>(client: MyServiceClientStub<C>) {
    {
        let requests = Box::pin(stream! {
            yield proto!(MyRequest { query: 1 });
            yield proto!(MyRequest { query: 2 });
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
        let res = client.unary_call(proto!(MyRequest { query: 3 })).await;
        println!("Owned response: {:?}", res);
    }

    {
        // Using a view for the request and response:
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
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
        let f1 = client.unary_call(proto!(MyRequest { query: 3 }));
        let f2 = client
            .unary_call(proto!(MyRequest { query: 3 }))
            .with_timeout(Duration::from_secs(2));
        let (a, b) = tokio::join!(f1, f2);
        println!("Joined: {:?}, {:?}", a, b);
    }
}

mod interceptor {
    use crate::grpc::Args;
    use crate::grpc::Callable;
    use crate::grpc::Decoder;
    use crate::grpc::Encoder;
    use crate::grpc::Headers;
    use crate::grpc::MethodDescriptor;
    use crate::grpc::RecvStream;
    use crate::grpc::SendStream;
    use crate::grpc::Trailers;
    use async_trait::async_trait;

    #[derive(Clone)]
    pub struct CallInterceptor<C> {
        pub inner: C,
    }

    #[async_trait]
    impl<C: Callable> Callable for CallInterceptor<C> {
        type SendStream<E: Encoder> = SendInterceptor<C::SendStream<E>>;
        type RecvStream<D: Decoder> = RecvInterceptor<C::RecvStream<D>>;

        async fn call<E: Encoder, D: Decoder>(
            &self,
            descriptor: &MethodDescriptor<E, D>,
            args: Args,
        ) -> (Self::SendStream<E>, Self::RecvStream<D>) {
            let (tx, rx) = self.inner.call(descriptor, args).await;
            (
                SendInterceptor { delegate: tx },
                RecvInterceptor { delegate: rx },
            )
        }
    }

    pub struct SendInterceptor<Delegate> {
        delegate: Delegate,
    }

    #[async_trait]
    impl<E: Encoder, Delegate: SendStream<E>> SendStream<E> for SendInterceptor<Delegate> {
        async fn send_msg(&self, msg: &E::View<'_>) -> bool {
            self.delegate.send_msg(msg).await
        }

        /// Sends msg on the stream and indicates the client has no further messages
        /// to send.
        async fn send_and_close(self, msg: &E::View<'_>) {
            self.delegate.send_and_close(msg).await
        }
    }

    pub struct RecvInterceptor<Delegate> {
        delegate: Delegate,
    }

    #[async_trait]
    impl<D: Decoder, Delegate: RecvStream<D>> RecvStream<D> for RecvInterceptor<Delegate> {
        async fn headers(&mut self) -> Option<Headers> {
            let headers = self.delegate.headers().await;
            if headers.is_some() {
                println!("got some headers");
            }
            None
        }
        async fn next_msg(&mut self, msg: &mut D::Mut<'_>) -> bool {
            self.delegate.next_msg(msg).await
        }
        async fn trailers(self) -> Trailers {
            let mut trailers = self.delegate.trailers().await;
            trailers.status.code = 3;
            trailers
        }
    }
}
