#![allow(unused)]

mod gencode;
mod grpc;
mod grpc_protobuf;

use async_stream::stream;
use futures_util::StreamExt;
use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::Channel;
use grpc_protobuf::SharedCall;
use protobuf::proto;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let channel = Channel::default();
    let client = MyServiceClientStub::new(channel.clone());
    unary(client.clone()).await;
    bidi(client).await;
}

async fn bidi(client: MyServiceClientStub) {
    /*
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
    */
}

async fn unary(client: MyServiceClientStub) {
    {
        // Using an owned message for request and response:
        let res = client.unary_call(proto!(MyRequest { query: 3 })).await;
        println!("1: {:?}", res.unwrap());
    }

    {
        // Using a view for the request and response:
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_response_message(&mut resp)
            .await;
        println!("2: {:?} / {:?}", resp, status);
    }

    {
        // Owned response with timeout:
        let res = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .await;
        println!("3: {:?}", res.unwrap());
    }

    {
        // View response with timeout:
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .with_response_message(&mut resp.as_mut())
            .await;
        println!("4: {:?} / {:?}", resp, status);
    }

    {
        // Two calls joined:
        let f1 = client.unary_call(proto!(MyRequest { query: 3 }));
        let f2 = client
            .unary_call(proto!(MyRequest { query: 3 }))
            .with_timeout(Duration::from_secs(2));
        let (a, b) = tokio::join!(f1, f2);
        println!("5: {:?}, {:?}", a.unwrap(), b.unwrap());
    }
}
