pub trait Incable {
    fn inc(&mut self);
}

mod gencode;
mod grpc;
mod grpc_protobuf;

use gencode::MyServiceClientStub;
use gencode::pb::*;
use grpc::{Channel, SharedCall};
use protobuf::proto;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let channel = Channel::default();
    let client = MyServiceClientStub::new(channel.clone());
    {
        let res = client
            .unary_call(proto!(MyRequest { query: 3 }).as_view())
            .await;
        println!("1: {:?}", res.unwrap());
    }

    {
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_response_message(&mut resp.as_mut())
            .await;
        println!("2: {:?} / {:?}", resp, status);
    }

    {
        let res = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .await;
        println!("3: {:?}", res.unwrap());
    }

    {
        let mut resp = MyResponse::default();
        let status = client
            .unary_call(MyRequestView::default())
            .with_timeout(Duration::from_secs(2))
            .with_response_message(&mut resp.as_mut())
            .await;
        println!("4: {:?} / {:?}", resp, status);
    }

    {
        let f1 = client.unary_call(proto!(MyRequest { query: 3 }));
        let f2 = client
            .unary_call(proto!(MyRequest { query: 3 }))
            .with_timeout(Duration::from_secs(2));
        let (a, b) = tokio::join!(f1, f2);
        println!("5: {:?}, {:?}", a.unwrap(), b.unwrap());
    }
}
