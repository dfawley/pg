use std::pin::pin;

use futures::{Sink, SinkExt, Stream, StreamExt};
use protobuf::proto;

use crate::{
    gencode::{pb::*, server::MyService},
    grpc::{ServerStatus, Status},
};

pub struct MyServiceImpl {}

impl MyService for MyServiceImpl {
    async fn unary_call(&self, req: MyRequestView<'_>, mut res: MyResponseMut<'_>) -> ServerStatus {
        res.set_result(req.query());
        ServerStatus(Status::ok())
    }

    async fn streaming_call(
        &self,
        requests: impl Stream<Item = MyRequest> + Send,
        responses: impl Sink<MyResponse> + Send,
    ) -> ServerStatus {
        let mut requests = pin!(requests);
        let receive = async move {
            while let Some(req) = requests.next().await {
                println!("Server saw request {req:?}");
            }
        };
        let mut responses = pin!(responses);
        let send = async move {
            if responses
                .send(proto!(MyResponse { result: 12 }))
                .await
                .is_err()
            {
                return;
            }
            if responses
                .send(proto!(MyResponse { result: 34 }))
                .await
                .is_err()
            {
                return;
            }
            let _ = responses.send(proto!(MyResponse { result: 56 })).await;
        };
        tokio::join!(send, receive);
        ServerStatus(Status::ok())
    }
}
