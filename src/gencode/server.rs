use std::sync::Arc;

use futures::Sink;
use futures_core::Stream;
use protobuf::{MutProxied, Proxied};

use crate::grpc::{Server, ServerStatus, Status};
use crate::grpc_protobuf::{BidiHandle, BidiHandler, UnaryHandler};
use crate::{register_bidi, register_unary};

use super::pb::*;

#[trait_variant::make(Send)]
pub trait MyService: Send + Sync + 'static {
    async fn unary_call(
        &self,
        req: MyRequestView<'_>,
        res: MyResponseMut<'_>,
    ) -> Result<(), ServerStatus>;

    async fn streaming_call(
        &self,
        requests: impl Stream<Item = MyRequest> + Send,
        responses: impl Sink<MyResponse> + Send,
    ) -> Result<(), ServerStatus>;
}

pub fn register_my_service(server: &mut Server, service: impl MyService) {
    let service = Arc::new(service);
    register_unary!(
        server,
        "/test.MyService/UnaryCall",
        service,
        unary_call,
        MyRequest,
        MyResponse
    );
    register_bidi!(
        server,
        "/test.MyService/StreamingCall",
        service,
        streaming_call,
        MyRequest,
        MyResponse
    );
}
