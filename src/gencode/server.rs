use std::future::ready;
use std::sync::Arc;

use futures::Sink;
use futures_core::Stream;
use protobuf::{MutProxied, Proxied};

use crate::grpc::{RegisterOn, Server, ServerStatus, Status};
use crate::grpc_protobuf::{BidiHandle, BidiHandler, UnaryHandler};
use crate::{register_bidi, register_unary};

use super::pb::*;

#[trait_variant::make(Send)]
pub trait MyService: Send + Sync + 'static {
    async fn unary_call(&self, _req: MyRequestView<'_>, _res: MyResponseMut<'_>) -> ServerStatus {
        ready(ServerStatus(Status {
            code: 12,
            _msg: "unary_call not implemented".to_string(),
        }))
    }

    async fn streaming_call(
        &self,
        _requests: impl Stream<Item = MyRequest> + Send,
        _responses: impl Sink<MyResponse> + Send,
    ) -> ServerStatus {
        ready(ServerStatus(Status {
            code: 12,
            _msg: "streaming_call not implemented".to_string(),
        }))
    }
}

impl<T: MyService> RegisterOn for T {
    fn register_on(self, server: &mut Server) {
        let service = Arc::new(self);
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
}
/* OR
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
*/
