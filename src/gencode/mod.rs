use crate::grpc::Call;
use crate::grpc_protobuf::{BidiCallBuilder, UnaryCallBuilder};

pub mod server;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use pb::*;
use protobuf::AsView;

#[derive(Clone)]
pub struct MyServiceClientStub<C> {
    channel: C,
}

impl<C> MyServiceClientStub<C> {
    pub fn new(channel: C) -> Self {
        Self { channel }
    }
}

impl<C: Call> MyServiceClientStub<C> {
    pub fn unary_call<ReqMsgView>(
        &self,
        req: ReqMsgView,
    ) -> UnaryCallBuilder<'_, &C, MyResponse, ReqMsgView>
    where
        ReqMsgView: AsView<Proxied = MyRequest> + Send + Sync,
    {
        UnaryCallBuilder::new(&self.channel, "/test.MyService/UnaryCall", req)
    }

    pub fn streaming_call(&self) -> BidiCallBuilder<&C, MyRequest, MyResponse> {
        BidiCallBuilder::new(&self.channel, "/test.MyService/StreamingCall")
    }
}
