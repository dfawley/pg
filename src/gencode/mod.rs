use crate::grpc::Call;
use crate::grpc_protobuf::{BidiCallBuilder, UnaryCallBuilder};

mod server;

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use futures_core::Stream;
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
        UnaryCallBuilder::new(&self.channel, "unary_call".to_string(), req)
    }

    pub fn streaming_call<ReqStream>(
        &self,
        req_stream: ReqStream,
    ) -> BidiCallBuilder<'_, &C, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send,
    {
        BidiCallBuilder::new(&self.channel, "streaming_call".to_string(), req_stream)
    }
}
