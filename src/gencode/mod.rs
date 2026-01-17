use crate::grpc::{Call, CallOnce};
use crate::grpc_protobuf::{BidiCallBuilder, UnaryCallBuilder};

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
    pub fn unary_call<'stub: 'call, 'call, ReqMsgView>(
        &'stub self,
        req: ReqMsgView,
    ) -> UnaryCallBuilder<'call, impl CallOnce, MyResponse, ReqMsgView>
    where
        ReqMsgView: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        UnaryCallBuilder::new(&self.channel, "unary_call".to_string(), req)
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCallBuilder<'call, impl CallOnce, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send + 'static,
    {
        BidiCallBuilder::new(&self.channel, "streaming_call".to_string(), req_stream)
    }
}
