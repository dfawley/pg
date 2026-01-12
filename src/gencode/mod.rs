use crate::grpc::{Call, CallOnce, MethodDescriptor, MethodType};
use crate::grpc_protobuf::{BidiCall, UnaryCall};

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
    ) -> UnaryCall<'call, impl CallOnce, MyResponse, ReqMsgView>
    where
        ReqMsgView: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        UnaryCall::new(
            &self.channel,
            MethodDescriptor {
                method_name: "unary_call".to_string(),
                method_type: MethodType::Unary,
            },
            req,
        )
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCall<'call, impl CallOnce, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send + 'static,
    {
        BidiCall::new(
            &self.channel,
            MethodDescriptor {
                method_name: "streaming_call".to_string(),
                method_type: MethodType::BidiStream,
            },
            req_stream,
        )
    }
}
