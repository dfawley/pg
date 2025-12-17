use std::sync::LazyLock;

use crate::grpc::{Callable, Interceptor, MethodDescriptor, MethodType};
use crate::grpc_protobuf::{BidiCall, ProtoDecoder, ProtoEncoder, UnaryCall};

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

impl<C: Callable + Clone> MyServiceClientStub<C> {
    pub fn new(channel: C) -> Self {
        Self { channel }
    }

    pub fn with_interceptor<I: Interceptor<C>>(
        &self,
        interceptor: I,
    ) -> MyServiceClientStub<I::Out> {
        MyServiceClientStub {
            channel: interceptor.wrap(self.channel.clone()),
        }
    }
}

impl<C: Callable> MyServiceClientStub<C> {
    pub fn unary_call<'stub: 'call, 'call, ReqMsgView>(
        &'stub self,
        req: ReqMsgView,
    ) -> UnaryCall<'call, C, MyRequest, MyResponse, ReqMsgView>
    where
        ReqMsgView: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        let desc = MethodDescriptor {
            method_name: "unary_call".to_string(),
            message_encoder: ProtoEncoder::new(),
            message_decoder: ProtoDecoder::new(),
            method_type: MethodType::Unary,
        };
        UnaryCall::new(&self.channel, desc, req)
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCall<'call, C, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send + 'static,
    {
        let desc = MethodDescriptor {
            method_name: "streaming_call".to_string(),
            message_encoder: ProtoEncoder::new(),
            message_decoder: ProtoDecoder::new(),
            method_type: MethodType::BidiStream,
        };
        BidiCall::new(&self.channel, desc, req_stream)
    }
}
