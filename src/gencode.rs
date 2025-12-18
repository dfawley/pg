use std::sync::LazyLock;

use crate::grpc::{Callable, MethodDescriptor, MethodType};
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

/* pub trait MyServiceClient {
    type UnaryCall<'a, ReqView, Res, ResView>: SharedCall + IntoFuture<Output = Result<Res, Status>>
    where
        Self: 'a,
        ReqView: Send + Sync + 'a,
        Res: Send + Sync + Default + 'static,
        ResView: Send + Sync + 'a;

    fn unary_call<'a>(
        &'a self,
        req: MyRequestView,
    ) -> Self::UnaryCall<'a, MyRequestView, MyResponse, MyResponseMut>;
}

 */

impl<C> MyServiceClientStub<C> {
    pub fn new(channel: C) -> Self {
        Self { channel }
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
        UnaryCall::new(
            &self.channel,
            MethodDescriptor {
                method_name: "unary_call".to_string(),
                message_encoder: ProtoEncoder::new(),
                message_decoder: ProtoDecoder::new(),
                method_type: MethodType::Unary,
            },
            req,
        )
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCall<'call, C, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send + 'static,
    {
        BidiCall::new(
            &self.channel,
            MethodDescriptor {
                method_name: "streaming_call".to_string(),
                message_encoder: ProtoEncoder::new(),
                message_decoder: ProtoDecoder::new(),
                method_type: MethodType::BidiStream,
            },
            req_stream,
        )
    }
}
