use std::sync::LazyLock;

use crate::grpc::{Callable, Channel, MethodDescriptor, MethodType};
use crate::grpc_protobuf::{BidiCall, ProtoDecoder, ProtoEncoder, UnaryCall};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use futures_core::Stream;
use pb::*;
use protobuf::{AsMut, AsView};

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

static UNARY_CALL_DESC: LazyLock<
    MethodDescriptor<ProtoEncoder<MyRequest>, ProtoDecoder<MyResponse>>,
> = LazyLock::new(|| MethodDescriptor {
    method_name: "unary_call".to_string(),
    message_encoder: ProtoEncoder::new(),
    message_decoder: ProtoDecoder::new(),
    method_type: MethodType::Unary,
});

static STREAMING_CALL_DESC: LazyLock<
    MethodDescriptor<ProtoEncoder<MyRequest>, ProtoDecoder<MyResponse>>,
> = LazyLock::new(|| MethodDescriptor {
    method_name: "streaming_call".to_string(),
    message_encoder: ProtoEncoder::new(),
    message_decoder: ProtoDecoder::new(),
    method_type: MethodType::BidiStream,
});

impl<C: Callable> MyServiceClientStub<C> {
    pub fn unary_call<'stub: 'call, 'call, R>(
        &'stub self,
        req: R,
    ) -> UnaryCall<'call, C, MyRequest, MyResponse, R>
    where
        R: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        UnaryCall::new(&self.channel, &UNARY_CALL_DESC, req)
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCall<'call, C, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest> + Send + 'static,
    {
        BidiCall::new(&self.channel, &STREAMING_CALL_DESC, req_stream)
    }
}
