use crate::grpc::{Channel, MethodDescriptor, MethodType};
use crate::grpc_protobuf::{ProtoDecoder, ProtoEncoder, UnaryCall};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use futures_core::Stream;
use pb::*;
use protobuf::{AsMut, AsView};

#[derive(Clone)]
pub struct MyServiceClientStub {
    channel: Channel,
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

impl MyServiceClientStub {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }
}

static DESC: MethodDescriptor<ProtoEncoder<MyRequest>, ProtoDecoder<MyResponse>> =
    MethodDescriptor {
        method_name: "unary_call",
        message_encoder: ProtoEncoder::new(),
        message_decoder: ProtoDecoder::new(),
        method_type: MethodType::Unary,
    };

impl MyServiceClientStub {
    pub fn unary_call<'stub: 'call, 'call, R>(
        &'stub self,
        req: R,
    ) -> UnaryCall<'call, ProtoEncoder<MyRequest>, ProtoDecoder<MyResponse>, R>
    where
        R: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        UnaryCall::new(&self.channel, &DESC, req)
    }

    /*
        pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
            &'stub self,
            req_stream: ReqStream,
        ) -> BidiCall<'call, ReqStream, MyResponse>
        where
            ReqStream: Unpin + Stream<Item = MyRequest /*View<'call>*/> + Send + 'static,
        {
            BidiCall::new(&self.channel, req_stream)
        }
    */
}
