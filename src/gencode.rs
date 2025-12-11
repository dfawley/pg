use crate::grpc::Channel;
use crate::grpc_protobuf::{BidiCall, UnaryCall};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use futures_core::Stream;
use pb::*;
use protobuf::AsView;

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

impl MyServiceClientStub {
    pub fn unary_call<'stub: 'call, 'call, R>(
        &'stub self,
        req: R,
    ) -> UnaryCall<'call, R, MyResponse, MyResponseMut<'call>>
    where
        R: AsView<Proxied = MyRequest> + Send + Sync + 'call,
    {
        UnaryCall::new(&self.channel, req)
    }

    pub fn streaming_call<'stub: 'call, 'call, ReqStream>(
        &'stub self,
        req_stream: ReqStream,
    ) -> BidiCall<'call, ReqStream, MyResponse>
    where
        ReqStream: Unpin + Stream<Item = MyRequest /*View<'call>*/> + Send + 'static,
    {
        BidiCall::new(&self.channel, req_stream)
    }
}
