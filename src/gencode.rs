use crate::grpc::{Channel, UnaryCall};

pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/protobuf_generated/generated.rs"));
}
use pb::*;
use protobuf::{AsView, IntoView, Proxied};

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
    pub fn unary_call<'stub: 'call, 'call>(
        &'stub self,
        req: impl AsView<Proxied = MyRequest>, // MyRequestView<'call>,
    ) -> UnaryCall<'call, MyRequestView<'call>, MyResponse, MyResponseMut<'call>> {
        UnaryCall::new(&self.channel, req.as_view())
    }
}
