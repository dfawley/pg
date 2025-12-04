#![cfg(feature = "protobuf")]
use crate::grpc::RequestMessage;
use protobuf::{AsView, Proxied};

impl<T: AsView + Send> RequestMessage for T {
    type View<'a>
        = <T::Proxied as Proxied>::View<'a>
    where
        T: 'a;

    fn as_view(&self) -> Self::View<'_> {
        self.as_view()
    }
}
