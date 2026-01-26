use crate::{
    gencode::{pb::*, server::MyService},
    grpc::ServerStatus,
};

pub struct MyServiceImpl {}

impl MyService for MyServiceImpl {
    async fn unary_call(
        &self,
        req: MyRequestView<'_>,
        mut res: MyResponseMut<'_>,
    ) -> Result<(), ServerStatus> {
        res.set_result(req.query());
        Ok(())
    }
}
