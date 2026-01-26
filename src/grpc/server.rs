use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::grpc::{Headers, RecvMessage, ServerResponseStreamItem, Status};

pub struct ServerStatus(pub Status);

#[trait_variant::make(Send)]
pub trait Handle: Send + Sync {
    fn handle(
        &self,
        _method: String,
        _headers: Headers,
        tx: impl ServerSendStream,
        rx: impl ServerRecvStream,
    ) -> impl Future<Output = ()> + Send;
}

#[trait_variant::make(Send)]
pub trait ServerRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> ServerResponseStreamItem;
}

#[trait_variant::make(Send)]
pub trait ServerSendStream {
    async fn send(&mut self, msg: ServerResponseStreamItem);
}

pub trait Register {
    fn register(&mut self, path: impl ToString, handler: impl Handle + 'static);
}

#[derive(Clone, Default)]
pub struct Server {
    methods: Arc<Mutex<HashMap<String, Box<dyn GrpcHandle>>>>,
}

struct GrpcServerSendStream {}
impl ServerSendStream for GrpcServerSendStream {
    async fn send(&mut self, _msg: ServerResponseStreamItem<'_>) {}
}

struct GrpcServerRecvStream {}
impl ServerRecvStream for GrpcServerRecvStream {
    async fn next(&mut self, _msg: &mut dyn RecvMessage) -> ServerResponseStreamItem {
        todo!()
    }
}

impl Register for Server {
    fn register(&mut self, path: impl ToString, handler: impl Handle + 'static) {
        self.methods
            .lock()
            .unwrap()
            .insert(path.to_string(), Box::new(handler));
    }
}

#[async_trait]
trait GrpcHandle: Send + Sync {
    async fn invoke(
        &self,
        method: String,
        headers: Headers,
        tx: GrpcServerSendStream,
        rx: GrpcServerRecvStream,
    );
}

#[async_trait]
impl<T: Handle> GrpcHandle for T {
    async fn invoke(
        &self,
        method: String,
        headers: Headers,
        tx: GrpcServerSendStream,
        rx: GrpcServerRecvStream,
    ) {
        self.handle(method, headers, tx, rx).await
    }
}
