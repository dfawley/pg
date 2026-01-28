use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::grpc::{Headers, RecvMessage, ResponseStreamItem, ServerResponseStreamItem, Status};

#[derive(Debug)]
pub struct ServerStatus(pub Status);

#[trait_variant::make(Send)]
pub trait Handle: Send + Sync {
    async fn handle(
        &self,
        _method: String,
        _headers: Headers,
        tx: impl ServerSendStream,
        rx: impl ServerRecvStream,
    );
}

#[trait_variant::make(Send)]
pub trait ServerRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Result<(), ()>;
}

#[trait_variant::make(Send)]
pub trait ServerSendStream {
    async fn send(&mut self, item: ServerResponseStreamItem);
}

pub trait RegisterMethod {
    fn register_method(&mut self, path: impl ToString, handler: impl Handle + 'static);
}

#[derive(Clone, Default)]
pub struct Server {
    pub(super) handlers: Arc<Mutex<HashMap<String, Arc<dyn GrpcHandle + 'static>>>>,
}

pub(super) struct GrpcServerSendStream {
    sent_headers: bool,
    pub(super) tx: mpsc::Sender<ResponseStreamItem<Vec<u8>>>,
}

impl GrpcServerSendStream {
    pub(super) fn new(tx: mpsc::Sender<ResponseStreamItem<Vec<u8>>>) -> Self {
        Self {
            sent_headers: false,
            tx,
        }
    }
}

impl ServerSendStream for GrpcServerSendStream {
    async fn send(&mut self, item: ServerResponseStreamItem<'_>) {
        let item = match item {
            ResponseStreamItem::Headers(headers) => {
                if !self.sent_headers {
                    self.sent_headers = true;
                    ResponseStreamItem::Headers(headers)
                } else {
                    // TODO: should always return this once there's an error.
                    ResponseStreamItem::StreamClosed
                }
            }
            ResponseStreamItem::Message(msg) => {
                if !self.sent_headers {
                    self.sent_headers = true;
                    self.tx
                        .send(ResponseStreamItem::Headers(Headers {}))
                        .await
                        .unwrap();
                }
                ResponseStreamItem::Message(msg.encode().pop().unwrap())
            }
            ResponseStreamItem::Trailers(trailers) => ResponseStreamItem::Trailers(trailers),
            ResponseStreamItem::StreamClosed => ResponseStreamItem::StreamClosed,
        };
        self.tx.send(item).await.unwrap();
    }
}

pub(super) struct GrpcServerRecvStream {
    pub(super) rx: mpsc::Receiver<Vec<u8>>,
}
impl ServerRecvStream for GrpcServerRecvStream {
    async fn next(&mut self, msg: &mut dyn RecvMessage) -> Result<(), ()> {
        let data = self.rx.recv().await.ok_or(())?;
        msg.decode(vec![data]);
        Ok(())
    }
}

impl Server {
    pub fn register(&mut self, service: impl RegisterOn) {
        service.register_on(self);
    }
}

impl RegisterMethod for Server {
    fn register_method(&mut self, path: impl ToString, handler: impl Handle + 'static) {
        self.handlers
            .lock()
            .unwrap()
            .insert(path.to_string(), Arc::new(handler));
    }
}

/// A convenience trait that services may implement to register themselves on a
/// server.
pub trait RegisterOn {
    /// Registers the service that implements this trait on server.
    fn register_on(self, server: &mut Server);
}

#[async_trait]
pub(super) trait GrpcHandle: Send + Sync {
    async fn grpc_handle(
        &self,
        method: String,
        headers: Headers,
        tx: GrpcServerSendStream,
        rx: GrpcServerRecvStream,
    );
}

#[async_trait]
impl<T: Handle> GrpcHandle for T {
    async fn grpc_handle(
        &self,
        method: String,
        headers: Headers,
        tx: GrpcServerSendStream,
        rx: GrpcServerRecvStream,
    ) {
        self.handle(method, headers, tx, rx).await
    }
}
