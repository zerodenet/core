//! Reusable opening of a stream through an already prepared relay prefix.
use crate::{RuntimeError, TcpRelayStream};
use std::{future::Future, pin::Pin, sync::Arc};

pub type RelayConnectFuture =
    Pin<Box<dyn Future<Output = Result<TcpRelayStream, RuntimeError>> + Send>>;
pub type RelayConnectFn = Arc<dyn Fn(String, u16) -> RelayConnectFuture + Send + Sync>;

#[derive(Clone)]
pub struct RelayStreamConnector {
    identity: String,
    generation: u64,
    open: RelayConnectFn,
    datagrams: Option<crate::OutboundDatagramSocketFactory>,
}
impl RelayStreamConnector {
    pub fn new(identity: String, generation: u64, open: RelayConnectFn) -> Self {
        Self {
            identity,
            generation,
            open,
            datagrams: None,
        }
    }
    pub fn with_datagrams(mut self, factory: crate::OutboundDatagramSocketFactory) -> Self {
        self.datagrams = Some(factory);
        self
    }
    pub fn datagrams(&self) -> Option<crate::OutboundDatagramSocketFactory> {
        self.datagrams.clone()
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn connect(&self, server: String, port: u16) -> RelayConnectFuture {
        (self.open)(server, port)
    }
}
