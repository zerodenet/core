//! Runtime-executed preparation for an external inbound carrier.
use crate::RuntimeError;
use std::{future::Future, pin::Pin, sync::Arc};
use zero_platform_tokio::TokioListener;

pub type CarrierFuture<T> = Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + Send>>;
pub struct InboundCarrier {
    pub listener: TokioListener,
    pub datagram: Arc<tokio::net::UdpSocket>,
    /// Completes if the carrier exits. Dropping this future closes its resources.
    pub completion: CarrierFuture<()>,
}
pub trait InboundCarrierPlan: Send + 'static {
    fn activate(self: Box<Self>, listener: TokioListener) -> CarrierFuture<InboundCarrier>;
}
