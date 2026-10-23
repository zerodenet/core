//! Runtime-executed preparation for an external inbound carrier.
use crate::RuntimeError;
use std::{future::Future, pin::Pin, sync::Arc};
use zero_platform_tokio::TokioListener;

pub type CarrierFuture<T> = Pin<Box<dyn Future<Output = Result<T, RuntimeError>> + Send>>;
pub struct InboundCarrier {
    pub listener: TokioListener,
    pub datagram: Arc<tokio::net::UdpSocket>,
    /// Observes carrier exit. Dropping this future does not confirm cleanup;
    /// the separately retained resource owner must still reap its child.
    pub completion: CarrierFuture<()>,
}
pub trait InboundCarrierPlan: Send + 'static {
    /// The host installs this owner before activation can suspend. It shares
    /// the actual protocol state used by activate, rather than a monitor copy.
    fn owner(&self) -> Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>;
    fn activate(self: Box<Self>, listener: TokioListener) -> CarrierFuture<InboundCarrier>;
}
