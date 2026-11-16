//! Existing system TCP API over the same standard inbound Source host.
pub(crate) mod source;
use crate::runtime::Proxy;
use std::net::SocketAddr;
use zero_engine::EngineError;
impl Proxy {
    /// Start the existing OS-redirect TCP entry in the actual inbound host.
    /// The listener and accepted originals retire with this Proxy's Sources.
    pub async fn start_system_inbound(
        &self,
        listen_addr: SocketAddr,
        tag: &str,
    ) -> Result<(), EngineError> {
        self.start_system_tcp_source(listen_addr, tag).await
    }
}
