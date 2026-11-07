//! Concrete per-connection HTTP/3 work; no owner, task or native authority.
use std::num::NonZeroUsize;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hysteria2H3QueueLimits {
    classifications: NonZeroUsize,
    tcp_streams: NonZeroUsize,
    web_streams: NonZeroUsize,
    web_requests: NonZeroUsize,
}
impl Default for Hysteria2H3QueueLimits {
    fn default() -> Self {
        Self {
            classifications: NonZeroUsize::new(128).unwrap(),
            tcp_streams: NonZeroUsize::new(128).unwrap(),
            web_streams: NonZeroUsize::new(128).unwrap(),
            web_requests: NonZeroUsize::new(64).unwrap(),
        }
    }
}
impl Hysteria2H3QueueLimits {
    pub fn new(
        classifications: NonZeroUsize,
        tcp_streams: NonZeroUsize,
        web_streams: NonZeroUsize,
        web_requests: NonZeroUsize,
    ) -> Result<Self, zero_transport::RuntimeError> {
        if [classifications, tcp_streams, web_streams, web_requests]
            .into_iter()
            .any(|n| n.get() > 4096)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "HY2 HTTP/3 queue limits exceed 4096",
            )
            .into());
        }
        Ok(Self {
            classifications,
            tcp_streams,
            web_streams,
            web_requests,
        })
    }
    pub fn classifications(self) -> NonZeroUsize {
        self.classifications
    }
    pub fn tcp_streams(self) -> NonZeroUsize {
        self.tcp_streams
    }
    pub fn web_streams(self) -> NonZeroUsize {
        self.web_streams
    }
    pub fn web_requests(self) -> NonZeroUsize {
        self.web_requests
    }
}
