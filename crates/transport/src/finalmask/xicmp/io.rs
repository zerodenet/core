//! The owned path consumes the unique original provider view; no bind or clone.
use super::*;
pub(super) enum Socket {
    Legacy(Arc<IcmpSocket>),
    Hosted(zero_platform_tokio::controlled_icmp::ControlledIcmpSocket),
}
impl Socket {
    pub(super) async fn send_to(&mut self, bytes: &[u8], peer: IpAddr) -> io::Result<usize> {
        match self {
            Self::Legacy(s) => s.send_to(bytes, peer).await,
            Self::Hosted(s) => s.send_to(bytes, peer).await,
        }
    }
    pub(super) async fn recv_from(&mut self, bytes: &mut [u8]) -> io::Result<(usize, IpAddr)> {
        match self {
            Self::Legacy(s) => s.recv_from(bytes).await,
            Self::Hosted(s) => s.recv_from(bytes).await,
        }
    }
}
