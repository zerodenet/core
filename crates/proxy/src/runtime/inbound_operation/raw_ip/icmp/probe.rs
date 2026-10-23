//! Original echo framing over either the legacy TUN socket or the granted pool.
use super::*;
use crate::component::instance::HostIcmpProbeGrant;
use zero_platform_tokio::{contract::Reason, controlled_icmp::ControlledIcmpSocket};

pub(super) async fn probe(
    packet: &[u8],
    mtu: u16,
    egress: Option<&EgressInterface>,
) -> std::io::Result<Option<Vec<u8>>> {
    let Some(bind) = bind_ip(packet) else {
        return Ok(None);
    };
    let mut socket = ProbeSocket::Legacy(IcmpSocket::bind(bind, egress)?);
    exchange(&mut socket, packet, mtu).await
}
pub(super) async fn owned(
    packet: &[u8],
    mtu: u16,
    egress: Option<&EgressInterface>,
    grant: Option<HostIcmpProbeGrant>,
) -> std::io::Result<Option<Vec<u8>>> {
    let grant = grant.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "raw-IP listener has no native ICMP authority",
        )
    })?;
    let Some(bind) = bind_ip(packet) else {
        return Ok(None);
    };
    // The sole canonical pool is installed and Live before this synchronous
    // acquisition. Every subsequent await retains its original in that pool.
    let mut socket =
        ProbeSocket::Controlled(grant.bind(bind, egress).map_err(std::io::Error::other)?);
    let operation = exchange(&mut socket, packet, mtu).await;
    let ProbeSocket::Controlled(view) = &socket else {
        unreachable!("same granted probe view")
    };
    let mut close_attempts = 0;
    let closed = loop {
        close_attempts += 1;
        match grant.close(view) {
            Ok(receipt) if receipt.matches(view) => break Ok(()),
            Ok(_) => {
                break Err(std::io::Error::other(
                    "ICMP close receipt differs from the original probe",
                ))
            }
            Err(error) if error.reason == Reason::Capacity && close_attempts < MAX_ECHO_PROBES => {
                // Busy means no close attempt touched the FD. Keep this exact
                // view and charged slot. At most the existing 64 concurrent
                // probe bound is retried; exhaustion returns the actual error
                // and leaves the original charged for resource shutdown.
                tokio::task::yield_now().await;
            }
            Err(error) => break Err(std::io::Error::other(error)),
        }
    };
    match (operation, closed) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(error),
        (Err(operation), Err(close)) => {
            Err(std::io::Error::other(ProbeFailure { operation, close }))
        }
    }
}
fn bind_ip(packet: &[u8]) -> Option<IpAddr> {
    Some(match packet::parse_icmp_echo_request(packet)?.destination {
        IpAddr::V4(_) => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(_) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    })
}
enum ProbeSocket {
    Legacy(IcmpSocket),
    Controlled(ControlledIcmpSocket),
}
impl ProbeSocket {
    async fn connect(&mut self, peer: IpAddr) -> std::io::Result<IpAddr> {
        match self {
            Self::Legacy(socket) => socket.connect(peer).await,
            Self::Controlled(socket) => socket.connect(peer).await,
        }
    }
    async fn send(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Legacy(socket) => socket.send(bytes).await,
            Self::Controlled(socket) => socket.send(bytes).await,
        }
    }
    fn reply_identifier(&self, sent: u16) -> std::io::Result<u16> {
        match self {
            Self::Legacy(socket) => socket.reply_identifier(sent),
            Self::Controlled(socket) => socket.reply_identifier(sent),
        }
    }
    async fn recv_from(&mut self, bytes: &mut [u8]) -> std::io::Result<(usize, IpAddr)> {
        match self {
            Self::Legacy(socket) => socket.recv_from(bytes).await,
            Self::Controlled(socket) => socket.recv_from(bytes).await,
        }
    }
}
async fn exchange(
    socket: &mut ProbeSocket,
    packet: &[u8],
    mtu: u16,
) -> std::io::Result<Option<Vec<u8>>> {
    let Some(request) = packet::parse_icmp_echo_request(packet) else {
        return Ok(None);
    };
    let local = socket.connect(request.destination).await?;
    let probe_id = NEXT_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let Some(message) = packet::build_icmp_echo_probe(&request, probe_id, local) else {
        return Ok(None);
    };
    socket.send(&message).await?;
    let reply_id = socket.reply_identifier(probe_id)?;
    let deadline = tokio::time::Instant::now() + ECHO_TIMEOUT;
    let mut buffer = vec![0_u8; usize::from(mtu).max(1_280)];
    loop {
        let received = tokio::time::timeout_at(deadline, socket.recv_from(&mut buffer)).await;
        let (size, source) = match received {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Ok(None),
        };
        if source != request.destination {
            continue;
        }
        if let Some(reply) = packet::build_icmp_echo_reply(
            &request,
            reply_id,
            &buffer[..size],
            local,
            usize::from(mtu),
        ) {
            return Ok(Some(reply));
        }
    }
}
#[derive(Debug)]
struct ProbeFailure {
    operation: std::io::Error,
    close: std::io::Error,
}
impl std::fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}; original ICMP close also failed: {}",
            self.operation, self.close
        )
    }
}
impl std::error::Error for ProbeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.operation)
    }
}
