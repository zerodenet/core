use std::net::{IpAddr, SocketAddr};
use tokio::net::{TcpListener, UdpSocket};
use zero_platform_tokio::{
    validate_dial_policy, EgressBindingReason, EgressInterfaceControl, TokioDatagramSocket,
    TokioSocket,
};
use zero_traits::{AddressFamily, DialPolicy};
fn source_policy(source: &str) -> DialPolicy {
    DialPolicy {
        source_ip: Some(source.parse().unwrap()),
        ..Default::default()
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn loopback_policy() -> DialPolicy {
    DialPolicy {
        interface: Some(
            if cfg!(target_os = "linux") {
                "lo"
            } else {
                "lo0"
            }
            .into(),
        ),
        ..source_policy("127.0.0.1")
    }
}
#[path = "dial_policy/selection.rs"]
mod selection;
#[path = "dial_policy/socket.rs"]
mod socket;
