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
    // Keep an independent interface-name oracle for the Unix selection tests.
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
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn loopback_policy() -> DialPolicy {
    // Only publish an in-memory topology. No host route or interface is changed.
    // Source-only TUN safety identifies the unique owner of the loopback source,
    // including Windows, whose adapter alias can be localized.
    let control = EgressInterfaceControl::default();
    control.replace_tunnel_addresses(["192.0.2.1".parse().unwrap()]);
    let source = source_policy("127.0.0.1");
    let selection = control
        .select_for_peer_with_policy("127.0.0.1:80".parse().unwrap(), &source)
        .expect("native dial acceptance requires a discoverable loopback interface");
    DialPolicy {
        interface: Some(
            selection
                .interface()
                .expect("source owner must be pinned")
                .name()
                .to_owned(),
        ),
        ..source
    }
}
#[path = "dial_policy/selection.rs"]
mod selection;
#[path = "dial_policy/socket.rs"]
mod socket;
