use crate::validation::PluginConfig;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::process::Child;

pub(super) fn loopback(host: &str) -> IpAddr {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => Ipv6Addr::LOCALHOST.into(),
        _ => Ipv4Addr::LOCALHOST.into(),
    }
}
pub(super) fn request(
    config: &PluginConfig,
    remote: (&str, u16),
    local: SocketAddr,
    server: bool,
) -> zero_platform_tokio::contract::component::ProcessRequest {
    use std::ffi::OsString;
    use zero_platform_tokio::contract::component::ProcessRequest;
    let mut request = ProcessRequest::new(&config.command);
    if config.command == "obfsproxy" {
        let remote = if remote.0.contains(':') {
            format!("[{}]:{}", remote.0, remote.1)
        } else {
            format!("{}:{}", remote.0, remote.1)
        };
        let data_dir = std::env::temp_dir().join(format!("zero-obfsproxy-{}", local.port()));
        request
            .arguments
            .extend([OsString::from("--data-dir"), data_dir.into_os_string()]);
        if let Some(options) = &config.options {
            request
                .arguments
                .extend(options.split(' ').map(OsString::from));
        }
        if server {
            request.arguments.extend([
                "--dest".into(),
                local.to_string().into(),
                "server".into(),
                remote.into(),
            ]);
        } else {
            request.arguments.extend([
                "--dest".into(),
                remote.into(),
                "client".into(),
                local.to_string().into(),
            ]);
        }
        request
            .arguments
            .extend(config.args.iter().map(OsString::from));
        return request;
    }
    request
        .arguments
        .extend(config.args.iter().map(OsString::from));
    for (name, value) in [
        ("SS_REMOTE_HOST", remote.0.to_owned()),
        ("SS_REMOTE_PORT", remote.1.to_string()),
        ("SS_LOCAL_HOST", local.ip().to_string()),
        ("SS_LOCAL_PORT", local.port().to_string()),
    ] {
        request.environment.insert(name.into(), Some(value.into()));
    }
    request.environment.insert(
        "SS_PLUGIN_OPTIONS".into(),
        config.options.as_ref().map(OsString::from),
    );
    request
}
pub(super) fn spawn_request(
    request: zero_platform_tokio::contract::component::ProcessRequest,
) -> io::Result<Child> {
    request.command().spawn()
}
pub(super) fn spawn(
    config: &PluginConfig,
    remote: (&str, u16),
    local: SocketAddr,
    server: bool,
) -> io::Result<Child> {
    spawn_request(request(config, remote, local, server))
}
pub(super) async fn ready(child: &mut Child, endpoint: SocketAddr, tcp: bool) -> io::Result<()> {
    ready_with_status(|| child.try_wait(), endpoint, tcp).await
}

pub(super) async fn ready_with_status<Status>(
    status: Status,
    endpoint: SocketAddr,
    tcp: bool,
) -> io::Result<()>
where
    Status: FnMut() -> io::Result<Option<std::process::ExitStatus>>,
{
    ready_with_probe(status, endpoint, tcp, || {
        tokio::net::TcpStream::connect(endpoint)
    })
    .await
}

pub(super) async fn ready_with_probe<Status, Probe, Pending>(
    mut status: Status,
    endpoint: SocketAddr,
    tcp: bool,
    mut probe: Probe,
) -> io::Result<()>
where
    Status: FnMut() -> io::Result<Option<std::process::ExitStatus>>,
    Probe: FnMut() -> Pending,
    Pending: std::future::Future<Output = io::Result<tokio::net::TcpStream>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if status()?.is_some() {
            return Err(io::Error::other("shadowsocks plugin exited during startup"));
        }
        let tcp_ready = !tcp
            || tokio::time::timeout_at(deadline, probe())
                .await
                .is_ok_and(|value| {
                    value.is_ok_and(|stream| {
                        match (stream.local_addr(), stream.peer_addr()) {
                            (Ok(local), Ok(peer)) if local != peer && peer == endpoint => true,
                            (Ok(local), Ok(peer)) => {
                                tracing::debug!(%local, %peer, %endpoint, "plugin startup probe reached an unexpected socket");
                                false
                            }
                            _ => false,
                        }
                    })
                });
        // SIP003u has no UDP readiness handshake. Match the reference: only TCP
        // readiness is probed. Binding the child's UDP endpoint to test readiness
        // would race its own bind and can make a healthy plugin fail to start.
        if tcp_ready {
            if status()?.is_some() {
                return Err(io::Error::other("shadowsocks plugin exited during startup"));
            }
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "shadowsocks plugin startup timed out",
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(all(test, unix))]
#[path = "../../../tests/plugin/process.rs"]
mod tests;
