use crate::validation::PluginConfig;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    process::Stdio,
    time::Duration,
};
use tokio::process::{Child, Command};

pub(super) fn loopback(host: &str) -> IpAddr {
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V6(_)) => Ipv6Addr::LOCALHOST.into(),
        _ => Ipv4Addr::LOCALHOST.into(),
    }
}
fn command(config: &PluginConfig, remote: (&str, u16), local: SocketAddr, server: bool) -> Command {
    let mut command = Command::new(&config.command);
    if config.command == "obfsproxy" {
        let remote = if remote.0.contains(':') {
            format!("[{}]:{}", remote.0, remote.1)
        } else {
            format!("{}:{}", remote.0, remote.1)
        };
        let data_dir = std::env::temp_dir().join(format!("zero-obfsproxy-{}", local.port()));
        command.arg("--data-dir").arg(data_dir);
        if let Some(options) = &config.options {
            command.args(options.split(' '));
        }
        if server {
            command
                .arg("--dest")
                .arg(local.to_string())
                .arg("server")
                .arg(remote);
        } else {
            command
                .arg("--dest")
                .arg(remote)
                .arg("client")
                .arg(local.to_string());
        }
        command
            .args(&config.args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        return command;
    }
    command
        .args(&config.args)
        .env("SS_REMOTE_HOST", remote.0)
        .env("SS_REMOTE_PORT", remote.1.to_string())
        .env("SS_LOCAL_HOST", local.ip().to_string())
        .env("SS_LOCAL_PORT", local.port().to_string())
        .env_remove("SS_PLUGIN_OPTIONS")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    if let Some(options) = &config.options {
        command.env("SS_PLUGIN_OPTIONS", options);
    }
    command
}
pub(super) fn spawn(
    config: &PluginConfig,
    remote: (&str, u16),
    local: SocketAddr,
    server: bool,
) -> io::Result<Child> {
    command(config, remote, local, server).spawn()
}
pub(super) async fn ready(child: &mut Child, endpoint: SocketAddr, tcp: bool) -> io::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if child.try_wait()?.is_some() {
            return Err(io::Error::other("shadowsocks plugin exited during startup"));
        }
        let tcp_ready = !tcp
            || tokio::time::timeout_at(deadline, tokio::net::TcpStream::connect(endpoint))
                .await
                .is_ok_and(|value| value.is_ok());
        // SIP003u has no UDP readiness handshake. Match the reference: only TCP
        // readiness is probed. Binding the child's UDP endpoint to test readiness
        // would race its own bind and can make a healthy plugin fail to start.
        if tcp_ready {
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
