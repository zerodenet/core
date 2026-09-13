//! Shared QUIC client TLS and endpoint construction. Protocols select policy.
use crate::RuntimeError;
use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

pub fn client_config(
    insecure: bool,
    fingerprint: Option<&str>,
    alpn: &[Vec<u8>],
    datagram_receive_buffer_size: Option<usize>,
) -> Result<quinn::ClientConfig, RuntimeError> {
    client_config_with_ca(
        insecure,
        fingerprint,
        alpn,
        datagram_receive_buffer_size,
        None,
    )
}

pub fn client_config_with_ca(
    insecure: bool,
    fingerprint: Option<&str>,
    alpn: &[Vec<u8>],
    datagram_receive_buffer_size: Option<usize>,
    ca_cert_path: Option<&std::path::Path>,
) -> Result<quinn::ClientConfig, RuntimeError> {
    client_config_with_options(
        insecure,
        fingerprint,
        alpn,
        datagram_receive_buffer_size,
        ca_cert_path,
        &Default::default(),
    )
}

pub fn client_config_with_options(
    insecure: bool,
    fingerprint: Option<&str>,
    alpn: &[Vec<u8>],
    datagram_receive_buffer_size: Option<usize>,
    ca_cert_path: Option<&std::path::Path>,
    options: &zero_traits::ClientTlsOptions,
) -> Result<quinn::ClientConfig, RuntimeError> {
    let profile = crate::profile::OwnedClientTlsProfile {
        options: options.clone(),
        server_name: None,
        disable_sni: false,
        ca_cert_path: ca_cert_path.map(|p| p.to_string_lossy().into_owned()),
        insecure,
        alpn: Vec::new(),
        client_fingerprint: fingerprint.map(str::to_owned),
    };
    let mut tls = crate::tls::config::client(&profile, None, true)?;
    tls.alpn_protocols = alpn.to_vec();
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).map_err(io::Error::other)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(std::time::Duration::from_secs(30).try_into().unwrap()));
    transport.datagram_receive_buffer_size(datagram_receive_buffer_size);
    config.transport_config(Arc::new(transport));
    Ok(config)
}

pub async fn connect_quic_endpoint(
    server: &str,
    port: u16,
    server_name: &str,
    client_config: quinn::ClientConfig,
    sockets: &crate::OutboundDatagramSocketFactory,
) -> Result<quinn::Connection, RuntimeError> {
    connect_quic_endpoint_masked(
        server,
        port,
        server_name,
        client_config,
        sockets,
        sockets.final_mask().udp(),
    )
    .await
}

pub async fn connect_quic_endpoint_masked(
    server: &str,
    port: u16,
    server_name: &str,
    client_config: quinn::ClientConfig,
    sockets: &crate::OutboundDatagramSocketFactory,
    masks: &[crate::finalmask::udp::Mask],
) -> Result<quinn::Connection, RuntimeError> {
    if sockets.is_relay()
        && masks
            .iter()
            .any(|mask| matches!(mask, crate::finalmask::udp::Mask::Xicmp { .. }))
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "raw ICMP cannot bypass a datagram relay",
        )
        .into());
    }
    let server_addrs = sockets
        .resolve_server_addresses(server, port)
        .await
        .map_err(|error| {
            RuntimeError::Io(io::Error::new(
                error.kind(),
                format!("quic resolve {server}:{port}: {error}"),
            ))
        })?;
    let mut last_error = None;

    for server_addr in server_addrs {
        let bind_addr = wildcard_bind_addr(server_addr);
        let socket = match sockets.open_socket(server_addr).await {
            Ok(socket) => socket,
            Err(error) => {
                last_error = Some(format!("bind {bind_addr} for {server_addr}: {error}"));
                continue;
            }
        };
        let socket = crate::finalmask::Socket::wrap_with_egress(
            socket,
            masks,
            false,
            sockets.egress_for(server_addr).as_ref(),
        )?;
        let mut endpoint = match quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            None,
            socket,
            Arc::new(quinn::TokioRuntime),
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                last_error = Some(format!("create endpoint for {server_addr}: {error}"));
                continue;
            }
        };
        endpoint.set_default_client_config(client_config.clone());

        let connecting = match endpoint.connect(server_addr, server_name) {
            Ok(connecting) => connecting,
            Err(error) => {
                last_error = Some(format!("connect {server_addr}: {error}"));
                continue;
            }
        };
        match connecting.await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                last_error = Some(format!("connect {server_addr}: {error}"));
            }
        }
    }

    Err(RuntimeError::Io(io::Error::other(format!(
        "quic connection to {server}:{port} failed: {}",
        last_error.unwrap_or_else(|| "no resolved address was connectable".to_owned())
    ))))
}

fn wildcard_bind_addr(server_addr: SocketAddr) -> SocketAddr {
    match server_addr {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
    }
}

#[cfg(test)]
#[path = "tests/client.rs"]
mod tests;
