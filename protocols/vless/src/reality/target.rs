// SPDX-License-Identifier: MPL-2.0
// REALITY behavior follows XTLS/REALITY 9234c772ba8f, as pinned by Xray-core v26.3.27.
//! REALITY target mirroring and authenticated handshake selection.
mod probe;
mod shape;
use super::{
    hello,
    reality_server_connection::{RealityServerConfig, RealityServerConnection},
    stream::{feed_reality_server_connection, perform_reality_server_handshake, RealityTlsStream},
};
pub(crate) use probe::{Access as ProbeAccess, Registry as ProbeRegistry};
pub(crate) use shape::Shape;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::TcpRelayStream;
use zero_transport::handshake_target::{Connector, RateLimit};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Profile {
    pub endpoint: zero_traits::FallbackEndpoint,
    pub proxy_protocol: u8,
    pub upload: RateLimit,
    pub download: RateLimit,
}
impl Profile {
    pub fn resolve_path(&mut self, base: Option<&std::path::Path>) {
        if let (Some(base), zero_traits::FallbackEndpoint::Unix { path }) =
            (base, &mut self.endpoint)
        {
            if !std::path::Path::new(path).is_absolute() {
                *path = base.join(&*path).to_string_lossy().into_owned();
            }
        }
    }
}
pub enum Acceptance {
    Established(RealityTlsStream<TcpRelayStream>),
    Forward(Box<dyn zero_core::inbound::InboundControlSession>),
}

pub(super) async fn accept(
    mut client: TcpRelayStream,
    config: RealityServerConfig,
    profile: &Profile,
    connector: &Connector,
    probes: &ProbeAccess,
) -> io::Result<Acceptance> {
    use zero_platform_tokio::ClientStream;
    let timeout = std::time::Duration::from_millis(config.handshake_timeout_ms);
    let mut target = tokio::time::timeout(timeout, connector.connect(profile.endpoint.clone()))
        .await
        .map_err(|_| timed_out())??;
    let prefix = zero_transport::proxy_protocol::encode(
        profile.proxy_protocol,
        client.peer_addr().ok(),
        client.local_addr().ok(),
    )?;
    tokio::io::AsyncWriteExt::write_all(&mut target, &prefix).await?;
    let mut client_saved = Vec::new();
    let mut target_saved = Vec::new();
    let selected = tokio::time::timeout(
        timeout,
        select(
            &mut client,
            &mut target,
            &config,
            &mut client_saved,
            &mut target_saved,
        ),
    )
    .await
    .map_err(|_| timed_out())??;
    let Some((shape, server_name, alpn)) = selected else {
        return Ok(Acceptance::Forward(
            zero_transport::handshake_target::forward_session(
                client,
                target,
                target_saved,
                profile.upload,
                profile.download,
            ),
        ));
    };
    let detection = probes.detect(profile, connector, &server_name, alpn).await;
    let shape = shape.with_detection(detection.post_handshake_lengths, detection.max_ccs_records);
    let mut connection = RealityServerConnection::new(config).with_target_shape(shape);
    feed_reality_server_connection(&mut connection, &client_saved)?;
    connection.process_new_packets()?;
    // The real target remains connected until authentication completes. An early
    // close must terminate this attempt rather than manufacturing a live target.
    let result = tokio::time::timeout(timeout, async {
        tokio::select! {
            result = perform_reality_server_handshake(&mut connection,&mut client) => result,
            result = watch_target(&mut target) => result,
        }
    })
    .await
    .map_err(|_| timed_out())?;
    result?;
    Ok(Acceptance::Established(RealityTlsStream::new_server(
        client, connection,
    )))
}
fn timed_out() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "REALITY target handshake timed out",
    )
}
async fn watch_target(target: &mut TcpRelayStream) -> io::Result<()> {
    let mut buffer = [0; 8192];
    while target.read(&mut buffer).await? != 0 {}
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "REALITY target closed during handshake",
    ))
}
async fn select(
    client: &mut TcpRelayStream,
    target: &mut TcpRelayStream,
    config: &RealityServerConfig,
    client_saved: &mut Vec<u8>,
    target_saved: &mut Vec<u8>,
) -> io::Result<Option<(Shape, String, probe::AlpnClass)>> {
    let mut client_buffer = [0; 8192];
    let mut target_buffer = [0; 8192];
    let mut authenticated = false;
    let mut probe_key = None;
    loop {
        tokio::select! {
            read = client.read(&mut client_buffer), if !authenticated => {
                let count = read?;
                if count==0 { return Ok(None); }
                target.write_all(&client_buffer[..count]).await?;
                client_saved.extend_from_slice(&client_buffer[..count]);
                if client_saved.len()>128*1024 { return Ok(None); }
                match hello::assembled(client_saved) {
                    Ok(Some((record,_))) => {
                        if hello::authenticate(config,&record).is_err() {return Ok(None);}
                        let metadata = ztls::hello::client_hello_metadata(&record)?;
                        let server_name = metadata.server_name.unwrap_or_else(|| config.server_name.clone());
                        let alpn = probe::AlpnClass::from_offered(&metadata.alpn);
                        probe_key = Some((server_name, alpn));
                        authenticated = true;
                    }
                    Ok(None) => {}
                    Err(_) => return Ok(None),
                }
            }
            read = target.read(&mut target_buffer) => {
                let count = read?;
                if count==0 {return Ok(None);}
                target_saved.extend_from_slice(&target_buffer[..count]);
                if !authenticated || target_saved.len()>16*1024 {return Ok(None);}
                match Shape::inspect(target_saved) {
                    Ok(Some(shape)) => {
                        let (server_name, alpn) = probe_key
                            .take()
                            .ok_or_else(hello::invalid)?;
                        return Ok(Some((shape, server_name, alpn)));
                    }
                    Ok(None) => {}
                    Err(_) => return Ok(None),
                }
            }
        }
    }
}
