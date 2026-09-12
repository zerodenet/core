use zero_core::Session;
use zero_transport::RuntimeError;
use zero_transport::{MeteredStream, StreamTraffic, TcpRelayStream};

pub(super) async fn establish_with_replay(
    mut stream: MeteredStream<TcpRelayStream>,
    session: &Session,
    cipher: &str,
    password: &str,
    replay: crate::shared::legacy_replay::LegacyReplay,
) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError> {
    let config = shadowsocks_tcp_connect_config(cipher, password)?.with_replay_guard(replay);
    let ss_session = config
        .establish_tcp_session(&mut stream, session)
        .await
        .map_err(|error| RuntimeError::Io(std::io::Error::other(error)))?;
    let traffic = stream.drain_traffic();
    let stream = stream.into_inner();
    Ok((
        TcpRelayStream::new(config.wrap_outbound_stream(stream, ss_session)),
        traffic,
    ))
}

pub(super) async fn relay_with_replay(
    mut stream: TcpRelayStream,
    session: &Session,
    cipher: &str,
    password: &str,
    replay: crate::shared::legacy_replay::LegacyReplay,
) -> Result<TcpRelayStream, RuntimeError> {
    let config = shadowsocks_tcp_connect_config(cipher, password)?.with_replay_guard(replay);
    let ss_session = config
        .establish_tcp_session(&mut stream, session)
        .await
        .map_err(|error| RuntimeError::Io(std::io::Error::other(error)))?;
    Ok(TcpRelayStream::new(
        config.wrap_outbound_stream(stream, ss_session),
    ))
}

fn shadowsocks_tcp_connect_config(
    cipher: &str,
    password: &str,
) -> Result<crate::ShadowsocksTcpConnectConfig, RuntimeError> {
    crate::tcp_connect_config_from_config(cipher, password).map_err(|error| {
        RuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid shadowsocks tcp config: {error}"),
        ))
    })
}

pub async fn establish_shadowsocks_tcp_connect(
    stream: MeteredStream<TcpRelayStream>,
    session: &Session,
    cipher: &str,
    password: &str,
) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError> {
    establish_with_replay(
        stream,
        session,
        cipher,
        password,
        crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
    )
    .await
}
pub async fn apply_shadowsocks_tcp_relay_hop(
    stream: TcpRelayStream,
    session: &Session,
    cipher: &str,
    password: &str,
) -> Result<TcpRelayStream, RuntimeError> {
    relay_with_replay(
        stream,
        session,
        cipher,
        password,
        crate::shared::legacy_replay::LegacyReplay::new(Default::default(), false),
    )
    .await
}
