//! Materializes the protocol profiles and atomic group of carrier listeners.
use super::*;
use crate::runtime::inbound_operation::{
    InboundListenerGroupOperation, PreparedInboundListenerOperation, TcpInboundListenerOperation,
};
pub(super) async fn bind(
    inbound: &InboundConfig,
    source_dir: Option<&std::path::Path>,
) -> Result<BoundInbound, EngineError> {
    let InboundProtocolConfig::Hysteria2 {
        cert_path,
        key_path,
        up_bps,
        down_bps,
        transport,
        masquerade,
        ..
    } = &inbound.protocol
    else {
        return Err(EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hysteria2 inbound bind received non-hysteria2 inbound config",
        )));
    };
    let plan = Hysteria2InboundBindPlan::from_options_refs(
        source_dir,
        Hysteria2InboundBindOptionsRef {
            cert_path: cert_path.as_deref(),
            key_path: key_path.as_deref(),
        },
    );
    let plan =
        plan.with_settings(transport.validated(*down_bps, *up_bps).map_err(|e| {
            EngineError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
        })?);
    let endpoint = plan.bind(&inbound_listen_addr(inbound)).await?;
    let mut listeners = vec![BoundInbound::Quic(endpoint)];
    for listen in masquerade.http.iter().chain(masquerade.https.iter()) {
        let address = listen.address.trim_matches(['[', ']']);
        let address = if address.contains(':') {
            format!("[{address}]:{}", listen.port)
        } else {
            format!("{address}:{}", listen.port)
        };
        listeners.push(BoundInbound::Tcp(
            zero_platform_tokio::TokioListener::bind(&address).await?,
        ));
    }
    if listeners.len() == 1 {
        Ok(listeners.pop().unwrap())
    } else {
        Ok(BoundInbound::Group(listeners))
    }
}
pub(super) fn prepare(
    adapter: &Hysteria2Adapter,
    inbound: InboundConfig,
    source_dir: Option<&std::path::Path>,
) -> Result<Box<dyn PreparedInboundListenerOperation>, EngineError> {
    let (profile, content) = match &inbound.protocol {
        InboundProtocolConfig::Hysteria2 {
            password,
            users,
            up_bps,
            down_bps,
            transport,
            masquerade,
            ..
        } => {
            let users = inbound_user_refs(password, users);
            let profile = adapter.inbound_profiles.replace(&inbound.tag, &users);
            let content = inbound::prepare_masquerade(masquerade, source_dir)?;
            let profile = Hysteria2AuthenticatedInboundProfile::from_options_refs(
                Hysteria2InboundOptionsRef {
                    users: users.iter().copied(),
                },
            )
            .with_settings(transport.validated(*down_bps, *up_bps).map_err(|e| {
                EngineError::Io(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
            })?)
            .with_masquerade(content.clone())
            .with_profile(profile);
            (profile, content)
        }
        _ => {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "hysteria2 inbound listener received non-hysteria2 inbound config",
            )));
        }
    };
    let mut operations = vec![inbound::prepare(profile)];
    let InboundProtocolConfig::Hysteria2 {
        masquerade,
        cert_path,
        key_path,
        ..
    } = &inbound.protocol
    else {
        unreachable!()
    };
    let tls = Hysteria2InboundBindPlan::from_options_refs(
        source_dir,
        Hysteria2InboundBindOptionsRef {
            cert_path: cert_path.as_deref(),
            key_path: key_path.as_deref(),
        },
    );
    let redirect_port = masquerade
        .force_https
        .then(|| masquerade.https.as_ref().unwrap().port);
    let website =
        ::hysteria2::transport::Hysteria2Website::new(content, inbound.listen.port, redirect_port);
    if masquerade.http.is_some() {
        operations.push(website_operation(website.clone()));
    }
    if masquerade.https.is_some() {
        operations.push(website_operation(website.with_tls(&tls)?));
    }
    if operations.len() == 1 {
        Ok(operations.pop().unwrap())
    } else {
        Ok(Box::new(InboundListenerGroupOperation(operations)))
    }
}
fn website_operation(
    website: ::hysteria2::transport::Hysteria2Website,
) -> Box<dyn PreparedInboundListenerOperation> {
    Box::new(TcpInboundListenerOperation {
        protocol_name: "http",
        error_protocol_name: "http",
        request: website,
        dispatch: |website: ::hysteria2::transport::Hysteria2Website,
                   socket: zero_platform_tokio::TokioSocket,
                   _context| async move {
            let peer = socket.peer_addr()?;
            website
                .serve(socket.into_inner(), peer)
                .await
                .map_err(EngineError::from)
        },
    })
}
