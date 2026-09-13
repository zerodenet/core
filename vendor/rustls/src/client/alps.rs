//! ALPS draft-01 negotiation, using the old and new uTLS/BoringSSL codepoints.
use super::{common::ClientHelloDetails, ClientConfig};
use crate::common_state::{CommonState, HandshakeFlightTls13};
use crate::msgs::{
    base::Payload,
    enums::ExtensionType,
    handshake::{HandshakeMessagePayload, HandshakePayload, ServerExtensions},
};
use crate::{AlertDescription, Error};
use alloc::{boxed::Box, vec::Vec};

#[derive(Clone, Debug)]
pub(crate) struct Negotiated {
    pub(crate) protocol: Vec<u8>,
    pub(crate) send_extension: bool,
    pub(crate) codepoint: u16,
    pub(crate) local: Vec<u8>,
    pub(crate) peer: Vec<u8>,
}
pub(super) fn protocols(data: &[u8]) -> Option<Vec<&[u8]>> {
    let n = u16::from_be_bytes(data.get(..2)?.try_into().ok()?) as usize;
    if n == 0 || n + 2 != data.len() {
        return None;
    }
    let mut rest = &data[2..];
    let mut out = Vec::new();
    while !rest.is_empty() {
        let n = rest[0] as usize;
        if n == 0 {
            return None;
        }
        out.push(rest.get(1..n + 1)?);
        rest = &rest[n + 1..];
    }
    Some(out)
}
pub(super) fn receive(
    common: &mut CommonState,
    config: &ClientConfig,
    hello: &ClientHelloDetails,
    exts: &ServerExtensions<'_>,
    saved: Option<&Negotiated>,
) -> Result<(), Error> {
    if common.early_traffic && exts.early_data_ack.is_some() {
        if exts.application_settings.is_some() || exts.application_settings_old.is_some() {
            return Err(fail(
                common,
                "ALPS exchange is forbidden with accepted early data",
            ));
        }
        if let Some(saved) = saved {
            if common.get_alpn_protocol() != Some(saved.protocol.as_slice())
                || !early_compatible(config, hello.wire_profile.as_ref(), Some(saved))
            {
                return Err(fail(
                    common,
                    "ALPS settings changed during accepted early data",
                ));
            }
            let mut restored = saved.clone();
            restored.send_extension = false;
            common.alps = Some(restored);
        }
        return Ok(());
    }
    let value = match (&exts.application_settings_old, &exts.application_settings) {
        (None, None) => return Ok(()),
        (Some(value), None) => (17513, value),
        (None, Some(value)) => (17613, value),
        _ => return Err(fail(common, "server selected both ALPS codepoints")),
    };
    let Some(alpn) = common.get_alpn_protocol() else {
        return Err(fail(common, "ALPS requires ALPN"));
    };
    let offered = hello
        .wire_profile
        .as_ref()
        .and_then(|p| p.extensions.iter().find(|(id, _)| *id == value.0))
        .and_then(|(_, data)| protocols(data))
        .is_some_and(|ps| ps.contains(&alpn));
    let Some(local) = config.application_settings.get(alpn).filter(|_| offered) else {
        return Err(fail(common, "server selected unoffered ALPS protocol"));
    };
    if local.len() > 65529 {
        return Err(fail(common, "local ALPS settings too large"));
    }
    common.alps = Some(Negotiated {
        protocol: alpn.to_vec(),
        send_extension: true,
        codepoint: value.0,
        local: local.clone(),
        peer: value.1.bytes().to_vec(),
    });
    Ok(())
}
fn fail(common: &mut CommonState, message: &str) -> Error {
    common.send_fatal_alert(
        AlertDescription::IllegalParameter,
        Error::General(message.into()),
    )
}
pub(super) fn emit(common: &CommonState, flight: &mut HandshakeFlightTls13<'_>) {
    let Some(alps) = common.alps.as_ref().filter(|a| a.send_extension) else {
        return;
    };
    let mut exts = ServerExtensions::default();
    let settings = Some(Payload::new(alps.local.as_slice()));
    match ExtensionType::from(alps.codepoint) {
        ExtensionType::ApplicationSettingsOld => exts.application_settings_old = settings,
        _ => exts.application_settings = settings,
    }
    flight.add(HandshakeMessagePayload(
        HandshakePayload::EncryptedExtensions(Box::new(exts)),
    ));
}

/// 0-RTT may reuse ALPS only with the identical protocol, offer and local settings.
pub(super) fn early_compatible(
    config: &ClientConfig,
    profile: Option<&super::hello_profile::ClientHelloProfile>,
    saved: Option<&Negotiated>,
) -> bool {
    let Some(saved) = saved else {
        return config.application_settings.is_empty();
    };
    config.alpn_protocols.contains(&saved.protocol)
        && config.application_settings.get(&saved.protocol) == Some(&saved.local)
        && profile
            .and_then(|p| p.extensions.iter().find(|(id, _)| *id == saved.codepoint))
            .and_then(|(_, data)| protocols(data))
            .is_some_and(|ps| ps.contains(&saved.protocol.as_slice()))
}

#[cfg(all(test, feature = "aws_lc_rs"))]
mod tests;
