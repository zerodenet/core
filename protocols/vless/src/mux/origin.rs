//! Reverse MUX source/local metadata uses the ordinary MUX address encoding.
use super::{MuxTarget, MUX_MAX_METADATA, NETWORK_TCP, NETWORK_UDP};
use alloc::vec::Vec;
use zero_core::{Error, Network, Session};
#[derive(Debug, Clone, Default)]
pub(crate) struct Origin {
    source: Option<MuxTarget>,
    local: Option<MuxTarget>,
}
impl Origin {
    pub(crate) fn from_session(session: &Session) -> Self {
        let network = match session.network {
            Network::Tcp => NETWORK_TCP,
            Network::Udp => NETWORK_UDP,
        };
        Self {
            source: session
                .source_ip
                .clone()
                .zip(session.source_port)
                .map(|(address, port)| MuxTarget {
                    network,
                    address,
                    port,
                }),
            local: session
                .inbound_local
                .clone()
                .map(|(address, port)| MuxTarget {
                    network,
                    address,
                    port,
                }),
        }
    }
    pub(crate) fn apply(self, session: &mut Session) {
        if let Some(source) = self.source {
            session.source_ip = Some(source.address);
            session.source_port = Some(source.port);
        }
        session.inbound_local = self.local.map(|target| (target.address, target.port));
    }
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self, Error> {
        let (source, consumed) = parse_endpoint(bytes)?;
        let (local, _) = if source.is_some() {
            parse_endpoint(&bytes[consumed..])?
        } else {
            (None, 0)
        };
        Ok(Self { source, local })
    }
    pub(crate) fn append(&self, frame: &mut Vec<u8>) -> Result<(), Error> {
        let Some(source) = &self.source else {
            return Ok(());
        };
        let mut extra = Vec::new();
        encode_endpoint(source, &mut extra)?;
        if let Some(local) = &self.local {
            encode_endpoint(local, &mut extra)?;
        }
        let length = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let updated = length + extra.len();
        if updated > MUX_MAX_METADATA {
            return Err(Error::Protocol("reverse MUX metadata is too large"));
        }
        frame.splice(2 + length..2 + length, extra);
        frame[..2].copy_from_slice(&(updated as u16).to_be_bytes());
        Ok(())
    }
}
fn parse_endpoint(bytes: &[u8]) -> Result<(Option<MuxTarget>, usize), Error> {
    let Some(&network) = bytes.first() else {
        return Ok((None, 0));
    };
    if network == 0 {
        return Ok((None, bytes.len()));
    }
    if !matches!(network, NETWORK_TCP | NETWORK_UDP) {
        return Err(Error::Protocol("invalid reverse MUX endpoint network"));
    }
    if bytes.len() < 4 {
        return Err(Error::Protocol("truncated reverse MUX endpoint"));
    }
    let port = u16::from_be_bytes([bytes[1], bytes[2]]);
    let (address, consumed) = super::parse_address_from_bytes_with_len(bytes[3], &bytes[4..])?;
    Ok((
        Some(MuxTarget {
            network,
            address,
            port,
        }),
        4 + consumed,
    ))
}
fn encode_endpoint(endpoint: &MuxTarget, bytes: &mut Vec<u8>) -> Result<(), Error> {
    bytes.push(endpoint.network);
    bytes.extend_from_slice(&endpoint.port.to_be_bytes());
    crate::shared::write_address(bytes, &endpoint.address)
}
