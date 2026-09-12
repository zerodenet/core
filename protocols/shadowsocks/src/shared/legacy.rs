//! Legacy stream cipher state. Only this protocol module owns the crypto object.
use crate::{validation::LegacyCipher, CipherKind};
use zero_core::{Error, Network, ProtocolType, Session};
use zero_traits::AsyncSocket;

pub struct LegacyCipherState(shadowsocks_crypto::v1::Cipher);
impl LegacyCipherState {
    pub(crate) fn new(cipher: CipherKind, password: &[u8], iv: &[u8]) -> Result<Self, Error> {
        if !cipher.is_stream() || iv.len() != cipher.salt_len() {
            return Err(Error::Protocol("ss: invalid stream cipher IV"));
        }
        let kind = cipher
            .name()
            .parse()
            .map_err(|_| Error::Protocol("ss: unsupported stream cipher"))?;
        let key = if cipher == CipherKind::Legacy(LegacyCipher::Table) {
            password.to_vec()
        } else {
            super::evp_bytes_to_key(password, cipher.key_len())
        };
        Ok(Self(shadowsocks_crypto::v1::Cipher::new(kind, &key, iv)))
    }
    pub(crate) fn encrypt(&mut self, data: &mut [u8]) {
        self.0.encrypt_packet(data);
    }
    pub(crate) fn decrypt(&mut self, data: &mut [u8]) {
        let _ = self.0.decrypt_packet(data);
    }
}
impl core::fmt::Debug for LegacyCipherState {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("LegacyCipherState { .. }")
    }
}
fn random_iv(cipher: CipherKind) -> Result<Vec<u8>, Error> {
    let mut iv = vec![0; cipher.salt_len()];
    super::fill_random(&mut iv)?;
    Ok(iv)
}
pub(crate) async fn send_request<S: AsyncSocket>(
    stream: &mut S,
    session: &Session,
    cipher: CipherKind,
    password: &[u8],
    replay: Option<&super::legacy_replay::LegacyReplay>,
) -> Result<crate::ShadowsocksOutboundSession, Error> {
    let iv = match replay {
        Some(replay) => replay.generate(cipher.salt_len())?,
        None => random_iv(cipher)?,
    };
    let mut state = LegacyCipherState::new(cipher, password, &iv)?;
    let mut address = super::build_target_data(&session.target, session.port, &[])?;
    state.encrypt(&mut address);
    stream
        .write_all(&[iv.as_slice(), &address].concat())
        .await
        .map_err(|_| Error::Io("ss: stream request write failed"))?;
    Ok(crate::ShadowsocksOutboundSession {
        session_key: vec![],
        next_upload_nonce: 0,
        cipher,
        request_salt: iv,
        legacy: Some(state),
    })
}
pub(crate) async fn accept_request<S: AsyncSocket>(
    stream: &mut S,
    cipher: CipherKind,
    password: &[u8],
) -> Result<crate::ShadowsocksAccept, Error> {
    let mut iv = vec![0; cipher.salt_len()];
    super::read_exact(stream, &mut iv).await?;
    let mut state = LegacyCipherState::new(cipher, password, &iv)?;
    // Read exactly the address, leaving application bytes for the stateful stream.
    let mut address = vec![0];
    super::read_exact(stream, &mut address).await?;
    state.decrypt(&mut address);
    let suffix = match address[0] {
        1 => 6,
        4 => 18,
        3 => {
            let mut size = [0];
            super::read_exact(stream, &mut size).await?;
            state.decrypt(&mut size);
            address.push(size[0]);
            size[0] as usize + 2
        }
        _ => return Err(Error::Protocol("ss: unknown stream address type")),
    };
    let start = address.len();
    address.resize(start + suffix, 0);
    super::read_exact(stream, &mut address[start..]).await?;
    state.decrypt(&mut address[start..]);
    let (target, port, _) = super::parse_target_data(&address)?;
    Ok(crate::ShadowsocksAccept {
        session: Session::new(
            0,
            target,
            port,
            Network::Tcp,
            ProtocolType::new("shadowsocks"),
        ),
        remaining_payload: vec![],
        session_key: vec![],
        cipher,
        next_upload_nonce: 0,
        request_salt: iv,
        legacy: Some(state),
    })
}
pub(crate) fn encode_datagram(
    cipher: CipherKind,
    password: &[u8],
    data: &[u8],
) -> Result<Vec<u8>, Error> {
    let iv = random_iv(cipher)?;
    let mut state = LegacyCipherState::new(cipher, password, &iv)?;
    let mut packet = data.to_vec();
    state.encrypt(&mut packet);
    Ok([iv, packet].concat())
}
pub(crate) fn decode_datagram(
    cipher: CipherKind,
    password: &[u8],
    packet: &[u8],
) -> Result<Vec<u8>, Error> {
    let iv = packet
        .get(..cipher.salt_len())
        .ok_or(Error::Protocol("ss: truncated stream IV"))?;
    let mut state = LegacyCipherState::new(cipher, password, iv)?;
    let mut plain = packet[iv.len()..].to_vec();
    state.decrypt(&mut plain);
    Ok(plain)
}
