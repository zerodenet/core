// Shadowsocks outbound protocol.

use zero_core::ProtocolType;
#[cfg(feature = "crypto")]
use zero_core::{Address, Error, Session};
#[cfg(feature = "crypto")]
use zero_traits::{AsyncSocket, DatagramCodec, TcpSessionProtocol, UdpDatagramFraming};

/// Shadowsocks outbound handler.
#[derive(Debug, Default, Clone, Copy)]
pub struct ShadowsocksOutbound;

#[cfg(feature = "crypto")]
pub struct ShadowsocksOutboundSession {
    pub session_key: Vec<u8>,
    pub next_upload_nonce: u64,
    pub cipher: super::shared::CipherKind,
    /// For 2022 edition: the request salt we sent. The stream verifies the
    /// server's response header echoes this salt. Empty for legacy AEAD.
    pub request_salt: Vec<u8>,
}

impl ShadowsocksOutbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("shadowsocks")
    }

    /// Write the initial encrypted stream payload containing target address.
    #[cfg(feature = "crypto")]
    pub async fn send_request<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksOutboundSession, Error> {
        if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                return self
                    .send_request_2022(stream, session, cipher, password)
                    .await;
            }
            #[cfg(not(feature = "blake3"))]
            return Err(Error::Protocol(
                "ss: 2022 tcp request requires `blake3` feature",
            ));
        }
        self.send_request_legacy(stream, session, cipher, password)
            .await
    }

    /// Legacy AEAD request: salt + one length/payload chunk carrying the
    /// target address (the first body chunk).
    #[cfg(feature = "crypto")]
    async fn send_request_legacy<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksOutboundSession, Error> {
        use super::shared::{build_target_data, derive_session_key, encrypt_tcp_chunk};

        let salt_len = cipher.salt_len();

        // Generate random salt
        let mut salt = vec![0u8; salt_len];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Protocol("ss: random failed"))?;

        let key = derive_session_key(cipher, password, &salt)?;

        // Build target data
        let target_data = build_target_data(&session.target, session.port, &[])?;

        let mut nonce = 0;
        let encrypted = encrypt_tcp_chunk(cipher, &key, &mut nonce, &target_data)?;

        let mut request = Vec::with_capacity(salt.len() + encrypted.len());
        request.extend_from_slice(&salt);
        request.extend_from_slice(&encrypted);
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("ss: failed to write request"))?;

        Ok(ShadowsocksOutboundSession {
            session_key: key,
            next_upload_nonce: nonce,
            cipher,
            request_salt: salt,
        })
    }

    /// 2022 edition (SIP022) request: salt + fixed-header chunk (nonce 0) +
    /// variable-header chunk (nonce 1). Body chunks follow from nonce 2.
    #[cfg(all(feature = "crypto", feature = "blake3"))]
    async fn send_request_2022<S: AsyncSocket>(
        &self,
        stream: &mut S,
        session: &Session,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Result<ShadowsocksOutboundSession, Error> {
        use super::shared::{
            build_2022_request_fixed_header, build_2022_request_var_header, derive_session_key,
            encode_tcp_2022_identity_headers, encrypt_tcp_2022_single_chunk, now_unix_seconds,
            parse_2022_key_chain, random_2022_padding,
        };

        let key_chain = parse_2022_key_chain(cipher, password)?;

        let salt_len = cipher.salt_len();
        let mut salt = vec![0u8; salt_len];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Protocol("ss: random failed"))?;

        let key = derive_session_key(cipher, &key_chain.user_password, &salt)?;
        let identity_headers = encode_tcp_2022_identity_headers(
            cipher,
            &key_chain.identity_keys,
            &key_chain.user_key,
            &salt,
        )?;

        // No initial payload is available at request time (the relay has not
        // started), so per SIP022 3.1.3 we MUST include non-zero padding.
        let padding = random_2022_padding(true)?;
        let var_header =
            build_2022_request_var_header(&session.target, session.port, &padding, &[])?;
        let var_len = u16::try_from(var_header.len())
            .map_err(|_| Error::Protocol("ss: 2022 variable header too long"))?;

        let fixed_header = build_2022_request_fixed_header(now_unix_seconds(), var_len);

        let mut nonce = 0u64;
        let enc_fixed = encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &fixed_header)?;
        let enc_var = encrypt_tcp_2022_single_chunk(cipher, &key, &mut nonce, &var_header)?;

        let mut request = Vec::with_capacity(
            salt.len() + identity_headers.len() + enc_fixed.len() + enc_var.len(),
        );
        request.extend_from_slice(&salt);
        request.extend_from_slice(&identity_headers);
        request.extend_from_slice(&enc_fixed);
        request.extend_from_slice(&enc_var);
        stream
            .write_all(&request)
            .await
            .map_err(|_| Error::Io("ss: failed to write 2022 request"))?;

        Ok(ShadowsocksOutboundSession {
            session_key: key,
            // Body length+payload chunks start at nonce 2 (after the two
            // header chunks consumed nonces 0 and 1).
            next_upload_nonce: nonce,
            cipher,
            request_salt: salt,
        })
    }
}

/// Target parameters for Shadowsocks TCP session.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksTcpTarget<'a> {
    pub session: &'a Session,
    pub cipher: super::shared::CipherKind,
    pub password: &'a [u8],
}

/// Parsed Shadowsocks TCP connect settings built from external config.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksTcpConnectConfig {
    cipher: super::shared::CipherKind,
    password: alloc::vec::Vec<u8>,
    response_password: alloc::vec::Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksTcpConnectConfig {
    pub fn from_config(cipher: &str, password: &str) -> Result<Self, Error> {
        let cipher = super::shared::CipherKind::from_str(cipher)
            .ok_or(Error::Protocol("ss: unknown tcp cipher"))?;
        let password = password.as_bytes().to_vec();
        let response_password = if cipher.is_blake3() {
            #[cfg(feature = "blake3")]
            {
                super::shared::parse_2022_key_chain(cipher, &password)?.user_password
            }
            #[cfg(not(feature = "blake3"))]
            {
                password.clone()
            }
        } else {
            password.clone()
        };
        Ok(Self {
            cipher,
            password,
            response_password,
        })
    }

    pub fn tcp_target<'a>(&'a self, session: &'a Session) -> ShadowsocksTcpTarget<'a> {
        ShadowsocksTcpTarget {
            session,
            cipher: self.cipher,
            password: &self.password,
        }
    }

    pub async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        session: &Session,
    ) -> Result<ShadowsocksOutboundSession, Error>
    where
        S: AsyncSocket,
    {
        <ShadowsocksOutbound as TcpSessionProtocol<ShadowsocksTcpTarget<'_>>>::establish_tcp_session(
            &ShadowsocksOutbound,
            stream,
            &self.tcp_target(session),
        )
        .await
    }

    pub fn wrap_outbound_stream<S>(
        &self,
        stream: S,
        session: ShadowsocksOutboundSession,
    ) -> super::stream::ShadowsocksAeadStream<S> {
        super::stream::ShadowsocksAeadStream::outbound(
            stream,
            session,
            self.response_password.clone(),
        )
    }
}

#[cfg(feature = "crypto")]
pub fn tcp_connect_config_from_config(
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksTcpConnectConfig, Error> {
    ShadowsocksTcpConnectConfig::from_config(cipher, password)
}

#[cfg(feature = "crypto")]
impl<'a> TcpSessionProtocol<ShadowsocksTcpTarget<'a>> for ShadowsocksOutbound {
    type Error = Error;
    type Session = ShadowsocksOutboundSession;

    async fn establish_tcp_session<S>(
        &self,
        stream: &mut S,
        target: &ShadowsocksTcpTarget<'a>,
    ) -> Result<Self::Session, Self::Error>
    where
        S: AsyncSocket,
    {
        self.send_request(stream, target.session, target.cipher, target.password)
            .await
    }
}

/// One plaintext UDP payload to encode into a Shadowsocks UDP datagram.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksUdpPacketTarget<'a> {
    pub target: &'a Address,
    pub port: u16,
    pub payload: &'a [u8],
    pub cipher: super::shared::CipherKind,
    pub password: &'a [u8],
}

/// Decryption context for a Shadowsocks UDP datagram received from upstream.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksUdpDecodeContext<'a> {
    pub cipher: super::shared::CipherKind,
    pub password: &'a [u8],
}

/// One decoded Shadowsocks UDP datagram.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacket {
    pub fn new(target: Address, port: u16, payload: Vec<u8>) -> Self {
        Self {
            target,
            port,
            payload,
        }
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpFlowPacket {
    target: Address,
    port: u16,
    payload: Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpFlowPacket {
    pub fn from_parts(target: &Address, port: u16, payload: &[u8]) -> Self {
        Self {
            target: target.clone(),
            port,
            payload: payload.to_vec(),
        }
    }

    pub fn encode_with(&self, resume: &ShadowsocksUdpFlowResume) -> Result<Vec<u8>, Error> {
        resume.encode_flow_packet(self)
    }

    pub fn target(&self) -> &Address {
        &self.target
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn into_parts(self) -> (Address, u16, Vec<u8>) {
        (self.target, self.port, self.payload)
    }
}

#[cfg(feature = "crypto")]
impl<'a> UdpDatagramFraming<ShadowsocksUdpPacketTarget<'a>, ShadowsocksUdpDecodeContext<'a>>
    for ShadowsocksOutbound
{
    type Error = Error;
    type Decoded = ShadowsocksUdpPacket;

    fn encode_udp_datagram(
        &self,
        packet: &ShadowsocksUdpPacketTarget<'a>,
    ) -> Result<Vec<u8>, Self::Error> {
        use super::shared::{aead_encrypt_udp, build_target_data, derive_udp_packet_key};

        if packet.cipher.is_blake3() {
            return super::shared::encode_udp_datagram_2022(
                packet.cipher,
                packet.password,
                packet.target,
                packet.port,
                packet.payload,
            );
        }

        let target_data = build_target_data(packet.target, packet.port, packet.payload)?;
        let mut salt = vec![0u8; packet.cipher.udp_salt_len()];
        use ring::rand::SecureRandom;
        ring::rand::SystemRandom::new()
            .fill(&mut salt)
            .map_err(|_| Error::Protocol("ss: random failed"))?;

        let key = derive_udp_packet_key(packet.cipher, packet.password, &salt)?;

        let encrypted = aead_encrypt_udp(packet.cipher, &key, &[0u8; 12], &target_data)?;
        let mut datagram = Vec::with_capacity(salt.len() + encrypted.len());
        datagram.extend_from_slice(&salt);
        datagram.extend_from_slice(&encrypted);
        Ok(datagram)
    }

    fn decode_udp_datagram(
        &self,
        context: &ShadowsocksUdpDecodeContext<'a>,
        datagram: &[u8],
    ) -> Result<Self::Decoded, Self::Error> {
        use super::shared::{aead_decrypt_udp, derive_udp_packet_key, parse_target_data};

        if context.cipher.is_blake3() {
            let (target, port, payload) = super::shared::decode_udp_datagram_2022(
                context.cipher,
                context.password,
                datagram,
            )?;
            return Ok(ShadowsocksUdpPacket::new(target, port, payload));
        }

        let salt_len = context.cipher.udp_salt_len();
        if datagram.len() < salt_len + context.cipher.tag_len() {
            return Err(Error::Protocol("ss: udp datagram too short"));
        }

        let key = derive_udp_packet_key(context.cipher, context.password, &datagram[..salt_len])?;

        let plain = aead_decrypt_udp(context.cipher, &key, &[0u8; 12], &datagram[salt_len..])?;
        let (target, port, payload_offset) = parse_target_data(&plain)?;
        Ok(ShadowsocksUdpPacket::new(
            target,
            port,
            plain[payload_offset..].to_vec(),
        ))
    }
}

#[cfg(feature = "crypto")]
fn encode_udp_datagram(
    target: &Address,
    port: u16,
    payload: &[u8],
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> Result<Vec<u8>, Error> {
    <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget<'_>,
        ShadowsocksUdpDecodeContext<'_>,
    >>::encode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpPacketTarget {
            target,
            port,
            payload,
            cipher,
            password,
        },
    )
}

#[cfg(feature = "crypto")]
fn decode_udp_datagram(
    datagram: &[u8],
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> Result<ShadowsocksUdpPacket, Error> {
    <ShadowsocksOutbound as UdpDatagramFraming<
        ShadowsocksUdpPacketTarget<'_>,
        ShadowsocksUdpDecodeContext<'_>,
    >>::decode_udp_datagram(
        &ShadowsocksOutbound,
        &ShadowsocksUdpDecodeContext { cipher, password },
        datagram,
    )
}

#[cfg(feature = "crypto")]
fn encode_udp_flow_packet(
    target: &Address,
    port: u16,
    payload: &[u8],
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> Result<Vec<u8>, Error> {
    encode_udp_datagram(target, port, payload, cipher, password)
}

#[cfg(feature = "crypto")]
fn decode_udp_flow_packet(
    datagram: &[u8],
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> Result<ShadowsocksUdpPacket, Error> {
    decode_udp_datagram(datagram, cipher, password)
}

#[cfg(feature = "crypto")]
fn udp_cache_key(tag: &str, server: &str, port: u16, cipher: &str, password: &str) -> String {
    alloc::format!("shadowsocks|{tag}|{server}:{port}|{cipher}|{password}")
}

#[cfg(feature = "crypto")]
pub fn parse_udp_cipher(cipher: &str) -> Result<super::shared::CipherKind, Error> {
    super::shared::CipherKind::from_str(cipher).ok_or(Error::Protocol("ss: unknown udp cipher"))
}

/// Codec state for a Shadowsocks UDP datagram chain hop.
///
/// Captures the cipher and password needed to encode/decode Shadowsocks
/// UDP datagrams in a relay chain. Implements [`DatagramCodec`] from
/// `zero-traits` so the proxy runtime can use it without protocol-specific
/// adapter code.
#[cfg(feature = "crypto")]
#[derive(Debug, Clone)]
pub struct ShadowsocksDatagramCodec {
    pub cipher: super::shared::CipherKind,
    pub password: alloc::vec::Vec<u8>,
}

#[cfg(feature = "crypto")]
fn udp_datagram_codec(
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> impl DatagramCodec<Address, Error = Error> {
    ShadowsocksDatagramCodec {
        cipher,
        password: password.to_vec(),
    }
}

#[cfg(feature = "crypto")]
fn udp_flow_codec(
    cipher: super::shared::CipherKind,
    password: &[u8],
) -> impl DatagramCodec<Address, Error = Error> {
    udp_datagram_codec(cipher, password)
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone)]
pub struct ShadowsocksUdpSocketFlowSpec {
    cache_key: alloc::string::String,
    codec: ShadowsocksDatagramCodec,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpSocketFlowSpec {
    pub fn into_cache_key(self) -> alloc::string::String {
        self.cache_key
    }

    pub fn into_codec(self) -> ShadowsocksDatagramCodec {
        self.codec
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpFlowResume {
    cache_key: alloc::string::String,
    cipher: super::shared::CipherKind,
    password: alloc::vec::Vec<u8>,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpFlowResume {
    pub fn new(
        cache_key: alloc::string::String,
        cipher: super::shared::CipherKind,
        password: &[u8],
    ) -> Self {
        Self {
            cache_key,
            cipher,
            password: password.to_vec(),
        }
    }

    pub fn from_config(
        tag: &str,
        server: &str,
        port: u16,
        cipher: &str,
        password: &str,
    ) -> Result<Self, Error> {
        let cipher_kind = parse_udp_cipher(cipher)?;
        Ok(Self::new(
            udp_cache_key(tag, server, port, cipher, password),
            cipher_kind,
            password.as_bytes(),
        ))
    }

    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }

    pub fn leaf_cache_key(&self) -> ShadowsocksUdpLeafKey {
        ShadowsocksUdpLeafKey {
            cache_key: self.cache_key.clone(),
        }
    }

    pub fn flow_cache_key(&self) -> alloc::string::String {
        self.cache_key.clone()
    }

    pub fn codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        udp_flow_codec(self.cipher, &self.password)
    }

    pub fn socket_flow_codec(&self) -> impl DatagramCodec<Address, Error = Error> {
        self.codec()
    }

    pub fn managed_socket_flow(&self) -> ShadowsocksUdpSocketFlowSpec {
        ShadowsocksUdpSocketFlowSpec {
            cache_key: self.flow_cache_key(),
            codec: ShadowsocksDatagramCodec {
                cipher: self.cipher,
                password: self.password.clone(),
            },
        }
    }

    pub fn into_managed_socket_flow_codec(self) -> ShadowsocksDatagramCodec {
        self.managed_socket_flow().into_codec()
    }

    pub fn into_shared_managed_socket_flow_codec(
        self,
    ) -> alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>> {
        alloc::sync::Arc::new(self.into_managed_socket_flow_codec())
    }

    fn encode_flow_packet(
        &self,
        packet: &ShadowsocksUdpFlowPacket,
    ) -> Result<alloc::vec::Vec<u8>, Error> {
        encode_udp_flow_packet(
            packet.target(),
            packet.port(),
            packet.payload(),
            self.cipher,
            &self.password,
        )
    }

    pub fn decode_flow_packet(&self, data: &[u8]) -> Option<ShadowsocksUdpFlowPacket> {
        let decoded = decode_udp_flow_packet(data, self.cipher, &self.password).ok()?;
        let (target, port, payload) = decoded.into_parts();
        Some(ShadowsocksUdpFlowPacket {
            target,
            port,
            payload,
        })
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathSpec {
    resume: ShadowsocksUdpFlowResume,
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone)]
pub struct ShadowsocksUdpPacketPathDatagramSourceBuild {
    tag: alloc::string::String,
    server: alloc::string::String,
    port: u16,
    cache_key: alloc::string::String,
    codec: ShadowsocksDatagramCodec,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathSpec {
    fn new(resume: ShadowsocksUdpFlowResume) -> Self {
        Self { resume }
    }

    pub fn carrier_codec(&self) -> alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>> {
        alloc::sync::Arc::new(ShadowsocksDatagramCodec {
            cipher: self.resume.cipher,
            password: self.resume.password.clone(),
        })
    }

    pub fn datagram_source_build(
        &self,
        tag: &str,
        server: &str,
        port: u16,
    ) -> ShadowsocksUdpPacketPathDatagramSourceBuild {
        ShadowsocksUdpPacketPathDatagramSourceBuild {
            tag: tag.to_owned(),
            server: server.to_owned(),
            port,
            cache_key: self.resume.flow_cache_key(),
            codec: ShadowsocksDatagramCodec {
                cipher: self.resume.cipher,
                password: self.resume.password.clone(),
            },
        }
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathCarrierBuild {
    cache_key: alloc::string::String,
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShadowsocksUdpPacketPathCarrierDescriptor {
    cache_key: alloc::string::String,
    server: alloc::string::String,
    port: u16,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathSpec {
    pub fn carrier_build(&self) -> ShadowsocksUdpPacketPathCarrierBuild {
        ShadowsocksUdpPacketPathCarrierBuild {
            cache_key: self.resume.flow_cache_key(),
        }
    }

    pub fn carrier_descriptor(
        &self,
        server: &str,
        port: u16,
    ) -> ShadowsocksUdpPacketPathCarrierDescriptor {
        ShadowsocksUdpPacketPathCarrierDescriptor {
            cache_key: self.resume.flow_cache_key(),
            server: server.to_owned(),
            port,
        }
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathCarrierDescriptor {
    pub fn into_parts(self) -> (alloc::string::String, alloc::string::String, u16) {
        (self.cache_key, self.server, self.port)
    }
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpPacketPathDatagramSourceBuild {
    pub fn into_parts(
        self,
    ) -> (
        alloc::string::String,
        alloc::string::String,
        u16,
        alloc::string::String,
        ShadowsocksDatagramCodec,
    ) {
        (self.tag, self.server, self.port, self.cache_key, self.codec)
    }

    pub fn into_codec(self) -> ShadowsocksDatagramCodec {
        self.codec
    }

    pub fn into_shared_codec_parts(
        self,
    ) -> (
        alloc::string::String,
        alloc::string::String,
        u16,
        alloc::string::String,
        alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>>,
    ) {
        let (tag, server, port, cache_key, codec) = self.into_parts();
        (tag, server, port, cache_key, alloc::sync::Arc::new(codec))
    }
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, Copy)]
pub struct ShadowsocksUdpFlowConfig<'a> {
    tag: &'a str,
    server: &'a str,
    port: u16,
    cipher: &'a str,
    password: &'a str,
}

#[cfg(feature = "crypto")]
impl<'a> ShadowsocksUdpFlowConfig<'a> {
    pub fn new(
        tag: &'a str,
        server: &'a str,
        port: u16,
        cipher: &'a str,
        password: &'a str,
    ) -> Self {
        Self {
            tag,
            server,
            port,
            cipher,
            password,
        }
    }

    pub fn flow_resume(&self) -> Result<ShadowsocksUdpFlowResume, Error> {
        ShadowsocksUdpFlowResume::from_config(
            self.tag,
            self.server,
            self.port,
            self.cipher,
            self.password,
        )
    }

    pub fn packet_path_spec(&self) -> Result<ShadowsocksUdpPacketPathSpec, Error> {
        Ok(ShadowsocksUdpPacketPathSpec::new(self.flow_resume()?))
    }
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_spec_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathSpec, Error> {
    ShadowsocksUdpFlowConfig::new(tag, server, port, cipher, password).packet_path_spec()
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_carrier_descriptor_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathCarrierDescriptor, Error> {
    Ok(
        udp_packet_path_spec_from_config(tag, server, port, cipher, password)?
            .carrier_descriptor(server, port),
    )
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_carrier_codec_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<alloc::sync::Arc<dyn DatagramCodec<Address, Error = Error>>, Error> {
    Ok(udp_packet_path_spec_from_config(tag, server, port, cipher, password)?.carrier_codec())
}

#[cfg(feature = "crypto")]
pub fn udp_packet_path_datagram_source_build_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpPacketPathDatagramSourceBuild, Error> {
    Ok(
        udp_packet_path_spec_from_config(tag, server, port, cipher, password)?
            .datagram_source_build(tag, server, port),
    )
}

#[cfg(feature = "crypto")]
pub fn udp_flow_resume_from_config(
    tag: &str,
    server: &str,
    port: u16,
    cipher: &str,
    password: &str,
) -> Result<ShadowsocksUdpFlowResume, Error> {
    ShadowsocksUdpFlowConfig::new(tag, server, port, cipher, password).flow_resume()
}

#[cfg(feature = "crypto")]
pub fn managed_socket_flow_from_resume(
    resume: &ShadowsocksUdpFlowResume,
) -> ShadowsocksUdpSocketFlowSpec {
    resume.managed_socket_flow()
}

#[cfg(feature = "crypto")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ShadowsocksUdpLeafKey {
    cache_key: alloc::string::String,
}

#[cfg(feature = "crypto")]
impl ShadowsocksUdpLeafKey {
    pub fn cache_key(&self) -> &str {
        &self.cache_key
    }
}

#[cfg(feature = "crypto")]
impl DatagramCodec<Address> for ShadowsocksDatagramCodec {
    type Error = Error;

    fn encode(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<alloc::vec::Vec<u8>, Self::Error> {
        <ShadowsocksOutbound as UdpDatagramFraming<
            ShadowsocksUdpPacketTarget<'_>,
            ShadowsocksUdpDecodeContext<'_>,
        >>::encode_udp_datagram(
            &ShadowsocksOutbound,
            &ShadowsocksUdpPacketTarget {
                target,
                port,
                payload,
                cipher: self.cipher,
                password: &self.password,
            },
        )
    }

    fn decode(&self, data: &[u8]) -> Option<(Address, u16, alloc::vec::Vec<u8>)> {
        let decoded = <ShadowsocksOutbound as UdpDatagramFraming<
            ShadowsocksUdpPacketTarget<'_>,
            ShadowsocksUdpDecodeContext<'_>,
        >>::decode_udp_datagram(
            &ShadowsocksOutbound,
            &ShadowsocksUdpDecodeContext {
                cipher: self.cipher,
                password: &self.password,
            },
            data,
        )
        .ok()?;
        Some(decoded.into_parts())
    }
}
