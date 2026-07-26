use std::sync::{Arc, RwLock};
use zero_core::{Error, Network, ProtocolType, Session, SessionAuth};
use zero_traits::AsyncSocket;

use crate::crypto::{
    derive_xray_cmd_key, hex, open_xray_aead_header_length, open_xray_aead_header_payload,
    seal_xray_response_header, GCM_TAG_LEN,
};
use crate::shared::{parse_address_from_bytes, read_exact, AUTH_ID_LEN, CMD_TCP, CMD_UDP, VERSION};
use crate::{parse_uuid, VmessCipher};

#[derive(Clone)]
pub struct VmessUser {
    pub id: [u8; 16],
    pub cipher: VmessCipher,
    pub principal_key: Option<String>,
    pub up_bps: Option<u64>,
    pub down_bps: Option<u64>,
    pub device_limit: Option<u32>,
    pub quota_remaining_bytes: Option<u64>,
    pub policy_revision: Option<u64>,
}

impl VmessUser {
    #[allow(clippy::too_many_arguments)]
    pub fn from_config(
        id: &str,
        cipher: &str,
        principal_key: Option<String>,
        up_bps: Option<u64>,
        down_bps: Option<u64>,
        device_limit: Option<u32>,
        quota_remaining_bytes: Option<u64>,
        policy_revision: Option<u64>,
    ) -> Result<Self, Error> {
        let id = parse_uuid(id)?;
        let cipher =
            VmessCipher::from_name(cipher).ok_or(Error::Protocol("vmess unknown cipher"))?;
        Ok(Self {
            id,
            cipher,
            principal_key,
            up_bps,
            down_bps,
            device_limit,
            quota_remaining_bytes,
            policy_revision,
        })
    }
}

pub type VmessInboundUserConfigParts = (
    String,
    String,
    Option<String>,
    Option<u64>,
    Option<u64>,
    Option<u32>,
    Option<u64>,
    Option<u64>,
);

#[derive(Clone)]
pub struct VmessInboundProfile {
    users: Arc<RwLock<Arc<[VmessUser]>>>,
}

impl core::fmt::Debug for VmessInboundProfile {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("VmessInboundProfile")
            .field("user_count", &self.user_count())
            .finish()
    }
}

impl VmessInboundProfile {
    pub fn from_users(users: Vec<VmessUser>) -> Self {
        Self {
            users: Arc::new(RwLock::new(users.into())),
        }
    }

    pub fn user_count(&self) -> usize {
        self.users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .len()
    }

    pub fn replace_users(&self, users: Vec<VmessUser>) {
        *self
            .users
            .write()
            .unwrap_or_else(|error| error.into_inner()) = users.into();
    }

    pub fn from_config_parts<I>(users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = VmessInboundUserConfigParts>,
    {
        users
            .into_iter()
            .map(
                |(
                    id,
                    cipher,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    VmessUser::from_config(
                        &id,
                        &cipher,
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect::<Result<Vec<_>, Error>>()
            .map(Self::from_users)
    }

    pub fn from_config_users<I, U>(users: I) -> Result<Self, Error>
    where
        I: IntoIterator<Item = U>,
        U: IntoVmessInboundUserConfig,
    {
        Self::from_config_parts(users.into_iter().map(U::into_vmess_inbound_user_config))
    }

    pub fn replace_config_users<I, U>(&self, users: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = U>,
        U: IntoVmessInboundUserConfig,
    {
        let replacement = users
            .into_iter()
            .map(IntoVmessInboundUserConfig::into_vmess_inbound_user_config)
            .map(
                |(
                    id,
                    cipher,
                    principal_key,
                    up_bps,
                    down_bps,
                    device_limit,
                    quota_remaining_bytes,
                    policy_revision,
                )| {
                    VmessUser::from_config(
                        &id,
                        &cipher,
                        principal_key,
                        up_bps,
                        down_bps,
                        device_limit,
                        quota_remaining_bytes,
                        policy_revision,
                    )
                },
            )
            .collect::<Result<Vec<_>, Error>>()?;
        self.replace_users(replacement);
        Ok(())
    }

    async fn accept_tcp<S: AsyncSocket>(
        &self,
        inbound: VmessInbound,
        stream: &mut S,
    ) -> Result<VmessAccept, Error> {
        let users = self
            .users
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        if users.len() == 1 {
            inbound.accept_tcp(stream, &users[0]).await
        } else {
            inbound.accept_tcp_multi(stream, &users).await
        }
    }

    pub async fn accept_tcp_stream<S: AsyncSocket>(
        &self,
        inbound: VmessInbound,
        mut stream: S,
    ) -> Result<(Session, crate::stream::VmessAeadStream<S>), Error> {
        let accepted = self.accept_tcp(inbound, &mut stream).await?;
        let session = accepted.session().clone();
        let client = crate::stream::wrap_tcp_inbound_stream(stream, accepted)?;
        Ok((session, client))
    }

    pub async fn accept_client<S>(
        &self,
        inbound: VmessInbound,
        stream: S,
    ) -> Result<crate::mux::VmessInboundAcceptedStream<crate::stream::VmessAeadStream<S>>, Error>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (session, client) = self.accept_tcp_stream(inbound, stream).await?;
        Ok(crate::mux::VmessInboundAcceptedStream::from_session_stream(
            session,
            client,
            crate::mux::MuxResponseBacklogPolicy::default(),
        ))
    }

    pub(crate) async fn accept_client_owned<S>(
        self,
        inbound: VmessInbound,
        stream: S,
        mux_response_backlog: crate::mux::MuxResponseBacklogPolicy,
    ) -> Result<crate::mux::VmessInboundAcceptedStream<crate::stream::VmessAeadStream<S>>, Error>
    where
        S: AsyncSocket + tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (session, client) = self.accept_tcp_stream(inbound, stream).await?;
        Ok(crate::mux::VmessInboundAcceptedStream::from_session_stream(
            session,
            client,
            mux_response_backlog,
        ))
    }
}

pub trait IntoVmessInboundUserConfig {
    fn into_vmess_inbound_user_config(self) -> VmessInboundUserConfigParts;
}

impl IntoVmessInboundUserConfig for VmessInboundUserConfigParts {
    fn into_vmess_inbound_user_config(self) -> VmessInboundUserConfigParts {
        self
    }
}

#[cfg(feature = "runtime")]
impl IntoVmessInboundUserConfig for crate::transport::VmessInboundUserRef<'_> {
    fn into_vmess_inbound_user_config(self) -> VmessInboundUserConfigParts {
        (
            self.id.to_owned(),
            self.cipher.to_owned(),
            self.principal_key.map(str::to_owned),
            self.up_bps,
            self.down_bps,
            self.device_limit,
            self.quota_remaining_bytes,
            self.policy_revision,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub struct VmessInbound;

pub(crate) struct VmessAcceptedStreamState {
    pub(crate) upload_key: Vec<u8>,
    pub(crate) upload_nonce: Vec<u8>,
    pub(crate) download_key: Vec<u8>,
    pub(crate) download_nonce: Vec<u8>,
    pub(crate) cipher: VmessCipher,
    pub(crate) authenticated_length: bool,
    pub(crate) chunk_masking: bool,
    pub(crate) global_padding: bool,
    pub(crate) length_key_source: Vec<u8>,
    pub(crate) length_nonce_source: Vec<u8>,
}

pub(crate) struct VmessAccept {
    session: Session,
    stream_state: VmessAcceptedStreamState,
}

struct ParsedCommand {
    session: Session,
    body_key: Vec<u8>,
    body_nonce: Vec<u8>,
    response_header: u8,
    cipher: VmessCipher,
    authenticated_length: bool,
    chunk_masking: bool,
    global_padding: bool,
}

impl VmessInbound {
    pub fn protocol(&self) -> ProtocolType {
        ProtocolType::new("vmess")
    }

    /// Accept with a single known user.
    pub async fn accept_tcp_with_auth<S: AsyncSocket>(
        &self,
        stream: &mut S,
        user: &VmessUser,
    ) -> Result<Session, Error> {
        self.accept_tcp(stream, user)
            .await
            .map(VmessAccept::into_session)
    }

    pub(crate) async fn accept_tcp<S: AsyncSocket>(
        &self,
        stream: &mut S,
        user: &VmessUser,
    ) -> Result<VmessAccept, Error> {
        let buf = VmessReadBuffer::read(stream, std::slice::from_ref(user)).await?;
        let parsed = try_user(&buf, user)?;
        let (download_key, download_nonce) = send_auth_response(
            stream,
            &parsed.body_key,
            &parsed.body_nonce,
            parsed.response_header,
        )
        .await?;
        Ok(VmessAccept {
            session: parsed.session,
            stream_state: VmessAcceptedStreamState {
                upload_key: parsed.body_key.clone(),
                upload_nonce: parsed.body_nonce.clone(),
                download_key,
                download_nonce,
                cipher: parsed.cipher,
                authenticated_length: parsed.authenticated_length,
                chunk_masking: parsed.chunk_masking,
                global_padding: parsed.global_padding,
                length_key_source: parsed.body_key,
                length_nonce_source: parsed.body_nonce,
            },
        })
    }

    /// Read the wire auth packet once, then try each user in order.
    /// Returns the session built from the first user whose key successfully
    /// decrypts and verifies the auth header.
    pub async fn accept_tcp_with_auth_multi<S: AsyncSocket>(
        &self,
        stream: &mut S,
        users: &[VmessUser],
    ) -> Result<Session, Error> {
        self.accept_tcp_multi(stream, users)
            .await
            .map(VmessAccept::into_session)
    }

    pub(crate) async fn accept_tcp_multi<S: AsyncSocket>(
        &self,
        stream: &mut S,
        users: &[VmessUser],
    ) -> Result<VmessAccept, Error> {
        let buf = VmessReadBuffer::read(stream, users).await?;

        for user in users {
            match try_user(&buf, user) {
                Ok(parsed) => {
                    let (download_key, download_nonce) = send_auth_response(
                        stream,
                        &parsed.body_key,
                        &parsed.body_nonce,
                        parsed.response_header,
                    )
                    .await?;
                    return Ok(VmessAccept {
                        session: parsed.session,
                        stream_state: VmessAcceptedStreamState {
                            upload_key: parsed.body_key.clone(),
                            upload_nonce: parsed.body_nonce.clone(),
                            download_key,
                            download_nonce,
                            cipher: parsed.cipher,
                            authenticated_length: parsed.authenticated_length,
                            chunk_masking: parsed.chunk_masking,
                            global_padding: parsed.global_padding,
                            length_key_source: parsed.body_key,
                            length_nonce_source: parsed.body_nonce,
                        },
                    });
                }
                Err(_) => continue,
            }
        }

        // Send a generic rejection so the client gets a response
        let reject = [0x00u8, 0x00u8];
        let _ = stream.write_all(&reject).await;
        Err(Error::Protocol("vmess: no user matched"))
    }
}

impl VmessAccept {
    fn session(&self) -> &Session {
        &self.session
    }

    fn into_session(self) -> Session {
        self.session
    }

    pub(crate) fn into_stream_state(self) -> VmessAcceptedStreamState {
        self.stream_state
    }
}

/// Buffered wire data read once, tried against multiple users.
struct VmessReadBuffer {
    auth_id: [u8; AUTH_ID_LEN],
    nonce: [u8; 8],
    encrypted_payload: Vec<u8>,
}

impl VmessReadBuffer {
    async fn read<S: AsyncSocket>(stream: &mut S, users: &[VmessUser]) -> Result<Self, Error> {
        let mut auth_id = [0u8; AUTH_ID_LEN];
        read_exact(stream, &mut auth_id).await?;

        let mut encrypted_len = [0_u8; 18];
        read_exact(stream, &mut encrypted_len).await?;
        let mut nonce = [0_u8; 8];
        read_exact(stream, &mut nonce).await?;

        let mut header_len = None;
        for user in users {
            let cmd_key = derive_xray_cmd_key(&user.id);
            if let Ok(len) =
                open_xray_aead_header_length(&cmd_key, &auth_id, &encrypted_len, &nonce)
            {
                header_len = Some(len);
                break;
            }
        }

        let header_len = header_len.ok_or(Error::Protocol("vmess: no user matched"))?;
        let mut encrypted_payload = vec![0_u8; header_len + GCM_TAG_LEN];
        read_exact(stream, &mut encrypted_payload).await?;

        Ok(Self {
            auth_id,
            nonce,
            encrypted_payload,
        })
    }
}

/// Try one user against the buffered wire data.
/// Returns (session, body_key, auth_id, cipher) on success so the caller
/// can send the response through the live stream.
fn try_user(buf: &VmessReadBuffer, user: &VmessUser) -> Result<ParsedCommand, Error> {
    let cmd_key = derive_xray_cmd_key(&user.id);
    let plaintext =
        open_xray_aead_header_payload(&cmd_key, &buf.auth_id, &buf.nonce, &buf.encrypted_payload)?;
    parse_command_body(&plaintext, user)
}

/// Parse the decrypted command body.
fn parse_command_body(plaintext: &[u8], user: &VmessUser) -> Result<ParsedCommand, Error> {
    if plaintext.len() < 42 {
        return Err(Error::Protocol("vmess body too short"));
    }

    let version = plaintext[0];
    if version != VERSION {
        return Err(Error::Protocol("vmess unsupported version"));
    }

    let body_nonce = plaintext[1..17].to_vec();
    let body_key = plaintext[17..33].to_vec();
    let response_header = plaintext[33];
    let options = plaintext[34];
    let padding_len = (plaintext[35] >> 4) as usize;
    let cipher = match plaintext[35] & 0x0f {
        0x03 => VmessCipher::Aes128Gcm,
        0x04 => VmessCipher::Chacha20Poly1305,
        0x05 => VmessCipher::None,
        0x06 => VmessCipher::Zero,
        _ => user.cipher,
    };
    let command = plaintext[37];
    let mut pos = 38;

    let target = if command == 0x03 {
        zero_core::Address::Domain(crate::shared::MUX_COOL_DOMAIN.to_owned())
    } else {
        if plaintext.len() < pos + 3 {
            return Err(Error::Protocol("vmess command section too short"));
        }
        let port = u16::from_be_bytes([plaintext[pos], plaintext[pos + 1]]);
        pos += 2;
        let atyp = plaintext[pos];
        pos += 1;
        let addr_len = match atyp {
            0x01 => 4usize,
            0x02 => {
                if plaintext.len() <= pos {
                    return Err(Error::Protocol("vmess domain address truncated"));
                }
                1 + plaintext[pos] as usize
            }
            0x03 => 16usize,
            _ => return Err(Error::Protocol("vmess unknown address type")),
        };
        let addr_end = pos + addr_len;
        if plaintext.len() < addr_end {
            return Err(Error::Protocol("vmess address truncated"));
        }
        let target = parse_address_from_bytes(atyp, &plaintext[pos..addr_end])?;
        let network = match command {
            CMD_TCP => Network::Tcp,
            CMD_UDP => Network::Udp,
            _ => return Err(Error::Protocol("vmess unsupported command")),
        };
        let mut session = Session::new(0, target, port, network, ProtocolType::new("vmess"));
        apply_user_auth(&mut session, user);
        return Ok(ParsedCommand {
            session,
            body_key,
            body_nonce,
            response_header,
            cipher,
            authenticated_length: options & 0x10 != 0,
            chunk_masking: options & 0x04 != 0,
            global_padding: options & 0x08 != 0,
        });
    };

    let mut session = Session::new(
        0,
        target,
        crate::shared::MUX_COOL_PORT,
        Network::Tcp,
        ProtocolType::new("vmess"),
    );
    apply_user_auth(&mut session, user);
    let _ = padding_len;
    Ok(ParsedCommand {
        session,
        body_key,
        body_nonce,
        response_header,
        cipher,
        authenticated_length: options & 0x10 != 0,
        chunk_masking: options & 0x04 != 0,
        global_padding: options & 0x08 != 0,
    })
}

fn apply_user_auth(session: &mut Session, user: &VmessUser) {
    let auth = SessionAuth {
        scheme: "vmess-uuid".into(),
        principal_key: user
            .principal_key
            .clone()
            .or_else(|| Some(hex::encode(&user.id))),
        up_bps: user.up_bps,
        down_bps: user.down_bps,
        device_limit: user.device_limit,
        quota_remaining_bytes: user.quota_remaining_bytes,
        policy_revision: user.policy_revision,
    };
    session.apply_auth(auth);
}

async fn send_auth_response<S: AsyncSocket>(
    stream: &mut S,
    body_key: &[u8],
    body_nonce: &[u8],
    response_header: u8,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let resp_key = sha256_16(body_key)?;
    let resp_nonce = sha256_16(body_nonce)?;

    let plaintext = [response_header, 0x00, 0x00, 0x00];
    let buf = seal_xray_response_header(&resp_key, &resp_nonce, &plaintext)?;

    stream
        .write_all(&buf)
        .await
        .map_err(|_| Error::Io("vmess: failed to write response"))?;

    Ok((resp_key, resp_nonce))
}

fn sha256_16(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    Ok(digest.as_ref()[..16].to_vec())
}
