use super::*;

pub struct RealityClientOptions<'a> {
    pub spider_x: &'a str,
    pub hybrid_key_exchange: bool,
    pub mldsa65_verify: Option<&'a str>,
    pub public_key: &'a str,
    pub short_id: &'a str,
    pub server_name: &'a str,
    pub cipher_suites: &'a [String],
    pub client_fingerprint: &'a str,
}

pub struct RealityServerOptions<'a> {
    pub policy: crate::reality_policy::ServerPolicy,
    pub mldsa65_seed: Option<&'a str>,
    pub private_key: &'a str,
    pub short_ids: &'a [String],
    pub server_name: &'a str,
    pub cipher_suites: &'a [String],
}

#[derive(Clone)]
pub struct VlessRealityServerProfile {
    prepared: Option<std::sync::Arc<RealityServerConfig>>,
    target: Option<super::super::target::Profile>,
    target_probes: super::super::target::ProbeAccess,
    policy: crate::reality_policy::ServerPolicy,
    mldsa65_seed: Option<String>,
    private_key: String,
    short_ids: Vec<String>,
    server_name: String,
    cipher_suites: Vec<String>,
}

impl VlessRealityServerProfile {
    pub fn with_target(mut self, target: Option<super::super::target::Profile>) -> Self {
        self.target = target;
        self
    }
    pub(crate) fn with_target_probes(mut self, probes: super::super::target::ProbeAccess) -> Self {
        self.target_probes = probes;
        self
    }
    pub fn resolve_target_path(mut self, base: Option<&std::path::Path>) -> Self {
        if let Some(target) = &mut self.target {
            target.resolve_path(base);
        }
        self
    }
    pub async fn accept_inbound(
        &self,
        io: zero_platform_tokio::TcpRelayStream,
        connector: Option<&zero_transport::handshake_target::Connector>,
    ) -> io::Result<super::super::target::Acceptance> {
        if let Some(target) = &self.target {
            let connector = connector.ok_or_else(|| {
                io::Error::other("REALITY target requires runtime network services")
            })?;
            return super::super::target::accept(
                io,
                self.server_config()?,
                target,
                connector,
                &self.target_probes,
            )
            .await;
        }
        self.upgrade_server(io)
            .await
            .map(super::super::target::Acceptance::Established)
    }
    fn server_config(&self) -> io::Result<RealityServerConfig> {
        if let Some(prepared) = &self.prepared {
            return Ok(prepared.as_ref().clone());
        }
        make_server_config(RealityServerOptions {
            policy: self.policy.clone(),
            mldsa65_seed: self.mldsa65_seed.as_deref(),
            private_key: &self.private_key,
            short_ids: &self.short_ids,
            server_name: &self.server_name,
            cipher_suites: &self.cipher_suites,
        })
    }

    pub fn with_policy(mut self, policy: crate::reality_policy::PolicyRef<'_>) -> io::Result<Self> {
        self.policy = crate::reality_policy::ServerPolicy::new(policy, Some(&self.server_name))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        self.prepared = None;
        self.prepared = Some(std::sync::Arc::new(self.server_config()?));
        Ok(self)
    }
    pub fn with_mldsa65_seed(mut self, seed: Option<&str>) -> Self {
        self.mldsa65_seed = seed.map(str::to_owned);
        self.prepared = None;
        self
    }
    pub fn new(
        private_key: impl Into<String>,
        short_ids: Vec<String>,
        server_name: Option<&str>,
        cipher_suites: Vec<String>,
    ) -> Self {
        Self {
            prepared: None,
            target: None,
            target_probes: Default::default(),
            policy: crate::reality_policy::ServerPolicy::for_name(server_name),
            private_key: private_key.into(),
            mldsa65_seed: None,
            short_ids,
            server_name: server_name.unwrap_or("localhost").to_owned(),
            cipher_suites,
        }
    }

    pub fn from_config_parts(
        private_key: impl Into<String>,
        short_ids: Vec<String>,
        server_name: Option<String>,
        cipher_suites: Vec<String>,
    ) -> Self {
        Self::new(
            private_key,
            short_ids,
            server_name.as_deref(),
            cipher_suites,
        )
    }

    pub fn from_config_server(
        private_key: impl Into<String>,
        short_ids: Vec<String>,
        server_name: Option<String>,
        cipher_suites: Vec<String>,
    ) -> Self {
        Self::from_config_parts(private_key, short_ids, server_name, cipher_suites)
    }

    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    pub async fn upgrade_server<IO>(&self, io: IO) -> io::Result<RealityTlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin,
    {
        let mut io = io;
        let mut session = RealityServerConnection::new(self.server_config()?);
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            perform_reality_server_handshake(&mut session, &mut io),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "REALITY handshake timed out"))??;
        Ok(RealityTlsStream::new_server(io, session))
    }
}

pub fn generate_reality_key_pair() -> (String, String) {
    let mut private_key = [0u8; 32];
    rand::rng().fill_bytes(&mut private_key);
    let public_key = PublicKey::from(&StaticSecret::from(private_key));
    (encode_key(&private_key), encode_key(public_key.as_bytes()))
}

pub async fn upgrade_reality_client<IO>(
    mut io: IO,
    options: RealityClientOptions<'_>,
) -> io::Result<RealityTlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let spider = crate::reality_spider::Profile::parse(options.spider_x)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let config = RealityClientConfig {
        decoy_trust: None,
        hybrid_key_exchange: options.hybrid_key_exchange,
        mldsa65: options
            .mldsa65_verify
            .map(super::super::mldsa::verifier)
            .transpose()?,
        public_key: decode_public_key(options.public_key)?,
        short_id: decode_short_id(options.short_id)?,
        server_name: options.server_name.to_owned(),
        cipher_suites: parse_cipher_suites(options.cipher_suites)?,
        client_hello_profile: options
            .client_fingerprint
            .parse()
            .map_err(|error: String| io::Error::new(io::ErrorKind::InvalidInput, error))?,
        handshake_timeout_ms: 10_000,
    };

    let mut session = RealityClientConnection::new(config)?;
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        perform_reality_handshake(&mut session, &mut io),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "REALITY handshake timed out"))??;

    admit_client(io, session, options.server_name, spider).await
}

async fn admit_client<IO>(
    io: IO,
    session: RealityClientConnection,
    server_name: &str,
    spider: crate::reality_spider::Profile,
) -> io::Result<RealityTlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if !session.is_authenticated_reality() {
        zero_transport::http_navigation::start(
            RealityTlsStream::new(io, session),
            server_name.to_owned(),
            zero_transport::http_navigation::Plan {
                path: spider.path,
                ranges: spider.ranges,
            },
        )
        .await;
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "REALITY received a real target certificate without proxy authentication",
        ));
    }
    Ok(RealityTlsStream::new(io, session))
}

#[cfg(test)]
#[path = "../../../tests/reality/decoy.rs"]
mod decoy_tests;

pub async fn upgrade_reality_server<IO>(
    mut io: IO,
    options: RealityServerOptions<'_>,
) -> io::Result<RealityTlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let config = make_server_config(options)?;

    let mut session = RealityServerConnection::new(config);
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        perform_reality_server_handshake(&mut session, &mut io),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "REALITY handshake timed out"))??;

    Ok(RealityTlsStream::new_server(io, session))
}

fn make_server_config(options: RealityServerOptions<'_>) -> io::Result<RealityServerConfig> {
    Ok(RealityServerConfig {
        policy: options.policy,
        mldsa65: options
            .mldsa65_seed
            .map(super::super::mldsa::signer)
            .transpose()?,
        private_key: decode_private_key(options.private_key)?,
        short_ids: options
            .short_ids
            .iter()
            .map(|short_id| decode_short_id(short_id))
            .collect::<io::Result<Vec<_>>>()?,
        server_name: options.server_name.to_owned(),
        cipher_suites: parse_cipher_suites(options.cipher_suites)?,
        handshake_timeout_ms: 10_000,
    })
}

fn parse_cipher_suites(names: &[String]) -> io::Result<Vec<CipherSuite>> {
    if names.is_empty() {
        return Ok(DEFAULT_CIPHER_SUITES.to_vec());
    }

    names
        .iter()
        .map(|name| {
            CipherSuite::from_name(name).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported reality cipher suite `{name}`"),
                )
            })
        })
        .collect()
}

impl PartialEq for VlessRealityServerProfile {
    fn eq(&self, other: &Self) -> bool {
        self.target == other.target
            && self.policy == other.policy
            && self.mldsa65_seed == other.mldsa65_seed
            && self.private_key == other.private_key
            && self.short_ids == other.short_ids
            && self.server_name == other.server_name
            && self.cipher_suites == other.cipher_suites
    }
}
impl Eq for VlessRealityServerProfile {}
impl std::fmt::Debug for VlessRealityServerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessRealityServerProfile")
            .field("target", &self.target)
            .field("policy", &self.policy)
            .field("server_name", &self.server_name)
            .field("cipher_suites", &self.cipher_suites)
            .field("mldsa65", &self.mldsa65_seed.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[path = "../../../tests/reality/tls_stream.rs"]
mod generic_tls_tests;
