//! Cold REALITY client policy. Ephemeral keys and timestamps are created per open.
use super::*;
use core::future::Future;

#[derive(Clone)]
pub struct PreparedRealityClient {
    config: RealityClientConfig,
    spider: crate::reality_spider::Profile,
}
impl core::fmt::Debug for PreparedRealityClient {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PreparedRealityClient")
            .field("server_name", &self.config.server_name)
            .field("hybrid_key_exchange", &self.config.hybrid_key_exchange)
            .finish_non_exhaustive()
    }
}
impl PreparedRealityClient {
    pub fn from_options(options: RealityClientOptions<'_>) -> io::Result<Self> {
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
            server_name: options.server_name.into(),
            cipher_suites: super::profile::parse_cipher_suites(options.cipher_suites)?,
            client_hello_profile: options
                .client_fingerprint
                .parse()
                .map_err(|error: String| io::Error::new(io::ErrorKind::InvalidInput, error))?,
            handshake_timeout_ms: 10_000,
        };
        Ok(Self { config, spider })
    }
    pub async fn connect<IO>(&self, io: IO) -> io::Result<RealityTlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.connect_with_navigation(io, |stream, host, plan| async move {
            zero_transport::http_navigation::start(stream, host, plan).await;
        })
        .await
    }
    /// The supplied executor owns the authenticated decoy carrier after proxy
    /// authentication fails. It can never grant REALITY proxy admission.
    pub async fn connect_with_navigation<IO, Start, Run>(
        &self,
        mut io: IO,
        start: Start,
    ) -> io::Result<RealityTlsStream<IO>>
    where
        IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        Start: FnOnce(RealityTlsStream<IO>, String, zero_transport::http_navigation::Plan) -> Run,
        Run: Future<Output = ()>,
    {
        let mut session = RealityClientConnection::new(self.config.clone())?;
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            perform_reality_handshake(&mut session, &mut io),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "REALITY handshake timed out"))??;
        if !session.is_authenticated_reality() {
            start(
                RealityTlsStream::new(io, session),
                self.config.server_name.clone(),
                zero_transport::http_navigation::Plan {
                    path: self.spider.path.clone(),
                    ranges: self.spider.ranges,
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
}

#[cfg(test)]
#[path = "../../../tests/reality/prepared_client.rs"]
mod tests;
