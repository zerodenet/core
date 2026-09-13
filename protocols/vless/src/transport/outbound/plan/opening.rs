use super::*;

impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) async fn open_direct<F, Fut, E>(
        &self,
        open: F,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        F: Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TokioSocket, E>> + Send + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        self.encrypt(self.open_direct_unencrypted(open, sockets).await?)
            .await
    }

    pub(in crate::transport) async fn open_direct_unencrypted<F, Fut, E>(
        &self,
        open: F,
        sockets: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        F: Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TokioSocket, E>> + Send + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        if self.uses_browser_dialer() {
            return self.open_browser_carrier().await;
        }
        let sockets = sockets.with_final_mask(self.final_mask.clone());
        if let (Some(pool), Some(profile)) = (&self.grpc_pool, &self.transport.grpc) {
            let mut carrier = self.clone();
            carrier.transport.grpc = None;
            carrier.grpc_pool = None;
            let authority = self
                .transport
                .tls
                .as_ref()
                .and_then(|tls| tls.server_name.as_deref())
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    self.server
                        .parse::<std::net::IpAddr>()
                        .map(|ip| std::net::SocketAddr::new(ip, self.port).to_string())
                        .unwrap_or_else(|_| self.server.clone())
                });
            let stream = pool
                .open_carrier(profile, &authority, || {
                    carrier.open_direct_carrier(open, sockets)
                })
                .await?;
            return Ok(TcpRelayStream::new(stream));
        }
        if self.xhttp_pool.is_some() && self.transport.split_http.is_some() {
            return self.open_xhttp_unencrypted(open, Some(sockets)).await;
        }
        self.open_direct_carrier(open, sockets).await
    }
    pub(in crate::transport) async fn open_relay(
        &self,
        stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.reject_browser_relay()?;
        self.encrypt(self.open_relay_carrier(stream).await?).await
    }
    pub(in crate::transport) async fn build_relay_two_stream_udp_transport(
        &self,
        post: TcpRelayStream,
        get: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.reject_browser_relay()?;
        self.encrypt(self.build_relay_two_stream_carrier(post, get).await?)
            .await
    }

    pub(super) async fn open_direct_carrier<OpenSocket, OpenSocketFut, E>(
        &self,
        open_socket: OpenSocket,
        socket_factory: zero_transport::OutboundDatagramSocketFactory,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        OpenSocket: FnOnce(&str, u16) -> OpenSocketFut,
        OpenSocketFut: Future<Output = Result<TokioSocket, E>> + Send + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        if let (Some(profile), Some(pool)) = (&self.hysteria, &self.hysteria_pool) {
            let socket_factory = profile.socket_factory(socket_factory);
            let quic = self.transport.quic.as_ref().ok_or_else(|| {
                std::io::Error::other("Hysteria carrier requires QUIC TLS settings")
            })?;
            let ca = quic.ca_cert_path.as_deref().map(|path| {
                self.transport
                    .source_dir
                    .as_deref()
                    .unwrap_or(std::path::Path::new("."))
                    .join(path)
            });
            let mut config = zero_transport::quic::client_config_with_options(
                quic.insecure,
                None,
                &[b"h3".to_vec()],
                None,
                ca.as_deref(),
                &quic.tls_options,
            )?;
            config.transport_config(std::sync::Arc::new(profile.transport(false)?));
            let stream = pool
                .open(profile, || {
                    zero_transport::quic::connect_quic_endpoint(
                        self.server(),
                        self.port(),
                        quic.server_name.as_deref().unwrap_or(self.server()),
                        config,
                        &socket_factory,
                    )
                })
                .await?;
            return Ok(TcpRelayStream::new(stream));
        }
        let transport = self.transport();
        if let Some(settings) = self.mkcp {
            let stream = zero_transport::mkcp::connect(
                self.server(),
                self.port(),
                settings,
                &socket_factory,
            )
            .await?;
            return self.open_relay_carrier(TcpRelayStream::new(stream)).await;
        }
        if transport.quic.is_some() {
            let quic = transport.quic;
            return build_vless_direct_outbound_transport(VlessDirectTransportRequest {
                socket: None,
                options: transport.stream_options(),
                quic,
                socket_factory,
                server: self.server(),
                port: self.port(),
            })
            .await;
        }

        let socket = open_socket(self.server(), self.port())
            .await
            .map_err(Into::into)?;
        if !self.final_mask.tcp().is_empty() {
            return self.open_relay_carrier(TcpRelayStream::from(socket)).await;
        }
        build_vless_udp_outbound_transport(VlessUdpOutboundTransportRequest {
            socket,
            options: transport,
            socket_factory,
            server: self.server(),
            port: self.port(),
        })
        .await
    }

    pub(super) async fn open_relay_carrier(
        &self,
        stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let stream =
            zero_transport::finalmask::tcp::wrap_prepared(stream, self.final_mask.tcp(), false)
                .await?;
        build_vless_outbound_transport_over_stream(VlessFinalHopTransportRequest {
            carrier: RelayCarrier {
                stream,
                server: self.server().to_owned(),
                port: self.port(),
            },
            options: self.stream_transport_options(),
        })
        .await
    }

    async fn build_relay_two_stream_carrier(
        &self,
        post_stream: TcpRelayStream,
        get_stream: TcpRelayStream,
    ) -> Result<TcpRelayStream, RuntimeError> {
        build_vless_split_http_over_relay(post_stream, get_stream, self.transport(), self.server())
            .await
    }
}
