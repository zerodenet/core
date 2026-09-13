use super::*;
use std::sync::Arc;
impl OwnedVlessOutboundTransportPlan {
    pub(super) async fn open_xhttp<F, Fut, E, S>(
        &self,
        open: F,
        sockets: Option<zero_transport::OutboundDatagramSocketFactory>,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        F: Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        self.encrypt(self.open_xhttp_unencrypted(open, sockets).await?)
            .await
    }

    pub(in crate::transport) async fn open_xhttp_unencrypted<F, Fut, E, S>(
        &self,
        open: F,
        sockets: Option<zero_transport::OutboundDatagramSocketFactory>,
    ) -> Result<TcpRelayStream, RuntimeError>
    where
        F: Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        let pool = self.xhttp_pool.as_ref().unwrap();
        let mut profile = self.transport.split_http.clone().unwrap();
        let mode = if self.download.is_some()
            && self.transport.reality.is_some()
            && split_http::XhttpMode::parse(&profile.mode) == split_http::XhttpMode::Auto
        {
            split_http::XhttpMode::StreamUp
        } else {
            super::super::xhttp::mode(self.stream_transport_options())
        };
        profile.mode = mode.as_str().to_owned();
        let open = Arc::new(open);
        let factory = self.xhttp_factory(open.clone(), sockets.clone());
        let download = self
            .download
            .as_ref()
            .map(|plan| split_http::XhttpDownload {
                pool: plan.xhttp_pool.as_ref().unwrap(),
                factory: plan.xhttp_factory(open, sockets),
                config: plan.transport.split_http.as_ref().unwrap(),
            });
        let stream = pool
            .connect_with_download(factory, &profile, download)
            .await?;
        Ok(TcpRelayStream::new(stream))
    }
    fn xhttp_factory<F, Fut, E, S>(
        &self,
        open: Arc<F>,
        sockets: Option<zero_transport::OutboundDatagramSocketFactory>,
    ) -> split_http::XhttpCarrierFactory
    where
        F: Fn(&str, u16) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<S, E>> + Send + 'static,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        E: Into<RuntimeError> + Send + 'static,
    {
        let mut plan = self.clone();
        // The factory owns only carrier configuration, never its own pool.
        plan.xhttp_pool = None;
        plan.download = None;
        plan.encryption = None;
        Arc::new(move || {
            let plan = plan.clone();
            let open = open.clone();
            let sockets = sockets.clone();
            Box::pin(async move {
                plan.open_xhttp_carrier(move |server, port| open(server, port), sockets)
                    .await
            })
        })
    }
}

impl OwnedVlessOutboundTransportPlan {
    async fn open_xhttp_carrier<F, Fut, E, S>(
        &self,
        open: F,
        sockets: Option<zero_transport::OutboundDatagramSocketFactory>,
    ) -> Result<split_http::XhttpCarrier, RuntimeError>
    where
        F: Fn(&str, u16) -> Fut,
        Fut: Future<Output = Result<S, E>>,
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + Sync + 'static,
        E: Into<RuntimeError>,
    {
        if let Some(quic) = &self.transport.quic {
            return super::super::direct::open_vless_xhttp_quic(
                self.server(),
                self.port(),
                quic,
                self.transport.source_dir.as_deref(),
                sockets.as_ref().ok_or_else(|| {
                    std::io::Error::other("QUIC requires a datagram relay carrier")
                })?,
                self.transport
                    .split_http
                    .as_ref()
                    .unwrap()
                    .options
                    .xmux
                    .h_keep_alive_period,
            )
            .await
            .map(split_http::XhttpCarrier::Http3);
        }
        let socket = open(self.server(), self.port()).await.map_err(Into::into)?;
        let socket = zero_transport::finalmask::tcp::wrap_prepared(
            TcpRelayStream::new(socket),
            self.final_mask.tcp(),
            false,
        )
        .await?;
        let options = self.stream_transport_options();
        let h2 = options.tls.is_some() || options.reality.is_some();
        let stream = super::super::xhttp::carrier(socket, options, self.server(), h2).await?;
        Ok(if h2 {
            split_http::XhttpCarrier::Http2(stream)
        } else {
            split_http::XhttpCarrier::Http1(stream)
        })
    }
}

impl OwnedVlessOutboundTransportPlan {
    pub(in crate::transport) async fn open_relay_connector(
        &self,
        connector: zero_transport::relay_connector::RelayStreamConnector,
        pools: &crate::transport::runtime::xhttp::PoolAccess,
        tag: &str,
    ) -> Result<TcpRelayStream, RuntimeError> {
        self.reject_browser_relay()?;
        let mut plan = self.clone();
        let identity = format!(
            "relay:{}:{}:{self:?}",
            connector.generation(),
            connector.identity()
        );
        if let Some(profile) = &self.hysteria {
            let identity = format!("{identity}:{:?}", profile.cache_identity());
            plan.hysteria_pool = Some(pools.hysteria_pool(tag, &identity));
        }
        if self.uses_datagrams() && self.transport.split_http.is_none() {
            let sockets = connector
                .datagrams()
                .ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        "relay prefix cannot supply the required datagram carrier",
                    )
                })?
                .with_final_mask(self.final_mask.clone());
            return plan
                .encrypt(
                    plan.open_direct_carrier(
                        |_, _| async {
                            Err::<TokioSocket, RuntimeError>(
                                std::io::Error::other("datagram carrier attempted a TCP dial")
                                    .into(),
                            )
                        },
                        sockets,
                    )
                    .await?,
                )
                .await;
        }
        if let Some(profile) = &self.transport.grpc {
            let pool = pools.grpc_pool(tag, &identity);
            let mut carrier = self.clone();
            carrier.transport.grpc = None;
            carrier.grpc_pool = None;
            let authority = self
                .transport
                .tls
                .as_ref()
                .and_then(|tls| tls.server_name.as_deref())
                .filter(|name| !name.is_empty())
                .unwrap_or(self.server());
            let stream = pool
                .open_carrier(profile, authority, || async {
                    let raw = connector
                        .connect(self.server().to_owned(), self.port())
                        .await?;
                    carrier.open_relay_carrier(raw).await
                })
                .await?;
            return self.encrypt(TcpRelayStream::new(stream)).await;
        }
        if self.transport.split_http.is_none() {
            return self
                .open_relay(
                    connector
                        .connect(self.server().to_owned(), self.port())
                        .await?,
                )
                .await;
        }
        let sockets = connector
            .datagrams()
            .map(|factory| factory.with_final_mask(self.final_mask.clone()));
        plan.scope_relay_pool(pools, tag, &connector);
        plan.open_xhttp(
            move |server, port| connector.connect(server.to_owned(), port),
            sockets,
        )
        .await
    }
    fn scope_relay_pool(
        &mut self,
        pools: &crate::transport::runtime::xhttp::PoolAccess,
        tag: &str,
        connector: &zero_transport::relay_connector::RelayStreamConnector,
    ) {
        let identity = format!(
            "relay:{}:{}:{self:?}",
            connector.generation(),
            connector.identity()
        );
        let config = self.transport.split_http.as_ref().unwrap().options.xmux;
        self.xhttp_pool = Some(pools.pool(tag, &identity, config));
        if let Some(download) = self.download.as_mut() {
            download.scope_relay_pool(pools, tag, connector);
        }
    }
}
