//! Native execution uses the same actual map and protocol wire driver.
mod stream;
use super::*;
mod datagram;
pub(crate) use datagram::DatagramIo;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::{
    pooled_driver::{PoolOptions, UserStream},
    RuntimeError,
};
type NativeCall = Option<(
    zero_platform_tokio::contract::network::CarrierContext,
    Option<zero_platform_tokio::contract::network::PendingCarrierObserver>,
)>;
impl TrojanMuxConnectionPool {
    pub(crate) fn native_eligible(&self) -> bool {
        self.control.native_eligible()
    }
    pub(crate) async fn installed_root_binding(
        &self,
    ) -> Result<Option<UpstreamBinding>, RuntimeError> {
        match self.control.installed()? {
            Some(owner) => owner.current_root_binding().await.map(Some),
            None => Ok(None),
        }
    }

    pub(crate) async fn open_native_udp_stream<S, F, Fut>(
        &self,
        key: TrojanMuxPoolKey,
        concurrency: u32,
        target: Address,
        port: u16,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        let (reserved, usage) = self
            .native_connection(
                key,
                concurrency,
                binding,
                None,
                wrap,
                |connection, sid, usage, _| {
                    Ok((
                        stream::StreamReservation::new(connection, sid, target, port, Network::Udp),
                        usage,
                    ))
                },
            )
            .await?;
        let stream = reserved.open().await?;
        Ok(UserStream::new(stream, usage))
    }
    pub(crate) fn bind_current(
        &self,
        expected: zero_core::component::ComponentInstanceId,
    ) -> Result<Self, RuntimeError> {
        let mut current = self.clone();
        current.control = current.control.refresh()?;
        if current.native_instance()? != expected {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "native pool generation differs from the actual dependency",
            )
            .into());
        }
        Ok(current)
    }

    pub(crate) fn native_instance(
        &self,
    ) -> Result<zero_core::component::ComponentInstanceId, RuntimeError> {
        Ok(self.control.managed()?.instance())
    }
    pub(crate) fn native_options(
        &self,
        key: &TrojanMuxPoolKey,
        concurrency: u32,
        endpoint: zero_core::request::Destination,
        binding: UpstreamBinding,
        limit: std::num::NonZeroUsize,
        budget: Duration,
    ) -> Result<PoolOptions, RuntimeError> {
        let options = self.control.options(
            format!(
                "{:?}:{key:?}:{concurrency}:native-upload-fifo-v1:8:66559",
                self.scope
            )
            .into(),
            endpoint,
            binding,
            limit,
            budget,
        )?;
        let entries = Arc::downgrade(&self.pool);
        Ok(options.on_confirmed_close(move |instance| {
            if let Some(entries) = entries.upgrade() {
                let retired = {
                    let mut entries = entries.lock().unwrap();
                    entries
                        .extract_if(|_, connection| {
                            connection
                                .original
                                .as_ref()
                                .is_some_and(|original| original.instance() == instance)
                        })
                        .map(|(_, connection)| connection)
                        .collect::<Vec<_>>()
                };
                // Matching whole-owner closure authorizes only reclamation.
                // Final protocol/channel drops must run outside the pool lock.
                drop(retired);
            }
        }))
    }
    async fn native_connection<S, F, Fut, R>(
        &self,
        key: TrojanMuxPoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        call: NativeCall,
        wrap: F,
        finish: impl FnOnce(
            Arc<TrojanMuxConn>,
            u16,
            zero_transport::pooled_driver::PoolUse,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ) -> Result<R, RuntimeError>,
    ) -> Result<R, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        let owner = self.control.managed()?;
        let profile = format!(
            "{:?}:{key:?}:{concurrency}:native-upload-fifo-v1:8:66559",
            self.scope
        );
        let _operation = owner.operation()?;
        let _connecting = owner.serialize().await?;
        owner.prepare_epoch(&profile, &binding).await?;
        let selected = binding.clone();
        let opening = owner.clone();
        let (connection, sid) = self
            .get_or_create_conn_with_binding(
                key,
                concurrency,
                Some(binding.clone()),
                |key, concurrency| async move {
                    let prepare = move |raw| async move {
                        let io = wrap(raw).await?;
                        let io = key.establish_mux_outbound_stream(io).await?;
                        Ok(key.prepare_pool_conn(io, concurrency))
                    };
                    let (mut connection, original) = match call {
                        Some((call, waiting)) => {
                            opening
                                .open_observed(&profile, &binding, call, waiting, prepare)
                                .await?
                        }
                        None => opening.open_owned(&profile, &binding, prepare).await?,
                    };
                    connection.original = Some(original);
                    Ok::<_, RuntimeError>(connection)
                },
            )
            .await?;
        owner.verify_epoch(&selected)?;
        let original = connection
            .original
            .as_ref()
            .ok_or_else(|| std::io::Error::other("native MUX entry lost its original owner"))?;
        if original.instance() != owner.instance() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "stale MUX owner",
            )
            .into());
        }
        let usage = original.user().map_err(std::io::Error::other)?;
        finish(
            connection.clone(),
            sid,
            usage,
            original.origin().map_err(std::io::Error::other)?,
        )
    }
    pub(crate) async fn open_native<S, F, Fut>(
        &self,
        key: TrojanMuxPoolKey,
        concurrency: u32,
        target: Address,
        port: u16,
        binding: UpstreamBinding,
        call: NativeCall,
        wrap: F,
    ) -> Result<zero_transport::pooled_driver::PooledStream, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        let (reserved, usage, origin) = self
            .native_connection(
                key,
                concurrency,
                binding,
                call,
                wrap,
                |connection, sid, usage, origin| {
                    Ok((
                        stream::StreamReservation::new(connection, sid, target, port, Network::Tcp),
                        usage,
                        origin,
                    ))
                },
            )
            .await?;
        let stream = reserved.open().await?;
        Ok(zero_transport::pooled_driver::PooledStream {
            origin,
            io: Box::pin(UserStream::new(stream, usage)),
        })
    }
    pub(crate) async fn open_native_udp<S, F, Fut>(
        &self,
        key: TrojanMuxPoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        call: NativeCall,
        wrap: F,
    ) -> Result<zero_transport::pooled_driver::datagram::PooledDatagram<DatagramIo>, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut + Send + 'static,
        Fut: Future<Output = Result<S, RuntimeError>> + Send + 'static,
    {
        self.native_connection(
            key,
            concurrency,
            binding,
            call,
            wrap,
            |connection, sid, usage, origin| {
                let writer = match connection.native_udp_tx.writer(encode_end_stream(sid)?) {
                    Ok(writer) => writer,
                    Err(source) => {
                        connection.release_stream(sid);
                        return Err(source.into());
                    }
                };
                let (tx, rx) = mpsc::channel(connection.response_backlog_frames + 1);
                connection.streams.lock().unwrap().insert(sid, tx);
                Ok(zero_transport::pooled_driver::datagram::PooledDatagram {
                    io: DatagramIo {
                        connection,
                        sid,
                        rx,
                        writer: Some(writer),
                        first: true,
                        target: None,
                    },
                    usage,
                    origin,
                })
            },
        )
        .await
    }
}
