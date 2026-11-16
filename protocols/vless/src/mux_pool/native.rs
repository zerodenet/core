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
impl MuxConnectionPool {
    pub(crate) fn with_native_policy(mut self, tcp: Option<u32>, udp: Option<u32>) -> Self {
        self.native_policy = Some((tcp, udp));
        self
    }
    fn native_profile(&self, key: &PoolKey, concurrency: u32) -> Result<String, RuntimeError> {
        let policy = match self.native_policy {
            Some((tcp, udp)) if tcp == Some(concurrency) || udp == Some(concurrency) => {
                format!("tcp={tcp:?}:udp={udp:?}")
            }
            Some(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "native MUX request differs from its actual TCP/XUDP concurrency policy",
                )
                .into())
            }
            None => concurrency.to_string(),
        };
        Ok(format!(
            "{:?}:{key:?}:{policy}:native-upload-fifo-v1:8:66559",
            self.scope
        ))
    }

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

    pub(crate) fn native_stream_dependency(
        &self,
    ) -> Result<Option<zero_core::component::ComponentInstanceId>, RuntimeError> {
        Ok(self.control.managed()?.stream_dependency())
    }
    pub(crate) fn native_instance(
        &self,
    ) -> Result<zero_core::component::ComponentInstanceId, RuntimeError> {
        Ok(self.control.managed()?.instance())
    }
    pub(crate) fn native_options(
        &self,
        key: &PoolKey,
        concurrency: u32,
        endpoint: zero_core::request::Destination,
        binding: UpstreamBinding,
        limit: std::num::NonZeroUsize,
        budget: Duration,
    ) -> Result<PoolOptions, RuntimeError> {
        let options = self.control.options(
            self.native_profile(key, concurrency)?.into(),
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
        key: PoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        call: NativeCall,
        wrap: F,
        finish: impl FnOnce(
            Arc<MuxPoolConn>,
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
        let profile = self.native_profile(&key, concurrency)?;
        let _operation = owner.operation()?;
        let _connecting = owner.serialize_binding(&binding).await?;
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
                        let mut io = wrap(raw).await?;
                        key.establish_mux_connection(&mut io).await?;
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
        key: PoolKey,
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
                        stream::StreamReservation::new(connection, sid, port, &target),
                        usage,
                        origin,
                    ))
                },
            )
            .await?;
        let mut stream = reserved.open().await?;
        // The actual NEW must enter the bounded FIFO before connect returns.
        // Interrupted readiness retains no encoded user DATA or detached work.
        std::future::poll_fn(|cx| stream.poll_native_initial(cx)).await?;
        Ok(zero_transport::pooled_driver::PooledStream {
            origin,
            io: Box::pin(UserStream::new(stream, usage)),
        })
    }
    pub(crate) async fn open_native_udp_with_id<S, F, Fut>(
        &self,
        key: PoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        call: NativeCall,
        global_id: Option<[u8; 8]>,
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
                let writer = match connection.native_udp_tx.writer(encode_mux_end_frame(sid)?) {
                    Ok(writer) => writer,
                    Err(source) => {
                        connection.release_stream(sid);
                        return Err(source.into());
                    }
                };
                let (tx, rx) = mpsc::channel(connection.response_backlog_frames + 1);
                connection.streams.lock().unwrap().insert(
                    sid,
                    MuxClientStreamState {
                        downlink: MuxClientDownlink::Udp(tx),
                        target: None,
                    },
                );
                Ok(zero_transport::pooled_driver::datagram::PooledDatagram {
                    io: DatagramIo {
                        block_udp443: false,
                        observer: None,
                        session: None,
                        connection,
                        sid,
                        rx,
                        writer: Some(writer),
                        first: true,
                        origin: None,
                        global_id,
                    },
                    usage,
                    origin,
                })
            },
        )
        .await
    }
}

/// Existing reverse worker uses this same physical FIFO and decoder map.
/// The caller keeps its actual accepted driver and separate logical custody.
pub(crate) fn reverse_stream(
    connection: Arc<MuxPoolConn>,
    sid: u16,
    destination: zero_core::request::Destination,
    origin: crate::mux::origin::Origin,
) -> impl Future<Output = Result<zero_platform_tokio::contract::stream::BoxStream, RuntimeError>> + Send
{
    let reserved =
        stream::StreamReservation::new(connection, sid, destination.port, &destination.address)
            .with_origin(origin);
    async move {
        let mut io = reserved.open().await?;
        std::future::poll_fn(|cx| io.poll_native_initial(cx)).await?;
        Ok(Box::pin(io) as zero_platform_tokio::contract::stream::BoxStream)
    }
}
pub(crate) fn reverse_datagram(
    connection: Arc<MuxPoolConn>,
    sid: u16,
    origin: crate::mux::origin::Origin,
    observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    session: Option<zero_core::component::SessionId>,
) -> impl Future<Output = Result<DatagramIo, RuntimeError>> + Send {
    struct Reserved {
        connection: Arc<MuxPoolConn>,
        sid: Option<u16>,
    }
    impl Drop for Reserved {
        fn drop(&mut self) {
            if let Some(sid) = self.sid.take() {
                self.connection.release_stream(sid);
            }
        }
    }
    let mut reserved = Reserved {
        connection: connection.clone(),
        sid: Some(sid),
    };
    let (tx, rx) = mpsc::channel(connection.response_backlog_frames);
    connection.streams.lock().unwrap().insert(
        sid,
        MuxClientStreamState {
            downlink: MuxClientDownlink::Udp(tx),
            target: None,
        },
    );
    async move {
        let writer = connection
            .native_udp_tx
            .writer_when_ready(encode_mux_end_frame(sid)?)
            .await?;
        reserved.sid.take();
        Ok(DatagramIo {
            block_udp443: false,
            observer,
            session,
            connection,
            sid,
            rx,
            writer: Some(writer),
            first: true,
            origin: Some(origin),
            global_id: None,
        })
    }
}

pub(crate) fn reverse_control(
    connection: Arc<MuxPoolConn>,
    sid: u16,
) -> Result<DatagramIo, RuntimeError> {
    let writer = match connection.native_udp_tx.writer(encode_mux_end_frame(sid)?) {
        Ok(writer) => writer,
        Err(source) => {
            connection.release_stream(sid);
            return Err(source.into());
        }
    };
    let (tx, rx) = mpsc::channel(connection.response_backlog_frames);
    connection.streams.lock().unwrap().insert(
        sid,
        MuxClientStreamState {
            downlink: MuxClientDownlink::Udp(tx),
            target: None,
        },
    );
    Ok(DatagramIo {
        block_udp443: false,
        observer: None,
        session: None,
        connection,
        sid,
        rx,
        writer: Some(writer),
        first: true,
        origin: None,
        global_id: None,
    })
}
