//! Native execution uses the same actual map and protocol wire driver.
mod stream;
use super::*;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::{
    pooled_driver::{PoolOptions, UserStream},
    RuntimeError,
};
impl VmessMuxConnectionPool {
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
    /// The old UDP codec keeps its byte-stream API, backed by the same native
    /// map, actual physical users and supervised protocol driver as Stream.
    pub(crate) async fn open_native_udp_stream<S, F, Fut>(
        &self,
        key: VmessMuxPoolKey,
        concurrency: u32,
        target: Address,
        port: u16,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<impl AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut,
        Fut: Future<Output = Result<S, RuntimeError>>,
    {
        let (reserved, usage) = self
            .native_connection(
                key,
                concurrency,
                binding,
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
        key: &VmessMuxPoolKey,
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
        key: VmessMuxPoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        wrap: F,
        finish: impl FnOnce(
            Arc<VmessMuxConn>,
            u16,
            zero_transport::pooled_driver::PoolUse,
            zero_platform_tokio::controlled_carrier::CarrierOrigin,
        ) -> Result<R, RuntimeError>,
    ) -> Result<R, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut,
        Fut: Future<Output = Result<S, RuntimeError>>,
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
            .get_or_create_conn(key, concurrency, |key, concurrency| async move {
                let (mut connection, original) = opening
                    .open(&profile, &binding, |raw| async move {
                        let io = wrap(raw).await?;
                        let io = key.establish_mux_outbound_stream(io).await?;
                        Ok(key.prepare_pool_conn(io, concurrency))
                    })
                    .await?;
                connection.original = Some(original);
                Ok::<_, RuntimeError>(connection)
            })
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
        key: VmessMuxPoolKey,
        concurrency: u32,
        target: Address,
        port: u16,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<zero_transport::pooled_driver::PooledStream, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut,
        Fut: Future<Output = Result<S, RuntimeError>>,
    {
        let (reserved, usage, origin) = self
            .native_connection(
                key,
                concurrency,
                binding,
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
        key: VmessMuxPoolKey,
        concurrency: u32,
        binding: UpstreamBinding,
        wrap: F,
    ) -> Result<zero_transport::pooled_driver::datagram::PooledDatagram<DatagramIo>, RuntimeError>
    where
        S: zero_traits::AsyncSocket + AsyncRead + AsyncWrite + Unpin + Send + 'static,
        F: FnOnce(zero_platform_tokio::controlled_carrier::CarrierIo) -> Fut,
        Fut: Future<Output = Result<S, RuntimeError>>,
    {
        self.native_connection(
            key,
            concurrency,
            binding,
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
                    },
                    usage,
                    origin,
                })
            },
        )
        .await
    }
}

/// Direct packet access to the existing protocol map; no stream flattening or upload task.
pub(crate) struct DatagramIo {
    connection: Arc<VmessMuxConn>,
    sid: u16,
    rx: mpsc::Receiver<VmessMuxDownlink>,
    writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    first: bool,
}
impl zero_transport::pooled_driver::datagram::PacketIo for DatagramIo {
    fn send(&mut self, frame: &zero_core::packet::Datagram) -> std::io::Result<()> {
        if self.connection.closed.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let payload = crate::udp::encode_udp_flow_packet(
            &frame.address.address,
            frame.address.port,
            &frame.payload,
        )
        .map_err(std::io::Error::other)?;
        let wire = if self.first {
            encode_open_stream_with_network(
                self.sid,
                &frame.address.address,
                frame.address.port,
                Network::Udp,
                &payload,
            )
        } else {
            encode_keep_stream(self.sid, &payload)
        }
        .map_err(std::io::Error::other)?;
        let streams = self.connection.streams.lock().unwrap();
        if !streams.contains_key(&self.sid) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.writer
            .as_mut()
            .expect("live packet writer")
            .send(wire)?;
        self.first = false;
        Ok(())
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> zero_transport::pooled_driver::datagram::PacketPoll {
        match std::task::ready!(self.rx.poll_recv(cx)) {
            Some(VmessMuxDownlink::Data(packet)) => Poll::Ready(
                crate::udp::decode_udp_flow_packet(&packet.into_inner())
                    .map(|packet| {
                        let (address, port, payload) = packet.into_parts();
                        Some((zero_core::request::Destination::new(address, port), payload))
                    })
                    .map_err(std::io::Error::other),
            ),
            Some(VmessMuxDownlink::Overflow) => Poll::Ready(Err(std::io::Error::other(
                "MUX UDP response capacity exceeded",
            ))),
            None => Poll::Ready(Ok(None)),
        }
    }
}
impl Drop for DatagramIo {
    fn drop(&mut self) {
        drop(self.writer.take());
        self.connection.release_stream(self.sid);
    }
}
