//! Native execution uses the same actual map and protocol wire driver.
use super::*;
use zero_platform_tokio::contract::network::UpstreamBinding;
use zero_transport::{
    pooled_driver::{PoolOptions, UserStream},
    RuntimeError,
};
impl VmessMuxConnectionPool {
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
            format!("{key:?}:{concurrency}").into(),
            endpoint,
            binding,
            limit,
            budget,
        )?;
        let entries = Arc::downgrade(&self.pool);
        Ok(options.on_confirmed_close(move |instance| {
            if let Some(entries) = entries.upgrade() {
                entries.lock().unwrap().retain(|_, connection| {
                    connection
                        .original
                        .as_ref()
                        .is_none_or(|original| original.instance() != instance)
                });
            }
        }))
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
        let owner = self.control.managed()?;
        let profile = format!("{key:?}:{concurrency}");
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
        let stream = connection.open_reserved_stream(sid, target, port, Network::Tcp);
        Ok(zero_transport::pooled_driver::PooledStream {
            origin: original.origin().map_err(std::io::Error::other)?,
            io: Box::pin(UserStream::new(stream, usage)),
        })
    }
}
