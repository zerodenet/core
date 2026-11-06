//! The real Host installs the same job before its actual actor can await.
use super::*;
impl PluginPlan {
    pub(in crate::transport::plugin) async fn acquire_authorized(
        &self,
        tcp: bool,
        resources: Arc<dyn zero_platform_tokio::contract::component::ResourceAccess>,
        claimed: Arc<std::sync::atomic::AtomicBool>,
        tasks: zero_platform_tokio::contract::scope::TaskRegistrar,
        actor: zero_platform_tokio::contract::scope::TaskJoinWitness,
        endpoint_ordinal: Option<usize>,
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<Arc<PluginLease>>> {
        #[cfg(any(target_os = "linux", windows))]
        if let Some(ordinal) = endpoint_ordinal {
            return self
                .acquire_endpoint(tcp, resources, claimed, tasks, actor, ordinal, deadline)
                .await;
        }
        #[cfg(not(any(target_os = "linux", windows)))]
        let _ = endpoint_ordinal;
        self.acquire_with(
            tcp,
            tokio::net::TcpStream::connect,
            move |request| {
                #[cfg(any(target_os = "linux", windows))]
                {
                    let original = resources
                        .prepare_owned_process(request, &tasks)
                        .map_err(io::Error::other)?;
                    claimed.store(true, std::sync::atomic::Ordering::Release);
                    original.start(actor)?;
                    Ok(PluginChild::Owned(original))
                }
                #[cfg(not(any(target_os = "linux", windows)))]
                {
                    let _ = (&tasks, &actor, deadline);
                    resources
                        .spawn_process(request)
                        .inspect(|_| {
                            claimed.store(true, std::sync::atomic::Ordering::Release);
                        })
                        .map(PluginChild::Library)
                        .map_err(io::Error::other)
                }
            },
            true,
            Some(deadline),
        )
        .await
    }
}

#[cfg(any(target_os = "linux", windows))]
pub(in crate::transport::plugin) mod endpoint;
