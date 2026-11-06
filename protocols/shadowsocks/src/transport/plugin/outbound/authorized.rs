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
        deadline: tokio::time::Instant,
    ) -> io::Result<Option<Arc<PluginLease>>> {
        self.acquire_with(
            tcp,
            tokio::net::TcpStream::connect,
            move |request| {
                #[cfg(target_os = "linux")]
                {
                    let original = resources
                        .prepare_owned_process(request, &tasks)
                        .map_err(io::Error::other)?;
                    claimed.store(true, std::sync::atomic::Ordering::Release);
                    original.start(actor)?;
                    Ok(PluginChild::Owned(original))
                }
                #[cfg(not(target_os = "linux"))]
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
