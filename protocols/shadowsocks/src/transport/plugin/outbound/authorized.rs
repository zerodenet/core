//! The real Host installs the same job before its actual actor can await.
use super::*;
impl PluginPlan {
    #[expect(
        clippy::too_many_arguments,
        reason = "preserve explicit protocol facts and independently owned caller, wire and resource bindings at this assembly boundary"
    )]
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
        #[cfg(any(unix, windows))]
        if let Some(ordinal) = endpoint_ordinal {
            return self
                .acquire_endpoint(tcp, resources, claimed, tasks, actor, ordinal, deadline)
                .await;
        }
        #[cfg(not(any(unix, windows)))]
        let _ = endpoint_ordinal;
        self.acquire_with(
            tcp,
            tokio::net::TcpStream::connect,
            move |request| {
                #[cfg(any(unix, windows))]
                {
                    let original = resources
                        .prepare_owned_process(request, &tasks)
                        .map_err(io::Error::other)?;
                    claimed.store(true, std::sync::atomic::Ordering::Release);
                    original.start(actor)?;
                    Ok(PluginChild::Owned(original))
                }
                #[cfg(not(any(unix, windows)))]
                {
                    let _ = (&tasks, &actor, deadline);
                    resources
                        .spawn_process(request)
                        .inspect(|_| {
                            claimed.store(true, std::sync::atomic::Ordering::Release);
                        })
                        .map(|c| PluginChild::Library(Box::new(c)))
                        .map_err(io::Error::other)
                }
            },
            true,
            Some(deadline),
        )
        .await
    }
}

#[cfg(any(unix, windows))]
pub(in crate::transport::plugin) mod endpoint;
