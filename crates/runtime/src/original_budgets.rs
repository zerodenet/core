//! Concrete original-provider accounting roots in the existing RuntimeHost.
//! No allocator, native controller, task registry or generic resource map.
#[derive(Default)]
pub struct HostOriginalBudgets {
    #[cfg(any(target_os = "linux", windows))]
    pub process: std::sync::Arc<zero_platform_tokio::owned_process::ProcessBudget>,
    #[cfg(feature = "raw-ip-runtime")]
    pub packet: std::sync::Arc<zero_tun::packet_memory::PacketReturnTotals>,
}
impl HostOriginalBudgets {
    pub(crate) fn install_process_limits(
        &self,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        #[cfg(any(target_os = "linux", windows))]
        self.process
            .install(zero_platform_tokio::owned_process::ProcessBudgetLimits {
                max_jobs: runtime.effective_process_max_jobs(),
                max_jobs_per_instance: runtime.effective_process_max_jobs_per_instance(),
            })
            .map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Capacity,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
        #[cfg(not(any(target_os = "linux", windows)))]
        let _ = runtime;
        Ok(())
    }
}
