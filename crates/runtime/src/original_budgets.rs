//! Concrete original-provider accounting roots in the existing RuntimeHost.
//! No allocator, native controller, task registry or generic resource map.
#[derive(Default)]
pub struct HostOriginalBudgets {
    pub(crate) carrier_wake: std::sync::Arc<crate::component::instance::MonitorWake>,
    /// Exact original reservation/custody records, distinct from OS FD count.
    pub carrier: std::sync::Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    #[cfg(any(target_os = "linux", windows))]
    pub process: std::sync::Arc<zero_platform_tokio::owned_process::ProcessBudget>,
    #[cfg(feature = "raw-ip-runtime")]
    pub packet: std::sync::Arc<zero_tun::packet_memory::PacketReturnTotals>,
}
impl HostOriginalBudgets {
    pub(crate) fn limits_changed(
        current: &zero_config::RuntimeOptionsConfig,
        target: &zero_config::RuntimeOptionsConfig,
    ) -> bool {
        if current.effective_carrier_max_total_originals()
            != target.effective_carrier_max_total_originals()
            || current.effective_process_max_jobs() != target.effective_process_max_jobs()
            || current.effective_process_max_jobs_per_instance()
                != target.effective_process_max_jobs_per_instance()
        {
            return true;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if current.effective_packet_return_max_frames()
            != target.effective_packet_return_max_frames()
            || current.effective_packet_return_max_bytes()
                != target.effective_packet_return_max_bytes()
            || current.effective_packet_return_total_max_frames()
                != target.effective_packet_return_total_max_frames()
            || current.effective_packet_return_total_max_bytes()
                != target.effective_packet_return_total_max_bytes()
            || current.effective_packet_native_read_total_max_frames()
                != target.effective_packet_native_read_total_max_frames()
            || current.effective_packet_native_read_total_max_bytes()
                != target.effective_packet_native_read_total_max_bytes()
            || current.effective_packet_transmit_total_max_frames()
                != target.effective_packet_transmit_total_max_frames()
            || current.effective_packet_transmit_total_max_bytes()
                != target.effective_packet_transmit_total_max_bytes()
        {
            return true;
        }
        false
    }
    pub(crate) fn install_original_limits(
        &self,
        runtime: &zero_config::RuntimeOptionsConfig,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let limit = runtime.effective_carrier_max_total_originals();
        if !(1..=65536).contains(&limit) {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        self.carrier
            .install_after_retirement(
                std::num::NonZeroUsize::new(limit).expect("validated positive cap"),
            )
            .map_err(|source| {
                zero_platform_tokio::contract::ChannelError::new(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::Capacity,
                    zero_platform_tokio::contract::Impact::Component,
                )
                .with_source(source)
            })?;
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

impl HostOriginalBudgets {
    pub(crate) fn poll_carrier_originals(
        &self,
        target: &std::sync::Arc<crate::component::instance::WaitTarget>,
    ) {
        self.carrier_wake.bind(target);
        let waker = std::task::Waker::from(self.carrier_wake.clone());
        let mut cx = std::task::Context::from_waker(&waker);
        self.carrier.poll_cancelled_retirements(&mut cx);
        #[cfg(any(target_os = "linux", windows))]
        self.process.poll_released(&mut cx);
    }
}
