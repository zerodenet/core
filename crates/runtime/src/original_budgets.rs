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
            || current.effective_navigation_max_jobs() != target.effective_navigation_max_jobs()
            || current.effective_navigation_max_requests()
                != target.effective_navigation_max_requests()
            || current.effective_quic_max_queue_packets()
                != target.effective_quic_max_queue_packets()
            || current.effective_quic_total_max_buffered_bytes()
                != target.effective_quic_total_max_buffered_bytes()
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
        // The owning transition retains every actual source/caller admission
        // fence here. Check all concrete lanes before changing any policy;
        // pending/failed/retiring originals remain in their existing roots.
        let prepare_error = |source: std::io::Error| {
            zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Capacity,
                zero_platform_tokio::contract::Impact::Component,
            )
            .with_source(source)
        };
        let preparation = [
            std::num::NonZeroUsize::new(runtime.effective_navigation_max_jobs()),
            std::num::NonZeroUsize::new(runtime.effective_navigation_max_requests()),
        ];
        if !(1..=64).contains(&runtime.effective_navigation_max_jobs())
            || !(1..=32).contains(&runtime.effective_navigation_max_requests())
        {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let preparation =
            preparation.map(|value| value.expect("validated positive preparation cap"));
        let buffers = zero_platform_tokio::controlled_carrier::CarrierBufferLimits {
            max_queue_packets: runtime.effective_quic_max_queue_packets(),
            total_max_bytes: runtime.effective_quic_total_max_buffered_bytes(),
        };
        self.carrier
            .check_buffer_limits_after_retirement(buffers)
            .map_err(prepare_error)?;
        self.carrier
            .check_preparation_limits_after_retirement(preparation[0], preparation[1])
            .map_err(prepare_error)?;
        self.carrier
            .check_install_after_retirement(
                std::num::NonZeroUsize::new(limit).expect("validated positive cap"),
            )
            .map_err(prepare_error)?;
        #[cfg(any(target_os = "linux", windows))]
        self.process
            .check_install(zero_platform_tokio::owned_process::ProcessBudgetLimits {
                max_jobs: runtime.effective_process_max_jobs(),
                max_jobs_per_instance: runtime.effective_process_max_jobs_per_instance(),
            })
            .map_err(prepare_error)?;
        #[cfg(feature = "raw-ip-runtime")]
        let packet_limits = [
            zero_tun::packet_memory::PacketMemoryLimit {
                frames: runtime.effective_packet_return_total_max_frames(),
                bytes: runtime.effective_packet_return_total_max_bytes(),
            },
            zero_tun::packet_memory::PacketMemoryLimit {
                frames: runtime.effective_packet_native_read_total_max_frames(),
                bytes: runtime.effective_packet_native_read_total_max_bytes(),
            },
            zero_tun::packet_memory::PacketMemoryLimit {
                frames: runtime.effective_packet_transmit_total_max_frames(),
                bytes: runtime.effective_packet_transmit_total_max_bytes(),
            },
        ];
        #[cfg(feature = "raw-ip-runtime")]
        self.packet
            .check_limits(packet_limits[0], packet_limits[1], packet_limits[2])
            .map_err(|source| prepare_error(std::io::Error::other(source)))?;
        self.carrier
            .install_preparation_limits_after_retirement(preparation[0], preparation[1])
            .map_err(prepare_error)?;
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
        self.carrier
            .install_buffer_limits_after_retirement(buffers)
            .map_err(prepare_error)?;
        #[cfg(feature = "raw-ip-runtime")]
        self.packet
            .install_limits(packet_limits[0], packet_limits[1], packet_limits[2])
            .map_err(|source| prepare_error(std::io::Error::other(source)))?;
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
