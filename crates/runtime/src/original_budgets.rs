//! Concrete original-provider accounting roots in the existing RuntimeHost.
//! No allocator, native controller, task registry or generic resource map.
use std::sync::Arc;
pub struct HostOriginalBudgets {
    pub(crate) carrier_wake: std::sync::Arc<crate::component::instance::MonitorWake>,
    /// Exact original reservation/custody records, distinct from OS FD count.
    pub carrier: std::sync::Arc<zero_platform_tokio::controlled_carrier::CarrierOriginalTotals>,
    /// Actual component/inbound task registrations through their real ACK.
    pub component_tasks:
        std::sync::Arc<zero_platform_tokio::contract::scope::TaskRegistrationBudget>,
    #[cfg(any(target_os = "linux", windows))]
    pub process: std::sync::Arc<zero_platform_tokio::owned_process::ProcessBudget>,
    /// Same pure memory Root even when native packet IO is not compiled.
    pub packet: std::sync::Arc<zero_tun::packet_memory::PacketReturnTotals>,
}
impl Default for HostOriginalBudgets {
    fn default() -> Self {
        Self::with_packet_root(Arc::default())
    }
}
impl HostOriginalBudgets {
    /// Host assembly supplies the already-owned Devices Root when present.
    /// This does not allocate and then discard a second temporary packet Root.
    pub fn with_packet_root(packet: Arc<zero_tun::packet_memory::PacketReturnTotals>) -> Self {
        Self {
            carrier_wake: Arc::default(),
            carrier: Arc::default(),
            component_tasks: Arc::default(),
            #[cfg(any(target_os = "linux", windows))]
            process: Arc::default(),
            packet,
        }
    }
    pub(crate) fn limits_changed(
        current: &zero_config::RuntimeOptionsConfig,
        target: &zero_config::RuntimeOptionsConfig,
    ) -> bool {
        if current.effective_carrier_max_total_originals()
            != target.effective_carrier_max_total_originals()
            || current.effective_component_total_max_tasks()
                != target.effective_component_total_max_tasks()
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
        if current.effective_packet_driver_metadata_total_max_allocations()
            != target.effective_packet_driver_metadata_total_max_allocations()
            || current.effective_packet_driver_metadata_total_max_bytes()
                != target.effective_packet_driver_metadata_total_max_bytes()
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
        #[cfg(all(feature = "raw-ip-runtime", windows))]
        if current.effective_packet_windows_ring_capacity()
            != target.effective_packet_windows_ring_capacity()
            || current.effective_packet_windows_ring_total_max_bytes()
                != target.effective_packet_windows_ring_total_max_bytes()
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
        let component_tasks = runtime.effective_component_total_max_tasks();
        self.component_tasks
            .check_limit(component_tasks)
            .map_err(prepare_error)?;
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
        let metadata_limit = zero_tun::packet_memory::PacketMetadataLayout::new(
            runtime.effective_packet_driver_metadata_total_max_allocations(),
            runtime.effective_packet_driver_metadata_total_max_bytes(),
        );
        #[cfg(all(feature = "raw-ip-runtime", windows))]
        let rings = Some(zero_tun::packet_memory::RequestedRingLimits {
            capacity: runtime.effective_packet_windows_ring_capacity(),
            total_requested_bytes: runtime.effective_packet_windows_ring_total_max_bytes(),
        });
        #[cfg(not(all(feature = "raw-ip-runtime", windows)))]
        let rings = None;
        self.packet
            .check_all_original_limits(
                packet_limits[0],
                packet_limits[1],
                packet_limits[2],
                metadata_limit,
                rings,
            )
            .map_err(|source| prepare_error(std::io::Error::other(source)))?;
        // Auxiliary workers use this same book without source admission.
        // Its atomic checked install must run before changing any of the
        // already admission-fenced original-resource policies below.
        self.component_tasks
            .install_limit(component_tasks)
            .map_err(prepare_error)?;
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
        self.packet
            .install_all_original_limits(
                packet_limits[0],
                packet_limits[1],
                packet_limits[2],
                metadata_limit,
                rings,
            )
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

#[cfg(test)]
mod tests;
