//! Concrete Host policy observation and explicit standalone library policy.
use super::*;
impl PacketReturns {
    pub(crate) fn new_for_options(
        options: &crate::runtime::raw_ip::RawIpStackOptions,
        hosted: bool,
        drop_observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> std::io::Result<Self> {
        if hosted {
            Self::check_host_policy(options)?;
        } else {
            Self::install_library_policy(options)?;
        }
        let rows = [
            options.packet_return_max_routes,
            options.packet_return_max_conversations,
            options.packet_return_max_observations,
        ];
        if rows.into_iter().any(|value| !(1..=65536).contains(&value)) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid actual return row bounds",
            ));
        }
        Ok(Self {
            budget: budget::ReturnBudget::try_with_totals(
                options.packet_return_max_frames,
                options.packet_return_max_bytes,
                options.packet_return_totals.clone(),
            )?,
            row_limits: ReturnRowLimits {
                routes: rows[0],
                conversations: rows[1],
                observations: rows[2],
            },
            drop_observer,
            receive_incomplete: Default::default(),
            routes: Default::default(),
            observations: Default::default(),
            conversations: Default::default(),
            translated: Default::default(),
        })
    }
    fn install_library_policy(
        options: &crate::runtime::raw_ip::RawIpStackOptions,
    ) -> std::io::Result<()> {
        // Explicit standalone library construction owns this Root policy.
        use zero_tun::packet_memory::PacketMemoryLimit;
        options
            .packet_return_totals
            .install_original_limits(
                PacketMemoryLimit {
                    frames: options.packet_return_total_max_frames,
                    bytes: options.packet_return_total_max_bytes,
                },
                PacketMemoryLimit {
                    frames: options.packet_native_read_total_max_frames,
                    bytes: options.packet_native_read_total_max_bytes,
                },
                PacketMemoryLimit {
                    frames: options.packet_transmit_total_max_frames,
                    bytes: options.packet_transmit_total_max_bytes,
                },
                zero_tun::packet_memory::PacketMetadataLayout::new(
                    options.packet_driver_metadata_total_max_allocations,
                    options.packet_driver_metadata_total_max_bytes,
                ),
            )
            .map_err(std::io::Error::other)?;
        Ok(())
    }
    fn check_host_policy(
        options: &crate::runtime::raw_ip::RawIpStackOptions,
    ) -> std::io::Result<()> {
        use zero_tun::packet_memory::PacketMemoryLimit;
        let expected = [
            PacketMemoryLimit {
                frames: options.packet_return_total_max_frames,
                bytes: options.packet_return_total_max_bytes,
            },
            PacketMemoryLimit {
                frames: options.packet_native_read_total_max_frames,
                bytes: options.packet_native_read_total_max_bytes,
            },
            PacketMemoryLimit {
                frames: options.packet_transmit_total_max_frames,
                bytes: options.packet_transmit_total_max_bytes,
            },
        ];
        let actual = [
            options.packet_return_totals.returns().limit,
            options.packet_return_totals.native().limit,
            options.packet_return_totals.transmit().limit,
        ];
        let metadata = zero_tun::packet_memory::PacketMetadataLayout::new(
            options.packet_driver_metadata_total_max_allocations,
            options.packet_driver_metadata_total_max_bytes,
        );
        if expected != actual || options.packet_return_totals.driver_metadata().limit != metadata {
            return Err(std::io::Error::other(
                "Host has not installed this accepted packet memory policy",
            ));
        }
        Ok(())
    }
}
