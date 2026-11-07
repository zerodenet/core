//! Concrete Host policy observation and explicit standalone library policy.
use super::*;
impl PacketReturns {
    pub(crate) fn with_limits(
        mut self,
        options: &crate::runtime::raw_ip::RawIpStackOptions,
    ) -> std::io::Result<Self> {
        // Explicit standalone library construction owns this Root policy.
        use zero_tun::packet_memory::PacketMemoryLimit;
        options
            .packet_return_totals
            .install_limits(
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
            )
            .map_err(std::io::Error::other)?;
        self.budget = budget::ReturnBudget::with_totals(
            options.packet_return_max_frames,
            options.packet_return_max_bytes,
            options.packet_return_totals.clone(),
        );
        Ok(self)
    }
    pub(crate) fn with_host_limits(
        mut self,
        options: &crate::runtime::raw_ip::RawIpStackOptions,
    ) -> std::io::Result<Self> {
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
        if expected != actual {
            return Err(std::io::Error::other(
                "Host has not installed this accepted packet memory policy",
            ));
        }
        self.budget = budget::ReturnBudget::with_totals(
            options.packet_return_max_frames,
            options.packet_return_max_bytes,
            options.packet_return_totals.clone(),
        );
        Ok(self)
    }
}
