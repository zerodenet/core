use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settings {
    pub mtu: u32,
    pub tti_ms: u32,
    pub uplink_capacity_mib: u32,
    pub downlink_capacity_mib: u32,
    pub congestion: bool,
    pub write_buffer_bytes: u32,
}
impl Default for Settings {
    fn default() -> Self {
        Self {
            mtu: 1350,
            tti_ms: 50,
            uplink_capacity_mib: 5,
            downlink_capacity_mib: 20,
            congestion: false,
            write_buffer_bytes: 2 * 1024 * 1024,
        }
    }
}
impl Settings {
    pub fn validate(&self) -> io::Result<()> {
        if !(19..=65507).contains(&self.mtu)
            || !(10..=5000).contains(&self.tti_ms)
            || self.write_buffer_bytes > 64 * 1024 * 1024
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid mKCP MTU, TTI or write buffer",
            ));
        }
        Ok(())
    }
    pub(super) fn send_window(self) -> u32 {
        self.in_flight(self.uplink_capacity_mib)
    }
    pub(super) fn receive_window(self) -> u32 {
        self.in_flight(self.downlink_capacity_mib)
    }
    fn in_flight(self, capacity: u32) -> u32 {
        // Preserve the reference's integer rounding for sub-second TTI. Larger
        // intervals use the same rate calculation without its division by zero.
        let divisor = 1000 / self.tti_ms;
        let count = if divisor != 0 {
            u64::from(capacity) * 1024 * 1024 / u64::from(self.mtu) / u64::from(divisor)
        } else {
            u64::from(capacity) * 1024 * 1024 / u64::from(self.mtu) * u64::from(self.tti_ms) / 1000
        };
        count.clamp(8, 65536) as u32
    }
    pub(super) fn send_buffer(self) -> usize {
        (self.write_buffer_bytes / self.mtu) as usize + 1
    }
    pub(super) fn mss(self) -> usize {
        self.mtu as usize - 18
    }
}
