//! Local CPU topology facts used to keep carrier fingerprints stable per host.
//! No machine identifier or other persistent identity is collected.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CpuTopology {
    pub family: u32,
    pub model: u32,
    pub physical_cores: u32,
    pub logical_cores: u32,
    pub cache_line: u32,
}
#[cfg(target_arch = "aarch64")]
mod arm;
#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
mod x86;
pub fn cpu_topology() -> CpuTopology {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        x86::read()
    }
    #[cfg(target_arch = "aarch64")]
    {
        arm::read()
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64")))]
    {
        CpuTopology::default()
    }
}
