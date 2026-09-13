use super::CpuTopology;
pub(super) fn read() -> CpuTopology {
    #[allow(unused_mut)]
    let mut count = std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1);
    #[cfg(target_os = "linux")]
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        if let Some(list) = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        {
            let parsed: Option<u32> = list.trim().split(',').try_fold(0u32, |sum, range| {
                let (lo, hi) = range.split_once('-').unwrap_or((range, range));
                let lo = lo.parse::<u32>().ok()?;
                let hi = hi.parse::<u32>().ok()?;
                sum.checked_add(hi.checked_sub(lo)?.checked_add(1)?)
            });
            if let Some(value) = parsed.filter(|n| *n > 0) {
                count = value;
            }
        }
    }
    #[allow(unused_mut)]
    let mut result = CpuTopology {
        logical_cores: count,
        physical_cores: count,
        cache_line: 64,
        ..Default::default()
    };
    #[cfg(target_os = "macos")]
    {
        fn value(names: &[&std::ffi::CStr], fallback: u32) -> u32 {
            for name in names {
                let mut data = 0u64;
                let mut size = std::mem::size_of_val(&data);
                // SAFETY: the name is NUL-terminated; data/size are writable and
                // the sysctl query supplies no input buffer or mutation.
                let ok = unsafe {
                    libc::sysctlbyname(
                        name.as_ptr(),
                        (&mut data as *mut u64).cast(),
                        &mut size,
                        std::ptr::null_mut(),
                        0,
                    )
                };
                if ok == 0 && size <= 8 && data != 0 {
                    return data as u32;
                }
            }
            fallback
        }
        result.family = value(&[c"machdep.cpu.family", c"hw.cpufamily"], 0);
        result.model = value(&[c"machdep.cpu.model"], 0);
        result.logical_cores = value(&[c"machdep.cpu.core_count"], count);
        result.physical_cores = value(&[c"hw.physicalcpu"], count);
        result.cache_line = value(&[c"hw.cachelinesize"], 0);
    }
    #[cfg(target_os = "linux")]
    if let Ok(midr) =
        std::fs::read_to_string("/sys/devices/system/cpu/cpu0/regs/identification/midr_el1")
    {
        if let Ok(midr) = u64::from_str_radix(midr.trim().trim_start_matches("0x"), 16) {
            result.family = (midr >> 16) as u32 & 255;
            result.model = midr as u32 & 65535;
        }
    }
    result
}
