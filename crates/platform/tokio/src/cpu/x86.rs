//! CPUID topology interpretation follows klauspost/cpuid v2.3.0 (MIT, see LICENSE).
use super::CpuTopology;
#[cfg(target_arch = "x86")]
use std::arch::x86::__cpuid_count;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::__cpuid_count;
fn cpuid(leaf: u32, subleaf: u32) -> [u32; 4] {
    let value = __cpuid_count(leaf, subleaf);
    [value.eax, value.ebx, value.ecx, value.edx]
}
pub(super) fn read() -> CpuTopology {
    decode(cpuid)
}
fn decode(cpuid: impl Fn(u32, u32) -> [u32; 4]) -> CpuTopology {
    let [max, b, c, d] = cpuid(0, 0);
    if max < 1 {
        return CpuTopology::default();
    }
    let mut vendor = Vec::with_capacity(12);
    for register in [b, d, c] {
        vendor.extend_from_slice(&register.to_le_bytes());
    }
    let intel = vendor == b"GenuineIntel";
    let amd = vendor == b"AuthenticAMD" || vendor == b"AMDisbetter!";
    let hygon = vendor == b"HygonGenuine";
    let ext = cpuid(0x80000000, 0)[0];
    let [a, b, _, d] = cpuid(1, 0);
    let base_family = (a >> 8) & 15;
    let family = base_family
        + if base_family == 15 {
            (a >> 20) & 255
        } else {
            0
        };
    let model = ((a >> 4) & 15)
        + if base_family == 6 || base_family == 15 {
            (a >> 12) & 240
        } else {
            0
        };
    let logical = if intel && max >= 11 {
        cpuid(11, 1)[1] & 65535
    } else if intel || amd || hygon {
        (b >> 16) & 255
    } else {
        0
    };
    let threads = if max < 4 || !(intel || amd) {
        1
    } else if max < 11 {
        if intel && d & (1 << 28) != 0 && (b >> 16) & 255 > 1 {
            ((b >> 16) & 255) / ((cpuid(4, 0)[0] >> 26) + 1)
        } else {
            1
        }
    } else {
        let threads = cpuid(11, 0)[1] & 65535;
        if threads != 0 {
            threads
        } else if amd && d & (1 << 28) != 0 && family >= 23 {
            if ext >= 0x8000001e {
                ((cpuid(0x8000001e, 0)[1] >> 8) & 255) + 1
            } else {
                2
            }
        } else {
            1
        }
    };
    let physical = if logical > 0 && threads > 0 {
        logical / threads
    } else if (amd || hygon) && ext >= 0x80000008 && cpuid(0x80000008, 0)[2] & 255 > 0 {
        (cpuid(0x80000008, 0)[2] & 255) + 1
    } else {
        0
    };
    let mut cache = (b & 0xff00) >> 5;
    if cache == 0 && ext >= 0x80000006 {
        cache = cpuid(0x80000006, 0)[2] & 255;
    }
    CpuTopology {
        family,
        model,
        logical_cores: logical,
        physical_cores: physical,
        cache_line: cache,
    }
}

#[cfg(test)]
#[path = "../../tests/cpu/x86.rs"]
mod tests;
