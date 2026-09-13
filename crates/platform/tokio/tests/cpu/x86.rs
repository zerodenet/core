use super::*;
fn vendor(max: u32, name: &[u8; 12]) -> [u32; 4] {
    [
        max,
        u32::from_le_bytes(name[..4].try_into().unwrap()),
        u32::from_le_bytes(name[8..].try_into().unwrap()),
        u32::from_le_bytes(name[4..8].try_into().unwrap()),
    ]
}
#[test]
fn intel_topology_uses_package_counts_and_extended_model() {
    let cpu = decode(|leaf, sub| match (leaf, sub) {
        (0, _) => vendor(11, b"GenuineIntel"),
        (1, _) => [0x906e9, (8 << 16) | (8 << 8), 0, 1 << 28],
        (11, 0) => [0, 2, 0, 0],
        (11, 1) => [0, 8, 0, 0],
        _ => [0; 4],
    });
    assert_eq!(
        cpu,
        CpuTopology {
            family: 6,
            model: 158,
            physical_cores: 4,
            logical_cores: 8,
            cache_line: 64
        }
    );
}
#[test]
fn amd_extended_topology_and_cache_fallback_match_reference() {
    let cpu = decode(|leaf, _| match leaf {
        0 => vendor(11, b"AuthenticAMD"),
        1 => [0x800f10, 16 << 16, 0, 1 << 28],
        0x80000000 => [0x8000001e, 0, 0, 0],
        0x8000001e => [0, 1 << 8, 0, 0],
        0x80000006 => [0, 0, 64, 0],
        _ => [0; 4],
    });
    assert_eq!(
        cpu,
        CpuTopology {
            family: 23,
            model: 1,
            physical_cores: 8,
            logical_cores: 16,
            cache_line: 64
        }
    );
    assert_eq!(decode(|_, _| [0; 4]), CpuTopology::default());
}
