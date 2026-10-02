use super::*;
const HEADER: &str = "Inter-| Receive | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n";
#[test]
fn procfs_preserves_direction_and_merged_rx_drop_basis() {
    let source = format!("{HEADER} eth0: 100 2 3 4 5 6 7 8 900 10 11 12 13 14 15 16\n");
    let samples = parse(&source, |_| Ok(7)).unwrap();
    assert_eq!(samples[0].index, 7);
    assert_eq!((samples[0].rx_bytes, samples[0].tx_bytes), (100, 900));
    assert_eq!(
        (samples[0].rx_dropped_packets, samples[0].tx_dropped_packets),
        (Some(4), Some(12))
    );
    assert!(samples[0].accounting_basis.contains("includes_missed"));
}
#[test]
fn malformed_or_partial_inventory_is_rejected_not_silently_zeroed() {
    assert!(parse("", |_| Ok(1)).is_err());
    assert!(parse(&format!("{HEADER} eth0: 1 2 3"), |_| Ok(1)).is_err());
    assert!(parse(&format!("{HEADER} eth0: {}", "0 ".repeat(16)), |_| Ok(0)).is_err());
    let row = format!(" eth0: {}\n", "0 ".repeat(16));
    assert!(parse(&format!("{HEADER}{row}{row}"), |_| Ok(1)).is_err());
    let oversized = (0..257)
        .map(|i| format!(" eth{i}: {}\n", "0 ".repeat(16)))
        .collect::<String>();
    assert!(parse(&format!("{HEADER}{oversized}"), |_| Ok(1)).is_err());
}
