use super::routes_present;
use crate::route::windows::route_row;

#[test]
fn route_readback_distinguishes_missing_healthy_and_conflicting_rows() {
    let gateway = "192.0.2.1".parse().unwrap();
    assert!(!routes_present(&[], 7, "1.1.1.1/32", gateway).unwrap());
    let mut row = route_row(7, "1.1.1.1/32", gateway).unwrap();
    assert!(!row.Loopback);
    assert!(routes_present(&[row], 7, "1.1.1.1/32", gateway).unwrap());
    assert!(routes_present(&[row], 7, "1.1.1.1/32", "192.0.2.2".parse().unwrap()).is_err());
    row.ValidLifetime = 0;
    assert!(routes_present(&[row], 7, "1.1.1.1/32", gateway).is_err());
    row.ValidLifetime = u32::MAX;
    row.Loopback = true;
    assert!(routes_present(&[row], 7, "1.1.1.1/32", gateway).is_err());
}
