use super::*;
use std::{
    net::{IpAddr, Ipv4Addr},
    num::NonZeroUsize,
};
#[path = "../../tests/controlled_icmp/echo.rs"]
mod echo;
#[path = "../../tests/controlled_icmp/live.rs"]
mod live;
static LIVE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
fn owner(limit: usize) -> ControlledIcmpOwner {
    ControlledIcmpOwner::new(NonZeroUsize::new(limit).unwrap(), CancellationToken::new())
}
fn local() -> IpAddr {
    Ipv4Addr::UNSPECIFIED.into()
}
#[test]
fn pure_reservations_are_bounded_and_unused_retirement_is_not_native_close() {
    let mut owner = owner(1);
    let entry = owner.reserve(false).unwrap();
    assert!(matches!(
        owner.reserve(false).err().unwrap().outcome,
        IcmpAcquisitionOutcome::KnownNoNative
    ));
    assert_eq!(owner.occupied(), 1);
    let report = owner.close_all();
    assert!(report.is_complete());
    assert_eq!(report.known_no_native, 1);
    assert_eq!(report.confirmed_closed, 0);
    assert!(matches!(*entry.native(), Native::NoNative));
    assert!(owner.reserve(false).is_err());
}
#[test]
fn checked_identity_never_wraps_and_pre_create_unwind_stays_unknown() {
    let mut owner = owner(1);
    owner.next = u64::MAX;
    assert!(owner.reserve(false).is_err());
    assert_eq!(owner.occupied(), 0);
    owner.next = 0;
    let entry = owner.reserve(false).unwrap();
    // Model only the explicit pre-native sentinel, never fake a native close.
    *entry.native() = Native::Unknown(entry.uncertain.clone());
    let first = owner.take_report().unconfirmed.remove(0).source;
    assert!(matches!(
        owner.reserve(false).err().unwrap().outcome,
        IcmpAcquisitionOutcome::Unknown(_)
    ));
    for _ in 0..2 {
        let report = owner.close_all();
        assert_eq!(report.confirmed_closed, 0);
        assert!(!report.is_complete());
        assert!(Arc::ptr_eq(&first, &report.unconfirmed[0].source));
    }
}
