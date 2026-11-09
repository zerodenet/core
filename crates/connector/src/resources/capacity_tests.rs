use super::*;
fn capped(originals: usize) -> DispatcherResources {
    let mut api = zero_config::ApiConfig::default();
    api.dispatcher.max_open_originals = Some(originals);
    DispatcherResources::with_capacity(Arc::new(DispatcherCapacity::from_api(&api)))
}
#[test]
fn actual_file_view_and_native_close_hold_the_one_original_position() {
    let resources = capped(1);
    let file = resources.open(tempfile::tempfile).unwrap();
    let identity = Arc::downgrade(&file.original);
    assert_eq!(
        resources
            .open(|| panic!("quota must refuse before native acquisition"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(!resources.retirement_complete());
    drop(file);
    assert!(
        identity.upgrade().is_some(),
        "returned view is not a native close"
    );
    let next = resources.open(tempfile::tempfile).unwrap();
    assert!(
        identity.upgrade().is_none(),
        "the same old controller was positively closed first"
    );
    drop(next);
    resources.close().unwrap();
    assert!(resources.retirement_complete());
}
#[test]
fn pending_acquisition_survives_opener_unwind_without_refund_or_replacement() {
    let resources = capped(1);
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = resources.open(|| panic!("original concrete opener did not return"));
    }))
    .is_err());
    assert!(!resources.retirement_complete());
    assert_eq!(
        resources
            .open(|| panic!("unknown original forbids another opener"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(resources.close().is_err());
    assert!(!resources.retirement_complete());
}
#[test]
fn original_database_reserves_before_directory_and_keeps_returned_views_charged() {
    let resources = capped(1);
    let database = resources.database(tempfile::tempdir).unwrap();
    assert!(resources
        .database(|| panic!("quota must refuse before scratch creation"))
        .is_err());
    assert!(!resources.retirement_complete());
    drop(database);
    let next = resources.database(tempfile::tempdir).unwrap();
    drop(next);
    resources.close().unwrap();
    assert!(resources.retirement_complete());
}
