//! Actual read-only File write failure; no synthetic close/task receipt.
use super::*;
#[test]
fn actual_os_write_fault_is_retained_separately_from_positive_close_and_fresh_generation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal");
    let owner = JournalOwner::standalone(spec(path.clone(), 64)).unwrap();
    owner.start().unwrap();
    let grant = owner.grant();
    grant
        .replace(6, |write| write.write_all(b"valid\n"))
        .unwrap();
    // Fault injection at the concrete native-file boundary: this exact original
    // is a real regular readonly File, so its attempted write really fails in OS.
    // Adopt immediately, without a duplicate/raw handle or fabricated receipt.
    owner
        .0
        .lock()
        .append
        .install(std::fs::File::open(&path).unwrap());
    let failed = grant.append(1, |write| write.write_all(b"x")).unwrap_err();
    let returned = failed
        .get_ref()
        .unwrap()
        .downcast_ref::<JournalFailure>()
        .unwrap();
    let work = std::error::Error::source(returned.work().unwrap().get_ref().unwrap())
        .unwrap()
        .downcast_ref::<io::Error>()
        .unwrap();
    let original = owner.failure().unwrap();
    let fault = std::error::Error::source(original.as_ref())
        .unwrap()
        .downcast_ref::<JournalFaults>()
        .unwrap();
    assert!(std::ptr::eq(fault.first().as_ref(), work));
    assert_eq!(fault.sources().iter().flatten().count(), 1);
    assert!(Arc::ptr_eq(&original, &owner.failure().unwrap()));
    assert!(returned.cleanup().is_none());
    assert!(!grant.native_closed());
    assert!(
        grant.replace(0, |_| Ok(())).is_err(),
        "new IO cannot bypass the failed original generation"
    );
    let closed = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&closed));
    assert!(grant.native_closed());
    assert!(
        Arc::ptr_eq(&original, &owner.failure().unwrap()),
        "physical closure does not discard accepted typed fault"
    );
    let next = JournalOwner::standalone(spec(path, 64)).unwrap();
    next.start().unwrap();
    next.grant()
        .append(5, |write| write.write_all(b"next\n"))
        .unwrap();
    let state = next
        .grant()
        .read_snapshot(|read| {
            let mut bytes = Vec::new();
            read.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap()
        .unwrap();
    assert_eq!(state, b"valid\nnext\n");
    assert!(next.matches_closed(&next.try_close().unwrap().unwrap()));
}

#[test]
fn later_actual_temporary_cleanup_fault_remains_in_same_accepted_error_ledger() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal");
    let owner = JournalOwner::standalone(spec(path.clone(), 64)).unwrap();
    owner.start().unwrap();
    owner
        .grant()
        .replace(2, |write| write.write_all(b"ok"))
        .unwrap();
    owner
        .0
        .lock()
        .append
        .install(std::fs::File::open(&path).unwrap());
    assert!(owner
        .grant()
        .append(1, |write| write.write_all(b"x"))
        .is_err());
    let original = owner.failure().unwrap();
    let record = std::error::Error::source(original.as_ref())
        .unwrap()
        .downcast_ref::<JournalFaults>()
        .unwrap();
    assert_eq!(record.sources().iter().flatten().count(), 1);
    let temp = tempfile::NamedTempFile::new_in(directory.path()).unwrap();
    let (file, mut temp_path) = temp.into_parts();
    temp_path.disable_cleanup(true);
    let name = temp_path.to_path_buf();
    std::fs::remove_file(&name).unwrap();
    std::fs::create_dir(&name).unwrap();
    {
        let mut state = owner.0.lock();
        state.temporary.install(file);
        state.temporary_path = Some(temp_path);
    }
    assert!(
        owner.try_close().is_err(),
        "actual remove_file(directory) fails in OS"
    );
    assert!(
        !owner.grant().native_closed(),
        "unknown cleanup is not positive close"
    );
    assert!(Arc::ptr_eq(&original, &owner.failure().unwrap()));
    assert_eq!(record.sources().iter().flatten().count(), 2);
    let late = record.sources()[5].clone().unwrap();
    std::fs::remove_dir(&name).unwrap();
    std::fs::write(&name, b"").unwrap();
    let closed = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&closed));
    assert!(Arc::ptr_eq(&late, record.sources()[5].as_ref().unwrap()));
    assert!(Arc::ptr_eq(&original, &owner.failure().unwrap()));
}
