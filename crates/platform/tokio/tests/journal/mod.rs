use super::*;
mod hosted;
fn spec(path: PathBuf, max: usize) -> JournalSpec {
    JournalSpec {
        path,
        max_bytes: NonZeroUsize::new(max).unwrap(),
        parent_private: false,
    }
}
#[test]
fn actual_restart_envelope_rejects_growth_before_encoding_and_preserves_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let owner = JournalOwner::standalone(spec(dir.path().join("state"), 8)).unwrap();
    owner.start().unwrap();
    let journal = owner.grant();
    journal
        .replace(4, |write| write.write_all(b"one\n"))
        .unwrap();
    journal
        .append(4, |write| write.write_all(b"two\n"))
        .unwrap();
    assert_eq!(journal.current_bytes().unwrap(), 8);
    let mut encoded = false;
    let error = journal
        .append(1, |_| {
            encoded = true;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::FileTooLarge);
    assert!(
        !encoded,
        "no large work is materialized before its exact envelope fits"
    );
    let bytes = journal
        .read_snapshot(|read| {
            let mut bytes = Vec::new();
            read.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap()
        .unwrap();
    assert_eq!(bytes, b"one\ntwo\n");
    // A fitting full checkpoint allows progress without evicting domain state.
    journal
        .replace(5, |write| write.write_all(b"next\n"))
        .unwrap();
    let closed = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&closed));
    assert!(journal.append(0, |_| Ok(())).is_err());
    let restored = JournalOwner::standalone(spec(dir.path().join("state"), 8)).unwrap();
    restored.start().unwrap();
    let bytes = restored
        .grant()
        .read_snapshot(|read| {
            let mut bytes = Vec::new();
            read.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap()
        .unwrap();
    assert_eq!(bytes, b"next\n");
    let other = restored.try_close().unwrap().unwrap();
    assert!(!owner.matches_closed(&other));
    assert!(restored.matches_closed(&other));
}
#[test]
fn partial_append_and_panicking_encoder_retain_same_original_until_positive_close() {
    let dir = tempfile::tempdir().unwrap();
    let owner = JournalOwner::standalone(spec(dir.path().join("state"), 32)).unwrap();
    owner.start().unwrap();
    let journal = owner.grant();
    journal
        .replace(4, |write| write.write_all(b"old\n"))
        .unwrap();
    let expected = Arc::new(io::Error::other("actual encoder work failed"));
    #[derive(Debug)]
    struct Work(Arc<io::Error>);
    impl std::fmt::Display for Work {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }
    impl std::error::Error for Work {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }
    let error = journal
        .append(4, |write| {
            write.write_all(b"pa")?;
            Err::<(), _>(io::Error::other(Work(expected.clone())))
        })
        .unwrap_err();
    let retained = error
        .get_ref()
        .unwrap()
        .downcast_ref::<JournalFailure>()
        .unwrap();
    let work = retained
        .work()
        .unwrap()
        .get_ref()
        .unwrap()
        .downcast_ref::<Work>()
        .unwrap();
    assert!(Arc::ptr_eq(&work.0, &expected));
    assert!(retained.cleanup().is_none());
    assert!(journal.append(1, |write| write.write_all(b"x")).is_err());
    journal
        .replace(5, |write| write.write_all(b"next\n"))
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = journal.replace::<()>(4, |write| {
            write.write_all(b"p")?;
            panic!("actual journal encoder");
        });
    }));
    assert!(panic.is_err());
    // Unwinding did not take/drop the temporary or lock original from State.
    let state = owner.0.lock();
    assert!(!state.temporary.confirmed());
    assert!(!state.lock.confirmed());
    drop(state);
    let receipt = owner.try_close().unwrap().unwrap();
    assert!(owner.matches_closed(&receipt));
    assert_eq!(std::fs::read(dir.path().join("state")).unwrap(), b"next\n");
}
#[test]
fn a_file_over_the_recovery_limit_is_preserved_and_can_reopen_with_larger_explicit_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state");
    std::fs::write(&path, b"larger valid snapshot\n").unwrap();
    let owner = JournalOwner::standalone(spec(path.clone(), 4)).unwrap();
    owner.start().unwrap();
    let mut parsed = false;
    assert_eq!(
        owner
            .grant()
            .read_snapshot(|_| {
                parsed = true;
                Ok(())
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::FileTooLarge
    );
    assert!(!parsed);
    assert_eq!(std::fs::read(&path).unwrap(), b"larger valid snapshot\n");
    assert!(owner.matches_closed(&owner.try_close().unwrap().unwrap()));
    let larger = JournalOwner::standalone(spec(path, 64)).unwrap();
    larger.start().unwrap();
    assert!(larger
        .grant()
        .read_snapshot(|read| {
            let mut bytes = Vec::new();
            read.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap()
        .is_some());
    assert!(larger.matches_closed(&larger.try_close().unwrap().unwrap()));
}

mod actual_fault;

#[cfg(unix)]
#[test]
fn declared_parent_policy_preserves_generic_permissions_and_allows_explicit_private_storage() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    for (private, expected) in [(false, 0o770), (true, 0o700)] {
        let parent = directory
            .path()
            .join(if private { "private" } else { "shared" });
        std::fs::create_dir(&parent).unwrap();
        std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o770)).unwrap();
        let mut declaration = spec(parent.join("journal"), 64);
        declaration.parent_private = private;
        let owner = JournalOwner::standalone(declaration).unwrap();
        owner.start().unwrap();
        assert_eq!(
            std::fs::metadata(&parent).unwrap().permissions().mode() & 0o777,
            expected
        );
        assert!(owner.matches_closed(&owner.try_close().unwrap().unwrap()));
    }
}
