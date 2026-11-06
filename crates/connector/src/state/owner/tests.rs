use super::*;
#[test]
fn held_actual_sqlite_view_keeps_original_attempt_and_both_faults_until_real_return() {
    // A genuine independent fixture owner, so its deliberately held original
    // cannot poison concurrent standalone inspector tests' production slot.
    let slot = Slot {
        state: Mutex::new(State {
            current: None,
            running: false,
        }),
        changed: Condvar::new(),
    };
    let source = Arc::new(ConfigurationSource::default());
    let (lease, prior) =
        begin_on(&slot, source.clone()).unwrap_or_else(|_| panic!("fresh concrete inspector"));
    assert!(prior.is_empty());
    let original = lease.original.clone();
    let scratch = tempfile::tempdir().unwrap();
    let scratch_path = scratch.path().to_owned();
    let database = original.resources.database(scratch).unwrap();
    database
        .lock()
        .unwrap()
        .connection
        .as_ref()
        .unwrap()
        .execute_batch("CREATE TABLE actual (id INTEGER);")
        .unwrap();
    let work = Arc::new(io::Error::new(
        io::ErrorKind::InvalidData,
        "original inspection decode error",
    ));
    original.work_failed_arc(work.clone());
    let (complete, received) = lease.finish();
    assert!(!complete && received.len() >= 2);
    assert!(scratch_path.exists());
    let prior = match begin_on(&slot, source.clone()) {
        Err(faults) => faults,
        Ok((lease, _)) => {
            drop(lease);
            panic!("a retained real SQLite view must fence successor native IO");
        }
    };
    assert!(prior.len() >= 2);
    assert!(Arc::strong_count(&work) > 2);
    drop(database);
    let (fresh, handed) = begin_on(&slot, source).unwrap_or_else(|_| {
        panic!("same original cleanup can progress after its real view returns")
    });
    assert!(handed.len() >= 2);
    assert!(original.resources.retirement_complete());
    assert!(!scratch_path.exists());
    assert!(!Arc::ptr_eq(&original, &fresh.original));
    let (complete, faults) = fresh.finish();
    assert!(complete && faults.is_empty());
    assert!(received.len() >= 2 && Arc::strong_count(&work) > 2);
}
