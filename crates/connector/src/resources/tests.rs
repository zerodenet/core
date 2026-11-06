use super::*;
use zero_api::{ApiEvent, EventSink};

#[test]
fn returned_view_does_not_close_original_until_matching_native_receipt_is_consumed() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original");
    let resources = DispatcherResources::default();
    let view = resources.open(|| File::create(&path)).unwrap();
    let identity = Arc::downgrade(&view.original);
    drop(view); // actual transferred view retired, no native receipt yet
    {
        let original = identity.upgrade().unwrap();
        assert!(original.lock().unwrap().metadata().is_ok());
        assert_eq!(resources.0.files.lock().unwrap().len(), 1);
    }
    resources.close().unwrap();
    assert!(identity.upgrade().is_none());
    assert!(resources.0.files.lock().unwrap().is_empty());
    let replacement = resources.open(|| File::open(&path)).unwrap();
    drop(replacement);
    resources.close().unwrap();
}

#[test]
fn outstanding_actual_writer_view_keeps_original_and_blocks_native_replacement() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("original");
    let resources = DispatcherResources::default();
    let view = resources.open(|| File::create(&path)).unwrap();
    let identity = Arc::downgrade(&view.original);
    assert!(resources.close().is_err());
    assert!(!resources.retirement_complete());
    assert!(identity.upgrade().is_some());
    // No native construction may occur after the unconfirmed retirement.
    let mut constructed = false;
    assert!(
        resources
            .open(|| {
                constructed = true;
                File::open(&path)
            })
            .is_err()
    );
    assert!(!constructed);
    drop(view);
    assert!(resources.close().is_err()); // sticky failure remains evidence
    assert!(identity.upgrade().is_none()); // actual native close still driven
    assert!(resources.retirement_complete()); // historical fault is not native debt
}

#[test]
fn dead_letter_preserves_public_envelope_and_jsonl_writer_views_retire_before_close() {
    let directory = tempfile::tempdir().unwrap();
    let public_path = directory.path().join("public.jsonl");
    let owned_path = directory.path().join("owned.jsonl");
    let event = ApiEvent::new("original", "test.fact", 3, serde_json::json!({"value":4}));
    let public = zero_api::DeadLetterSink::new(&public_path).unwrap();
    public.publish(&event).unwrap();
    let resources = DispatcherResources::default();
    let owned = RetainedDeadLetter::new(&owned_path, &resources).unwrap();
    owned.publish(&event).unwrap();
    let parse = |path| {
        serde_json::from_str::<serde_json::Value>(&std::fs::read_to_string(path).unwrap()).unwrap()
    };
    let mut expected = parse(&public_path);
    let mut actual = parse(&owned_path);
    assert!(actual["dead_lettered_at_unix_ms"].is_u64());
    expected
        .as_object_mut()
        .unwrap()
        .remove("dead_lettered_at_unix_ms");
    actual
        .as_object_mut()
        .unwrap()
        .remove("dead_lettered_at_unix_ms");
    assert_eq!(actual, expected);
    drop(owned);
    resources.close().unwrap();
    assert!(resources.0.files.lock().unwrap().is_empty());
    assert!(resources.0.dead_letters.lock().unwrap().is_empty());
}

#[test]
fn sqlite_close_busy_retains_the_exact_original_connection_and_error() {
    let resources = DispatcherResources::default();
    let original = resources.database(tempfile::tempdir().unwrap()).unwrap();
    let raw_connection;
    let mut raw_statement = std::ptr::null_mut();
    {
        let database = original.lock().unwrap();
        // An actual outstanding SQLite native user, outside Rust's Statement
        // borrow, makes the genuine Connection.close return SQLITE_BUSY.
        raw_connection = unsafe { database.connection.as_ref().unwrap().handle() };
        let sql = std::ffi::CString::new("SELECT 1").unwrap();
        let result = unsafe {
            rusqlite::ffi::sqlite3_prepare_v2(
                raw_connection,
                sql.as_ptr(),
                -1,
                &mut raw_statement,
                std::ptr::null_mut(),
            )
        };
        assert_eq!(result, rusqlite::ffi::SQLITE_OK);
    }
    {
        let mut database = original.lock().unwrap();
        assert!(database.close().is_err());
        assert_eq!(
            unsafe { database.connection.as_ref().unwrap().handle() },
            raw_connection
        );
        assert!(
            matches!(database.failure.as_deref(), Some(rusqlite::Error::SqliteFailure(error, _)) if error.extended_code == rusqlite::ffi::SQLITE_BUSY)
        );
    }
    assert_eq!(
        unsafe { rusqlite::ffi::sqlite3_finalize(raw_statement) },
        rusqlite::ffi::SQLITE_OK
    );
    let first = original.lock().unwrap().failure.as_ref().unwrap().clone();
    let repeated = original.lock().unwrap().close().unwrap_err(); // do not erase first failure
    assert!(
        repeated
            .get_ref()
            .and_then(|source| source.downcast_ref::<Arc<rusqlite::Error>>())
            .is_some_and(|source| Arc::ptr_eq(source, &first))
    );
    {
        let database = original.lock().unwrap();
        assert!(
            database.connection.is_none(),
            "same original Connection.close succeeded after its real statement returned"
        );
        assert!(
            database.scratch.is_none(),
            "actual private scratch removal completed"
        );
        assert!(database.retirement_complete());
    }
    drop(original);
    assert!(resources.close().is_err());
    assert!(resources.0.databases.lock().unwrap().is_empty());
    assert!(resources.retirement_complete());
    assert!(
        resources.0.retired_faults.lock().unwrap()[0]
            .get_ref()
            .and_then(|source| source.downcast_ref::<Arc<rusqlite::Error>>())
            .is_some_and(|source| Arc::ptr_eq(source, &first))
    );
}
