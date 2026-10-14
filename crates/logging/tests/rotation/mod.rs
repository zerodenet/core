use super::*;

#[test]
fn single_file_rotation_bounds_repeated_writes_and_reopened_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.log");
    fs::write(&path, b"old content exceeding the limit").unwrap();
    let mut writer = RotatingWriter::new(path.to_str().unwrap(), 16, 1).unwrap();
    for _ in 0..100 {
        writer.write_all(b"record\n").unwrap();
        writer.flush().unwrap();
        assert!(fs::metadata(&path).unwrap().len() <= 16);
    }
    assert!(!path.with_extension("1").exists());
    assert_eq!(fs::read_to_string(&path).unwrap(), "record\nrecord\n");
}

#[test]
fn oversized_write_remains_intact_then_rotates_before_next_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut writer = RotatingWriter::new(path.to_str().unwrap(), 4, 2).unwrap();
    writer.write_all(b"large-record\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"large-record\n");
    writer.write_all(b"ok\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"ok\n");
    assert_eq!(
        fs::read(path.with_extension("1")).unwrap(),
        b"large-record\n"
    );
}

#[test]
fn failed_rotation_preserves_length_and_reports_the_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.log");
    let mut writer = RotatingWriter::new(path.to_str().unwrap(), 4, 2).unwrap();
    writer.write_all(b"old\n").unwrap();
    fs::create_dir(path.with_extension("1")).unwrap();
    assert!(writer.write_all(b"new\n").is_err());
    assert_eq!(writer.written, 4);
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
    fs::remove_dir(path.with_extension("1")).unwrap();
    writer.write_all(b"new\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"new\n");
}
