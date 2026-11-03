use super::*;
#[test]
fn native_file_receipt_matches_only_the_same_original_and_fences_io() {
    let directory = tempfile::tempdir().unwrap();
    let mut first = OriginalFile::adopt(
        std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(directory.path().join("first"))
            .unwrap(),
    );
    let mut second =
        OriginalFile::adopt(std::fs::File::create(directory.path().join("second")).unwrap());
    first.write_all(b"original file bytes").unwrap();
    first.sync_all().unwrap();
    first.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = Vec::new();
    first.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"original file bytes");
    let receipt = first.close().unwrap();
    assert!(first.matches_closed(&receipt));
    assert!(!second.matches_closed(&receipt));
    assert!(first.write_all(b"retired").is_err());
    let repeated = first.close().unwrap();
    assert!(first.matches_closed(&repeated));
    let other = second.close().unwrap();
    assert!(!first.matches_closed(&other));
}
