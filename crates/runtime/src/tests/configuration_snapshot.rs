use super::*;
#[test]
fn borrowed_seek_snapshot_keeps_original_inode_length_and_matches_actual_close_on_decode_error() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state.jsonl");
    std::fs::write(&path, b"original").unwrap();
    let source = Arc::new(ConfigurationSource::default());
    let before = source.io.original_observation();
    let (length, result) = source
        .inspect_seekable_file_snapshot(&path, |length, reader| {
            assert_eq!(length, 8);
            let mut append = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            use std::io::Write;
            append.write_all(b"-new").unwrap();
            drop(append);
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            assert_eq!(bytes, b"original");
            reader.seek(SeekFrom::End(-3))?;
            let mut tail = [0; 3];
            reader.read_exact(&mut tail)?;
            assert_eq!(&tail, b"nal");
            assert_eq!(
                reader.seek(SeekFrom::Start(length + 1)).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
            Err::<(), _>(io::Error::new(
                io::ErrorKind::InvalidData,
                "actual decoder failure",
            ))
        })
        .unwrap();
    assert_eq!(length, 8);
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
    let after = source.io.original_observation();
    assert_eq!(after, (before.0 + 1, before.1 + 1, false, false));
    source.confirm_original_io_closed().unwrap();
    let (_, decoded) = source
        .inspect_seekable_file_snapshot(&path, |_, reader| {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap();
    assert_eq!(decoded.unwrap(), b"original-new");
    source.confirm_original_io_closed().unwrap();
}
