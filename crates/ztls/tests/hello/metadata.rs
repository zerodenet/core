use ztls::hello::client_hello_metadata;
#[test]
fn extension_inspection_preserves_wire_sni_and_alpn_and_rejects_truncation() {
    let mut body = vec![3, 3];
    body.extend_from_slice(&[0; 32]);
    body.push(0);
    body.extend_from_slice(&[0, 2, 0x13, 1, 1, 0]);
    let name = b"front.example";
    let mut extensions = vec![0, 0];
    extensions.extend_from_slice(&((name.len() + 5) as u16).to_be_bytes());
    extensions.extend_from_slice(&((name.len() + 3) as u16).to_be_bytes());
    extensions.push(0);
    extensions.extend_from_slice(&(name.len() as u16).to_be_bytes());
    extensions.extend_from_slice(name);
    extensions.extend_from_slice(&[0, 16, 0, 5, 0, 3, 2, b'h', b'2']);
    body.extend_from_slice(&(extensions.len() as u16).to_be_bytes());
    body.extend(extensions);
    let mut record = vec![22, 3, 1];
    record.extend_from_slice(&((body.len() + 4) as u16).to_be_bytes());
    record.push(1);
    record.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
    record.extend(body);
    let metadata = client_hello_metadata(&record).unwrap();
    assert_eq!(metadata.server_name.as_deref(), Some("front.example"));
    assert_eq!(metadata.alpn, ["h2"]);
    for length in 0..record.len() {
        assert!(client_hello_metadata(&record[..length]).is_err());
    }
}
