use super::*;
fn hello() -> Vec<u8> {
    let extensions = [0, 16, 0, 5, 0, 3, 2, b'h', b'2'];
    let mut body = vec![3, 3];
    body.extend([0; 32]);
    body.extend([0, 0, 2, 0x13, 1, 1, 0]);
    body.extend((extensions.len() as u16).to_be_bytes());
    body.extend(extensions);
    let mut handshake = vec![1, 0, 0, body.len() as u8];
    handshake.extend(body);
    handshake
}
#[tokio::test]
async fn inspection_reassembles_fragmented_records_and_replays_exact_original_wire() {
    let hello = hello();
    for length in [1, 3, 7, hello.len()] {
        let mut wire = Vec::new();
        for fragment in hello.chunks(length) {
            wire.extend([22, 3, 1]);
            wire.extend((fragment.len() as u16).to_be_bytes());
            wire.extend(fragment);
        }
        let expected = wire.clone();
        wire.extend(b"later");
        let mut reader = wire.as_slice();
        let result = peek_client_hello(&mut reader).await.unwrap().unwrap();
        assert_eq!(result.alpn, ["h2"]);
        assert_eq!(result.consumed, expected);
        assert_eq!(reader, b"later");
    }
}
#[tokio::test]
async fn non_tls_prefix_is_preserved_and_oversized_records_fail_before_allocation() {
    let mut reader = b"GET /rest".as_slice();
    let result = peek_client_hello(&mut reader).await.unwrap().unwrap();
    assert_eq!(result.consumed, b"GET /");
    assert_eq!(reader, b"rest");
    let mut reader = [22, 3, 1, 255, 255].as_slice();
    assert!(peek_client_hello(&mut reader).await.is_err());
}
