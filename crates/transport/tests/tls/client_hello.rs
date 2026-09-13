use super::parse_extensions;
#[test]
fn alpn_list_starts_after_its_two_byte_length() {
    let hello = parse_extensions(
        &[
            0, 16, 0, 14, 0, 12, 2, b'h', b'2', 8, b'h', b't', b't', b'p', b'/', b'1', b'.', b'1',
        ],
        vec![],
    );
    assert_eq!(hello.alpn, ["h2", "http/1.1"]);
    for malformed in [
        &[0, 16, 0, 4, 0, 3, 1, b'h'][..],
        &[0, 16, 0, 3, 0, 1, 0][..],
    ] {
        assert!(parse_extensions(malformed, vec![]).alpn.is_empty());
    }
}
