use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_transport::proxy_protocol::{accept, encode};
#[tokio::test]
async fn proxy_preambles_preserve_following_payload_and_ipv4_ipv6_addresses() {
    for (source, destination) in [
        ("192.0.2.1:1234", "198.51.100.2:443"),
        ("[2001:db8::1]:1234", "[2001:db8::2]:443"),
    ] {
        for version in [1, 2] {
            let source = source.parse().unwrap();
            let destination = destination.parse().unwrap();
            let mut bytes = encode(version, Some(source), Some(destination)).unwrap();
            bytes.extend_from_slice(b"payload");
            let (mut a, mut b) = tokio::io::duplex(8192);
            a.write_all(&bytes).await.unwrap();
            assert_eq!(accept(&mut b).await.unwrap(), Some((source, destination)));
            let mut payload = [0; 7];
            b.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"payload");
        }
    }
    assert_eq!(
        encode(
            1,
            Some("192.0.2.1:1234".parse().unwrap()),
            Some("198.51.100.2:443".parse().unwrap())
        )
        .unwrap(),
        b"PROXY TCP4 192.0.2.1 198.51.100.2 1234 443\r\n"
    );
    assert_eq!(
        encode(2, None, None).unwrap(),
        b"\r\n\r\n\0\r\nQUIT\n\x20\0\0\0"
    );
}
#[tokio::test]
async fn required_proxy_preamble_rejects_invalid_family_version_lengths_and_missing_header() {
    for bytes in [
        b"GET / HTTP/1.1\r\n\r\n".as_slice(),
        b"PROXY TCP4 ::1 127.0.0.1 1 2\r\n",
        b"PROXY TCP4 192.0.2.1 192.0.2.2 65536 1\r\n",
        b"\r\n\r\n\0\r\nQUIT\n\x11\x11\0\0",
        b"\r\n\r\n\0\r\nQUIT\n\x21\x11\0\x01\0",
    ] {
        let (mut a, mut b) = tokio::io::duplex(8192);
        a.write_all(bytes).await.unwrap();
        drop(a);
        assert!(accept(&mut b).await.is_err());
    }
}
