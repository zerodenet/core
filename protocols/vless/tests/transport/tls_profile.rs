use super::{client_tls, server_tls};
use zero_transport::profile::{OwnedClientTlsProfile, OwnedServerTlsProfile};

#[test]
fn reference_tls_alpn_defaults_apply_without_replacing_explicit_protocols() {
    let mut client = OwnedClientTlsProfile {
        options: Default::default(),
        server_name: Some("localhost".into()),
        disable_sni: false,
        ca_cert_path: None,
        insecure: false,
        alpn: Vec::new(),
        client_fingerprint: None,
    };
    let mut server = OwnedServerTlsProfile {
        options: Default::default(),
        cert_path: "cert.pem".into(),
        key_path: "key.pem".into(),
        alpn: Vec::new(),
        server_fingerprint: None,
    };
    assert_eq!(client_tls(&client).alpn, ["h2", "http/1.1"]);
    assert_eq!(server_tls(&server).alpn, ["h2", "http/1.1"]);
    client.alpn = vec!["http/1.1".into()];
    server.alpn = vec!["http/1.1".into()];
    assert_eq!(client_tls(&client).alpn, client.alpn);
    assert_eq!(server_tls(&server).alpn, server.alpn);
}
