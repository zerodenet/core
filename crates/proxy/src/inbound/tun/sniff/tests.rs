use super::{parse_http_host, parse_tls_extensions};

#[test]
fn detects_ech_and_does_not_treat_outer_sni_as_the_hidden_name() {
    let hostname = b"public.example";
    let mut extensions = Vec::new();
    extensions.extend_from_slice(&0_u16.to_be_bytes());
    extensions.extend_from_slice(&((hostname.len() + 5) as u16).to_be_bytes());
    extensions.extend_from_slice(&((hostname.len() + 3) as u16).to_be_bytes());
    extensions.push(0);
    extensions.extend_from_slice(&(hostname.len() as u16).to_be_bytes());
    extensions.extend_from_slice(hostname);
    extensions.extend_from_slice(&0xfe0d_u16.to_be_bytes());
    extensions.extend_from_slice(&0_u16.to_be_bytes());

    let parsed = parse_tls_extensions(&extensions);
    assert_eq!(parsed.sni.as_deref(), Some("public.example"));
    assert!(parsed.encrypted_client_hello);
}

#[test]
fn parses_host_header_and_absolute_form_without_accepting_userinfo() {
    assert_eq!(
        parse_http_host(b"GET / HTTP/1.1\r\nhOsT: Example.COM:8080\r\n\r\n"),
        Some("Example.COM")
    );
    assert_eq!(
        parse_http_host(b"GET http://absolute.example/path HTTP/1.1\r\n\r\n"),
        Some("absolute.example")
    );
    assert_eq!(
        parse_http_host(b"GET http://user@unsafe.example/ HTTP/1.1\r\n\r\n"),
        None
    );
}
