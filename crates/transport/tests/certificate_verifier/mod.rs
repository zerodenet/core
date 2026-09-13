use super::*;
use rustls::client::danger::ServerCertVerifier;
use rustls::internal::msgs::codec::{Codec, Reader};

#[test]
fn insecure_trust_still_rejects_invalid_tls12_and_tls13_handshake_signatures() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let verifier = InsecureServerVerifier {
        provider: Arc::new(rustls::crypto::ring::default_provider()),
    };
    let forged = [4, 3, 0, 4, 1, 2, 3, 4];
    let signed = rustls::DigitallySignedStruct::read(&mut Reader::init(&forged)).unwrap();
    assert!(verifier
        .verify_tls12_signature(b"handshake", cert.cert.der(), &signed)
        .is_err());
    assert!(verifier
        .verify_tls13_signature(b"handshake", cert.cert.der(), &signed)
        .is_err());
}
