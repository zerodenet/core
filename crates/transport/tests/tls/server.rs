use super::{
    super::authority::IssuedCertificate, IssuedCertificateCache, ISSUED_CERTIFICATE_CACHE_CAPACITY,
};
use rustls::{pki_types::PrivateKeyDer, sign::CertifiedKey};
use std::sync::Arc;

fn certificate() -> Arc<CertifiedKey> {
    let generated = rcgen::generate_simple_self_signed(vec!["cache.test".into()]).unwrap();
    Arc::new(
        CertifiedKey::from_der(
            vec![generated.cert.der().clone()],
            PrivateKeyDer::Pkcs8(generated.signing_key.serialize_der().into()),
            &rustls::crypto::ring::default_provider(),
        )
        .unwrap(),
    )
}

#[test]
fn issued_certificate_cache_is_bounded_and_evicts_the_least_recent_name() {
    let key = certificate();
    let mut cache = IssuedCertificateCache::default();
    for index in 0..ISSUED_CERTIFICATE_CACHE_CAPACITY {
        cache.insert(
            format!("{index}.test"),
            IssuedCertificate {
                key: key.clone(),
                not_after: i64::MAX,
            },
        );
    }
    assert!(cache.get("0.test", 0).is_some());
    cache.insert(
        "new.test".into(),
        IssuedCertificate {
            key,
            not_after: i64::MAX,
        },
    );
    assert_eq!(cache.values.len(), ISSUED_CERTIFICATE_CACHE_CAPACITY);
    assert!(cache.get("0.test", 0).is_some());
    assert!(cache.get("1.test", 0).is_none());
}
