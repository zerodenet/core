use openssl::{
    ssl::{SslContextBuilder, SslMethod},
    stack::Stack,
    x509::{X509StoreContext, X509},
};

fn verifies_published_root(disable_system_roots: bool) -> bool {
    let mut builder = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
    super::configure(&mut builder, disable_system_roots).unwrap();
    let context = builder.build();
    let root = X509::from_der(webpki_root_certs::TLS_SERVER_ROOT_CERTS[0].as_ref()).unwrap();
    let chain = Stack::new().unwrap();
    X509StoreContext::new()
        .unwrap()
        .init(context.cert_store(), &root, &chain, |context| {
            context.verify_cert()
        })
        .unwrap()
}

#[test]
fn openssl_store_trusts_a_published_mozilla_root() {
    assert!(verifies_published_root(false));
}

#[test]
fn disabling_system_roots_leaves_the_published_root_untrusted() {
    assert!(!verifies_published_root(true));
}
