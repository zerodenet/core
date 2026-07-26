use tonic::Request;

use super::GrpcServerAuth;

#[test]
fn bearer_auth_accepts_the_configured_credential() {
    let auth = GrpcServerAuth::single_admin("secret".to_owned());
    let mut request = Request::new(());
    request
        .metadata_mut()
        .insert("authorization", "Bearer secret".parse().expect("metadata"));

    assert!(auth.is_authorized(&request));
}

#[test]
fn bearer_auth_rejects_missing_or_different_credentials() {
    let auth = GrpcServerAuth::single_admin("secret".to_owned());
    assert!(!auth.is_authorized(&Request::new(())));

    let mut different = Request::new(());
    different
        .metadata_mut()
        .insert("authorization", "Bearer other".parse().expect("metadata"));
    assert!(!auth.is_authorized(&different));
}
