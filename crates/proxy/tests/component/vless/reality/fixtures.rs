pub(super) fn profile<'a>(
    key: &'a str,
    short: &'a str,
    name: &'a str,
) -> vless::transport::VlessRealityClientOptionsRef<'a> {
    vless::transport::VlessRealityClientOptionsRef {
        spider_x: "/",
        hybrid_key_exchange: false,
        mldsa65_verify: None,
        public_key: key,
        short_id: short,
        server_name: Some(name),
        cipher_suites: &[],
        client_fingerprint: "chrome",
    }
}
pub(super) fn server(private: &str) -> vless::reality::VlessRealityServerProfile {
    vless::reality::VlessRealityServerProfile::new(
        private,
        vec!["0123456789abcdef".into()],
        Some("localhost"),
        vec![],
    )
    .with_policy(vless::reality_policy::PolicyRef {
        min_client_version: Some("26.3.27"),
        max_client_version: Some("26.3.27"),
        max_time_diff_ms: 60_000,
        ..Default::default()
    })
    .unwrap()
}
