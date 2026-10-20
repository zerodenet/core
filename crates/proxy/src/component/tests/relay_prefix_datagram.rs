use super::*;

#[tokio::test]
async fn datagram_prefix_rejects_multiple_missing_role_and_nonshrinking_binding_before_io() {
    use zero_core::{component::AssociationId, request::DatagramTargets};
    let (opener, capture) = fixture();
    let request = |targets| DatagramRequest {
        association: AssociationId(11),
        targets,
    };
    let fixed = || zero_core::request::DatagramTargets::Fixed(opener.expected_endpoint.clone());
    let error = opener
        .open_datagram(
            &opener.expected_binding,
            request(DatagramTargets::Multiple),
            carrier(&opener),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::Unsupported);
    let error = opener
        .open_datagram(&opener.expected_binding, request(fixed()), carrier(&opener))
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::Unsupported);
    let mut nonshrinking = opener.expected_binding.clone();
    nonshrinking.prefix.clear();
    let error = opener
        .open_datagram(&nonshrinking, request(fixed()), carrier(&opener))
        .await
        .err()
        .unwrap();
    assert_eq!(error.reason, Reason::StalePlan);
    assert_eq!(capture.calls.load(Ordering::SeqCst), 0);
}
