use super::*;
use std::{io, sync::Arc};

#[derive(Debug)]
struct OriginalFault(&'static str);
impl std::fmt::Display for OriginalFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for OriginalFault {}

#[tokio::test]
async fn all_original_join_and_native_failures_survive_final_application_result() {
    let native = Arc::new(OriginalFault("matching native close failed"));
    let native_source = native.clone();
    let server = tokio::spawn(async move { Err::<(), _>(io::Error::other(native_source)) });
    let worker =
        tokio::task::spawn_blocking(|| -> io::Result<()> { panic!("original subscription panic") });
    let server = server.await.unwrap();
    let worker = worker.await.unwrap_err();
    assert!(worker.is_panic());
    let mut failures = RunFailures::default();
    failures.result("proxy", Err::<(), _>(zero_engine::EngineError::NoInbounds));
    failures.result("control", server);
    failures.result("worker", Err::<(), _>(worker));
    let returned = failures.finish().unwrap_err();
    let all = returned.downcast_ref::<RunFailures>().unwrap();
    assert_eq!(all.0.len(), 3);
    assert!(matches!(
        all.0[0].1.downcast_ref::<zero_engine::EngineError>(),
        Some(zero_engine::EngineError::NoInbounds)
    ));
    let original = all.0[1]
        .1
        .downcast_ref::<io::Error>()
        .unwrap()
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<OriginalFault>>()
        .unwrap();
    assert!(Arc::ptr_eq(original, &native));
    assert!(all.0[2]
        .1
        .downcast_ref::<tokio::task::JoinError>()
        .unwrap()
        .is_panic());
    assert!(returned.to_string().contains("original subscription panic"));
}

#[test]
fn a_single_application_fault_keeps_its_original_error_identity() {
    let original = Arc::new(OriginalFault("only fault"));
    let mut failures = RunFailures::default();
    failures.error("control", Box::new(original.clone()));
    let returned = failures.finish().unwrap_err();
    assert!(Arc::ptr_eq(
        returned.downcast_ref::<Arc<OriginalFault>>().unwrap(),
        &original
    ));
}
