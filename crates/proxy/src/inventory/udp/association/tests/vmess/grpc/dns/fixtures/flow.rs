//! Domain fixtures use the same real Source actor as other gRPC cases.
pub(crate) use super::super::super::flow::{open, Flow};
use super::*;
pub(crate) async fn retire(proxy: &Proxy, dispatch: Flow) {
    let completed = dispatch.finish_all().await;
    assert_eq!(completed[0].upstream.as_ref().unwrap().0, NAME);
    let report = proxy
        .tcp_runtime_services()
        .execution()
        .retired_datagrams()
        .progress_retired(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    assert!(report.is_complete(), "{report:?}");
}
