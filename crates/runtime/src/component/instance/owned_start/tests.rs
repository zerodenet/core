use super::*;
use crate::component::{
    ComponentRecipe,
    catalog::{ComponentCatalog, supervision::testing::TestContext},
};
use shadowsocks::{
    component::PluginFactory,
    transport::ShadowsocksTransportState,
    validation::{PluginConfig, PluginMode},
};
use std::{num::NonZeroUsize, path::PathBuf, task::Poll};
use tokio::time::Duration;
fn end() -> Instant {
    Instant::now() + Duration::from_secs(5)
}
struct Fixture {
    directory: tempfile::TempDir,
    slot: Arc<ComponentSlot>,
}
impl Fixture {
    fn cold() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("started");
        let gate = directory.path().join("listen");
        let script = format!(
            r#"
import os,socket,time
with open({marker:?},'a') as output: output.write(str(os.getpid())+'\n')
while not os.path.exists({gate:?}): time.sleep(.01)
s=socket.socket();s.bind((os.environ['SS_LOCAL_HOST'],int(os.environ['SS_LOCAL_PORT'])));s.listen()
time.sleep(60)
"#,
            marker = marker.to_str().unwrap(),
            gate = gate.to_str().unwrap()
        );
        let state = ShadowsocksTransportState::default();
        let options = state
            .plugin_component_options(
                "127.0.0.1",
                9999,
                PluginConfig {
                    command: "python3".into(),
                    args: vec!["-c".into(), script],
                    options: None,
                    mode: PluginMode::TcpOnly,
                },
            )
            .with_process_endpoint(NetworkGeneration(0))
            .unwrap();
        let slot = Arc::new(ComponentSlot::from_recipe(
            ComponentCatalog::default().allocate_id().unwrap(),
            ResourceGeneration(1),
            ComponentRecipe::from_validated(Arc::new(PluginFactory), Arc::new(options)),
            Default::default(),
            "actual_owned_start",
        ));
        assert_eq!(slot.recipe.spec().task_limit, NonZeroUsize::new(1));
        Self { directory, slot }
    }
    fn marker(&self) -> PathBuf {
        self.directory.path().join("started")
    }
    async fn native_started(&self) {
        tokio::time::timeout_at(end(), async {
            while self.starts() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn starts(&self) -> usize {
        std::fs::read_to_string(self.marker()).map_or(0, |value| value.lines().count())
    }
    fn permit_listen(&self) {
        std::fs::write(self.directory.path().join("listen"), b"x").unwrap();
    }
}
#[tokio::test]
async fn actual_host_finishes_same_process_initialization_after_its_first_borrower_is_cancelled() {
    let fixture = Fixture::cold();
    let slot = &fixture.slot;
    let host = TestContext::new([slot.as_ref()]);
    let roots = slot.bound_original_budgets().unwrap();
    assert_eq!(roots.process.snapshot().charged_jobs, 0);
    assert!(slot.live.get().is_none());
    let mut first = Box::pin(slot.start_owned(end()));
    host.drive(async {
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        fixture.native_started().await;
    })
    .await;
    assert_eq!(roots.process.snapshot().charged_jobs, 1);
    assert!(!slot.has_started());
    drop(first);
    assert!(
        !slot.cancel.is_cancelled(),
        "a borrower owns no initializer cancellation"
    );
    fixture.permit_listen();
    // There is no startup borrower here. Only the already existing host pump
    // can complete the same original operation and consume its actual events.
    host.drive(tokio::time::timeout_at(
        end(),
        std::future::poll_fn(|_| {
            if slot.has_started() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }),
    ))
    .await
    .unwrap();
    slot.start_owned(end()).await.unwrap();
    assert_eq!(fixture.starts(), 1);
    let endpoint = slot.ready_process_endpoint(0).unwrap();
    assert!(endpoint.closed().is_none());
    let closed = host.drive(slot.shutdown(end())).await.unwrap();
    assert!(closed.is_complete(), "{closed:?}");
    assert_eq!(roots.process.snapshot().charged_jobs, 0);
    assert!(endpoint.closed().unwrap().matches(&endpoint));
    assert!(
        slot.live
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .resources
            .io_evidence()
            .physical_confirmed()
    );
}
#[tokio::test]
async fn actual_shutdown_drains_retained_process_start_before_owner_lock_and_native_tree_close() {
    let fixture = Fixture::cold();
    let slot = &fixture.slot;
    let host = TestContext::new([slot.as_ref()]);
    let roots = slot.bound_original_budgets().unwrap();
    let mut first = Box::pin(slot.start_owned(end()));
    host.drive(async {
        std::future::poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        fixture.native_started().await;
    })
    .await;
    drop(first);
    assert_eq!(roots.process.snapshot().charged_jobs, 1);
    let report = host.drive(slot.shutdown(end())).await.unwrap();
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(roots.process.snapshot().charged_jobs, 0);
    let original = slot.owned_start_failure().unwrap();
    let observed = slot.start_owned(end()).await.unwrap_err();
    assert!(Arc::ptr_eq(&original, &observed));
    assert!(!slot.can_reuse() && !slot.has_started());
    assert_eq!(fixture.starts(), 1);
    assert!(
        slot.live
            .get()
            .unwrap()
            .as_ref()
            .unwrap()
            .resources
            .io_evidence()
            .physical_confirmed()
    );
}
