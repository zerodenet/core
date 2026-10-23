use super::super::catalog::ComponentCatalog;
use super::host_admission::{config, deadline};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::time::Instant;

#[tokio::test]
async fn cancelled_catalog_shutdown_retains_the_actual_started_owners_for_confirmed_retry() {
    use std::future::Future;
    use zero_platform_tokio::contract::{
        component::{BuildContext, ComponentFactory, ComponentInstance, ComponentSpec},
        lifecycle::{
            erase_lifecycle, BuildError, CloseReport, DynResourceLifecycle, ResourceLifecycle,
        },
        CloseReason,
    };
    use zero_traits::ProtocolMetadata;
    struct Gate {
        release: tokio::sync::watch::Receiver<bool>,
        entered: Arc<tokio::sync::Notify>,
        drops: Arc<AtomicUsize>,
        inner: Box<dyn DynResourceLifecycle>,
    }
    impl Drop for Gate {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl ResourceLifecycle for Gate {
        async fn start(&mut self, deadline: Instant) -> Result<(), BuildError> {
            self.inner.start(deadline).await
        }
        fn revoke(&self, reason: CloseReason) {
            self.inner.revoke(reason);
        }
        async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, BuildError> {
            let waiting = self.release.wait_for(|ready| *ready);
            tokio::pin!(waiting);
            std::future::poll_fn(|cx| match waiting.as_mut().poll(cx) {
                std::task::Poll::Pending => {
                    self.entered.notify_one();
                    std::task::Poll::Pending
                }
                std::task::Poll::Ready(result) => std::task::Poll::Ready(result),
            })
            .await
            .expect("actual cleanup gate retained");
            self.inner.shutdown(deadline).await
        }
    }
    struct Factory {
        release: tokio::sync::watch::Receiver<bool>,
        entered: Arc<tokio::sync::Notify>,
        drops: Arc<AtomicUsize>,
    }
    impl ComponentFactory for Factory {
        type Options = crate::component::DirectOptions;
        fn describe(&self, options: &Self::Options) -> ComponentSpec {
            crate::component::DirectFactory.describe(options)
        }
        fn build(
            &self,
            options: Arc<Self::Options>,
            context: BuildContext,
        ) -> Result<ComponentInstance, BuildError> {
            let mut instance = crate::component::DirectFactory.build(options, context)?;
            instance.lifecycle = erase_lifecycle(Gate {
                release: self.release.clone(),
                entered: self.entered.clone(),
                drops: self.drops.clone(),
                inner: instance.lifecycle,
            });
            Ok(instance)
        }
    }
    let (release, receiver) = tokio::sync::watch::channel(false);
    let entered = Arc::new(tokio::sync::Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut factories = crate::component::ComponentRegistry::default();
    factories
        .register(
            crate::adapters::direct::DirectAdapter::default().descriptor(),
            Factory {
                release: receiver,
                entered: entered.clone(),
                drops: drops.clone(),
            },
            crate::adapters::direct::factory::decode,
        )
        .unwrap();
    let catalog = Arc::new(ComponentCatalog::with_factories(factories));
    catalog.start(config(), deadline()).await.unwrap();
    let closing = {
        let catalog = catalog.clone();
        tokio::spawn(async move { catalog.shutdown(deadline()).await })
    };
    entered.notified().await;
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "interrupted cleanup retains both real owners"
    );
    release.send_replace(true);
    catalog.shutdown(deadline()).await.unwrap();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        2,
        "owners drop only after actual retry confirms both"
    );
}
