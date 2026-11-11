//! Actual Source readiness and original native/user/Scope receipts.
use crate::Proxy;
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

pub(super) async fn owner(
    proxy: &Proxy,
    tag: &str,
) -> Arc<crate::runtime::inbound_owners::InboundOwner> {
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(owner) = proxy.inbound_owners.owner(tag) {
                owner.wait_ready().await.unwrap();
                return owner;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}

pub(super) fn closed(owner: &crate::runtime::inbound_owners::InboundOwner) {
    let actual = owner
        .terminal_for_test()
        .expect("actual original Source native/users/Scope receipt");
    assert_eq!(actual.identity(), owner.identity());
    assert!(actual.joined_tasks() > 0 && actual.closed_io_receipts() > 0);
    assert!(
        actual.failure().is_none()
            && actual.task_failures().is_empty()
            && actual.native_bound_failures().is_empty()
    );
}
