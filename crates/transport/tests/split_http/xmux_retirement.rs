//! Pure group custody, not native-provider or original-close qualification.
use super::*;

fn retains(pool: &XhttpClientPool, group: &GroupRef) -> bool {
    pool.0
        .state
        .lock()
        .unwrap()
        .groups
        .iter()
        .any(|held| std::ptr::eq(&**held, &**group))
}

#[test]
fn expired_group_keeps_usage_and_body_custody_but_cannot_be_selected_again() {
    let config = SplitHttpXmux {
        h_max_request_times: SplitHttpRange { from: 1, to: 1 },
        ..Default::default()
    };
    let pool = XhttpClientPool::new(config);
    let usage = pool.select(None).unwrap();
    let original = usage.group.clone();
    let body = group::ResponseUse::new(original.clone(), true);
    assert!(original.take_request());
    assert!(!original.request_reusable());
    pool.reap();
    assert!(
        retains(&pool, &original),
        "the original usage and body still occupy this group"
    );

    let next = pool.select(None).unwrap();
    assert!(
        !std::ptr::eq(&*original, &*next.group),
        "an exhausted group is retained for return, not reuse"
    );
    drop(usage);
    pool.reap();
    assert!(
        retains(&pool, &original),
        "the actual response body still owns its original group"
    );
    drop(body);
    pool.reap();
    assert!(!retains(&pool, &original));
    assert!(retains(&pool, &next.group));
}

#[tokio::test]
async fn rotating_requests_wait_for_finite_response_return() {
    let pool = XhttpClientPool::new(SplitHttpXmux {
        h_max_request_times: SplitHttpRange::new(1, 1),
        ..Default::default()
    });
    let previous = pool.select(None).unwrap();
    assert!(previous.group.take_request());
    let response = group::ResponseUse::new(previous.group.clone(), false);
    let next = pool.select(None).unwrap();
    let wait = pool.wait_rotating_responses(&next.group, None);
    tokio::pin!(wait);
    std::future::poll_fn(|cx| {
        assert!(wait.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(response);
    tokio::time::timeout(std::time::Duration::from_secs(1), &mut wait)
        .await
        .unwrap();
}

#[tokio::test]
async fn exhausted_groups_cannot_wait_on_each_others_finite_responses() {
    let pool = XhttpClientPool::new(SplitHttpXmux {
        h_max_request_times: SplitHttpRange::new(1, 1),
        ..Default::default()
    });
    let first = pool.select(None).unwrap();
    assert!(first.group.take_request());
    let first_response = group::ResponseUse::new(first.group.clone(), false);
    let second = pool.select(None).unwrap();
    assert!(second.group.take_request());
    let _second_response = group::ResponseUse::new(second.group.clone(), false);

    let first_wait = pool.wait_rotating_responses(&first.group, None);
    let second_wait = pool.wait_rotating_responses(&second.group, None);
    tokio::pin!(first_wait, second_wait);
    std::future::poll_fn(|cx| {
        assert!(first_wait.as_mut().poll(cx).is_ready());
        assert!(second_wait.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(first_response);
    tokio::time::timeout(std::time::Duration::from_secs(1), second_wait)
        .await
        .unwrap();
}

#[tokio::test]
async fn rotation_never_waits_for_streaming_or_its_own_response() {
    for streaming in [false, true] {
        let pool = XhttpClientPool::new(SplitHttpXmux {
            h_max_request_times: SplitHttpRange::new(1, 1),
            ..Default::default()
        });
        let usage = pool.select(None).unwrap();
        assert!(usage.group.take_request());
        let _response = group::ResponseUse::new(usage.group.clone(), streaming);
        pool.wait_rotating_responses(&usage.group, None).await;
        if streaming {
            let next = pool.select(None).unwrap();
            pool.wait_rotating_responses(&next.group, None).await;
        }
    }
}
