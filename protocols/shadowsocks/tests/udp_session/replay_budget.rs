use super::*;

#[tokio::test(start_paused = true)]
async fn total_budget_keeps_retired_originals_until_the_real_61_second_expiry() {
    let budget = ReplayBudget::new(Some(2)).unwrap();
    let first = ReplaySaltPool::configured_with_budget(
        ReplaySaltPool::DEFAULT_TTL,
        Some(2),
        budget.clone(),
    );
    let second = ReplaySaltPool::configured_with_budget(
        ReplaySaltPool::DEFAULT_TTL,
        Some(2),
        budget.clone(),
    );
    first.check_and_insert(&[1; 16]).unwrap();
    second.check_and_insert(&[2; 16]).unwrap();
    let original = Arc::downgrade(&first.inner);
    drop(first);
    assert_eq!(budget.retained(), 2);
    assert!(
        original.upgrade().is_some(),
        "valid retired history has a real owner"
    );
    assert!(second.check_and_insert(&[3; 16]).is_err());
    assert!(
        second.with_total(Some(1)).is_err(),
        "shrink cannot evict valid history"
    );
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(budget.retained(), 2);
    assert!(second.check_and_insert(&[3; 16]).is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(budget.retained(), 0);
    assert!(
        original.upgrade().is_none(),
        "only the actual expired book was released"
    );
    let smaller = second.with_total(Some(1)).unwrap();
    smaller.install_total().unwrap();
    smaller.check_and_insert(&[3; 16]).unwrap();
    assert!(smaller.check_and_insert(&[4; 16]).is_err());
    assert_eq!(budget.retained(), 1);
}

#[tokio::test(start_paused = true)]
async fn cap_and_policy_views_keep_exact_membership_and_charge_one_actual_total() {
    let budget = ReplayBudget::new(Some(3)).unwrap();
    let pool = ReplaySaltPool::configured_with_budget(
        ReplaySaltPool::DEFAULT_TTL,
        Some(1),
        budget.clone(),
    );
    let same = pool.alias(Some(1));
    let larger = pool.alias(Some(2));
    pool.check_and_insert(&[1; 16]).unwrap();
    assert!(
        same.check_and_insert(&[2; 16]).is_err(),
        "clone cannot fork its configured bucket"
    );
    assert!(
        larger.check_and_insert(&[1; 16]).is_err(),
        "capacity change cannot forget a live salt"
    );
    larger.check_and_insert(&[2; 16]).unwrap();
    larger.check_and_insert(&[3; 16]).unwrap();
    assert!(larger.check_and_insert(&[4; 16]).is_err());
    let unbounded_local = pool.alias(None);
    assert!(
        unbounded_local.check_and_insert(&[4; 16]).is_err(),
        "omitted local cap cannot bypass adapter total"
    );
    assert_eq!(budget.retained(), 3);
    drop(larger);
    drop(same);
    assert!(pool.check_and_insert(&[1; 16]).is_err());
    let independent = ReplaySaltPool::configured_with_budget(
        ReplaySaltPool::DEFAULT_TTL,
        None,
        ReplayBudget::new(Some(1)).unwrap(),
    );
    independent.check_and_insert(&[1; 16]).unwrap();
    tokio::time::advance(Duration::from_secs(61)).await;
    unbounded_local.check_and_insert(&[4; 16]).unwrap();
    assert_eq!(budget.retained(), 1);
}
