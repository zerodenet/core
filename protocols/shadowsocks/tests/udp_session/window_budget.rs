use super::*;

#[tokio::test(start_paused = true)]
async fn receiver_total_preserves_retired_original_bitmaps_and_reclaims_only_original_expiry() {
    let budget = UdpReplayBudget::new(2).unwrap();
    let limits = crate::validation::StateLimits {
        udp_timeout_secs: 1,
        ..Default::default()
    };
    let mut inbound = ReceiveWindows::with_budget(limits, budget.clone());
    let mut outbound = ReceiveWindows::with_budget(limits, budget.clone());
    assert!(inbound.accept(11, 1).unwrap());
    assert!(outbound.accept(22, 1).unwrap());
    drop(inbound);
    assert_eq!(budget.retained(), 2);
    assert!(outbound.accept(33, 1).is_err());
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(
        budget.retained(),
        2,
        "short native idle does not shorten SIP022 security retention"
    );
    assert!(!outbound.accept(22, 1).unwrap());
    assert!(outbound.accept(33, 1).is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(budget.retained(), 0);
    assert!(outbound.accept(33, 1).unwrap());
    assert_eq!(budget.retained(), 1);
}

#[tokio::test(start_paused = true)]
async fn receiver_view_change_does_not_erase_old_bitmap_or_shorten_its_original_deadline() {
    let original_limits = crate::validation::StateLimits {
        udp_capacity: Some(2),
        ..Default::default()
    };
    let mut original = ReceiveWindows::with_limits(original_limits);
    assert!(original.accept(7, 17).unwrap());
    let mut smaller = original
        .clone()
        .with_limits_view(crate::validation::StateLimits {
            udp_capacity: Some(1),
            udp_timeout_secs: 1,
            ..Default::default()
        });
    assert!(!smaller.accept(7, 17).unwrap());
    assert!(smaller.accept(8, 1).is_err());
    tokio::time::advance(Duration::from_secs(61)).await;
    assert!(
        !smaller.accept(7, 17).unwrap(),
        "original configured300s still protects accepted packet"
    );
    assert!(smaller.accept(8, 1).is_err());
    tokio::time::advance(Duration::from_secs(239)).await;
    assert!(smaller.accept(8, 1).unwrap());
}

#[tokio::test(start_paused = true)]
async fn actual_request_and_response_senders_share_one_total_without_restarting_nonce_authority() {
    let caps = UdpBudgetLimits {
        senders: 1,
        ..Default::default()
    };
    let budget = UdpReplayBudget::with_limits(caps).unwrap();
    let mut request = SenderSession::default();
    request.use_budget(&budget).unwrap();
    let original = request.next().unwrap();
    let mut replies = ResponseSessions::with_budget(Default::default(), budget.clone());
    assert!(
        replies.next(7).is_err(),
        "inbound/outbound cannot each spend the whole adapter cap"
    );
    assert_eq!(request.next().unwrap(), (original.0, original.1 + 1));
    assert_eq!(budget.occupancy()[1], 1);
    drop(request);
    let response = replies.next(7).unwrap();
    assert_eq!(budget.occupancy()[1], 1);
    let mut view = replies.clone();
    view.set_limits(crate::validation::StateLimits {
        udp_capacity: Some(1),
        udp_timeout_secs: 1,
        ..Default::default()
    });
    assert_eq!(view.next(7).unwrap(), (response.0, response.1 + 1));
    assert!(
        view.next(8).is_err(),
        "the local bucket and global cap are distinct"
    );
    drop(view);
    drop(replies);
    assert_eq!(
        budget.occupancy()[1],
        1,
        "dropping a view is not original sender expiry"
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        budget.occupancy()[1],
        1,
        "a shorter view cannot shorten the original default deadline"
    );
    tokio::time::advance(Duration::from_secs(299)).await;
    assert_eq!(budget.occupancy()[1], 0);
}

#[tokio::test(start_paused = true)]
async fn a_cold_udp_budget_shrink_preserves_actual_debt_and_does_not_install_on_validation() {
    let caps = UdpBudgetLimits {
        receiver_windows: 2,
        ..Default::default()
    };
    let budget = UdpReplayBudget::with_limits(caps).unwrap();
    let mut first = ReceiveWindows::with_budget(Default::default(), budget.clone());
    let mut second = ReceiveWindows::with_budget(Default::default(), budget.clone());
    assert!(first.accept(1, 1).unwrap());
    assert!(second.accept(2, 1).unwrap());
    drop(first);
    drop(second);
    assert!(budget
        .validate(UdpBudgetLimits {
            receiver_windows: 1,
            ..caps
        })
        .is_err());
    assert_eq!(budget.retained(), 2);
    let mut third = ReceiveWindows::with_budget(Default::default(), budget.clone());
    budget
        .validate(UdpBudgetLimits {
            receiver_windows: 3,
            ..caps
        })
        .unwrap();
    assert!(
        third.accept(3, 1).is_err(),
        "cold growth is no actual-owner start"
    );
    budget
        .install(UdpBudgetLimits {
            receiver_windows: 3,
            ..caps
        })
        .unwrap();
    assert!(third.accept(3, 1).unwrap());
    assert_eq!(budget.retained(), 3);
}
