use super::*;
#[tokio::test]
async fn system_answer_requires_the_actual_blocking_child_join_in_the_same_scope() {
    let mut scope = TaskScope::new(Handle::current(), NonZeroUsize::new(2).unwrap());
    let tasks = OwnedDnsQueryTasks::new_critical(&scope.registrar(), NonZeroUsize::new(1).unwrap());
    let actual = tasks.clone();
    let (completed, mut answer) = oneshot::channel();
    tasks
        .spawn(
            async move {
                let result = actual
                    .lookup_system(
                        "127.0.0.1".into(),
                        OwnedDnsQueryTasks::current_witness().unwrap(),
                        CancellationToken::new(),
                        Instant::now() + Duration::from_secs(2),
                    )
                    .await;
                let _ = completed.send(result);
            },
            CancellationToken::new(),
        )
        .unwrap();
    let mut joined = Vec::new();
    let lookup = loop {
        tokio::select! {biased;
            result = &mut answer => break result.unwrap().unwrap(),
            exit = scope.next_exit() => joined.push(exit.unwrap().id),
        }
    };
    assert!(lookup
        .result
        .unwrap()
        .contains(&zero_traits::IpAddress::V4([127, 0, 0, 1])));
    assert!(
        joined.contains(&lookup.joined.id()),
        "the actual blocking child, not just the result sender, was joined"
    );
    assert!(lookup.joined.result().is_ok());
    assert!(
        tasks
            .observe()
            .iter()
            .all(|record| record.original.is_none() && record.metadata.is_none()),
        "OS resolver task completion is not a fabricated native socket receipt"
    );
    let (closed, report) = tokio::join!(
        tasks.close(Instant::now() + Duration::from_secs(2)),
        scope.shutdown(Instant::now() + Duration::from_secs(2))
    );
    closed.unwrap();
    assert!(report.is_complete());
}
