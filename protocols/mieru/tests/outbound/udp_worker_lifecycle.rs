use super::*;

#[tokio::test]
async fn terminated_worker_closes_existing_and_new_response_waiters() {
    let (send_tx, send_rx) = mpsc::channel(1);
    let (worker_responses, _) = broadcast::channel::<MieruUdpFlowResponse>(1);
    let connection = MieruUdpFlowConnection::new(MieruUdpFlowSession::new(MieruUdpFlowHandle {
        sender: MieruUdpFlowSender { send_tx },
        responses: worker_responses.downgrade(),
    }));
    let mut waiting = connection.subscribe_responses();
    assert!(!connection.is_closed());
    drop(send_rx);
    drop(worker_responses);
    assert!(connection.is_closed());
    assert!(matches!(
        waiting.recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
    assert!(matches!(
        connection.subscribe_responses().recv().await,
        Err(broadcast::error::RecvError::Closed)
    ));
}
