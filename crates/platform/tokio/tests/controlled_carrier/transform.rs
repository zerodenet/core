use super::*;
use std::future::Future;
#[tokio::test]
async fn cancelled_loan_transform_returns_same_custody_before_original_native_close() {
    timeout(Duration::from_secs(8), async {
        let mut fixture = Fixture::cold(1).await;
        let mut scope = TaskScope::empty(tokio::runtime::Handle::current());
        let cancel = Arc::new(CancellationToken::new());
        fixture
            .owner
            .registration()
            .bind_declared(
                &crate::contract::component::ResourceDeclaration::ControlledNodeCarrier {
                    endpoint: fixture.destination.clone(),
                    dial: fixture.transport.binding().dial.clone(),
                    max_sockets: NonZeroUsize::new(1).unwrap(),
                },
                &scope.registrar(),
                &cancel,
            )
            .unwrap();
        fixture.owner.start(deadline()).await.unwrap();
        let mut grant = fixture.grants.reserve().unwrap();
        let view = grant.claim_transferred_view().unwrap();
        let original = grant.adoption();
        let connected = fixture
            .transport
            .prepare_controlled_stream(fixture.destination.clone(), context(), grant)
            .unwrap()
            .open(deadline())
            .await
            .unwrap();
        let (_peer, _) = fixture.listener.accept().await.unwrap();
        let loan = connected.into_parts().0.into_stream_loan(view).unwrap();
        let mut owner = loan.owner;
        let origin = owner.original().clone();
        let mut pending = Box::pin(loan.io.try_map(|mut io| async move {
            let mut byte = [0];
            io.read_exact(&mut byte).await?;
            Ok::<_, std::io::Error>(io)
        }));
        std::future::poll_fn(|cx| {
            assert!(
                pending.as_mut().poll(cx).is_pending(),
                "real peer withholds the transform's handshake byte"
            );
            std::task::Poll::Ready(())
        })
        .await;
        assert!(!owner.view_returned());
        assert!(original.closed().is_none());
        drop(pending);
        assert!(owner.view_returned());
        assert!(owner.original().same_original(&origin));
        assert!(
            original.closed().is_none(),
            "view return grants no native close evidence"
        );
        assert!(owner.close(deadline()).await.unwrap().is_complete());
        assert!(original.closed().unwrap().matches(&original));
        assert!(origin.closed().unwrap().matches(&origin));
        assert!(fixture
            .owner
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        assert!(scope.shutdown(deadline()).await.is_complete());
    })
    .await
    .unwrap();
}
