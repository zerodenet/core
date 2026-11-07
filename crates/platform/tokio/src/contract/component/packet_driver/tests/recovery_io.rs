use super::*;
pub(super) async fn malformed_actor(
    root: &Arc<PacketDriverCalls>,
    tasks: &crate::contract::scope::TaskRegistrar,
    wire: &TaskJoinWitness,
) -> TaskJoinWitness {
    let (actor, mut malformed) = pending_actor(root, tasks, wire).await;
    malformed.write_all(b"no").await.unwrap();
    actor
}
pub(super) async fn pending_actor(
    root: &Arc<PacketDriverCalls>,
    tasks: &crate::contract::scope::TaskRegistrar,
    wire: &TaskJoinWitness,
) -> (TaskJoinWitness, tokio::io::DuplexStream) {
    let (malformed, mut input) = tokio::io::duplex(8);
    let (ready, pending) = tokio::sync::oneshot::channel();
    let (_, actor) = root
        .spawn_reconnect(&tasks.downgrade(), wire, move |_| async move {
            let mut bytes = [0; 2];
            let reading = input.read_exact(&mut bytes);
            tokio::pin!(reading);
            poll_fn(|cx| {
                assert!(reading.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            ready.send(()).unwrap();
            reading
                .await
                .map_err(|e| failure(Reason::Io).with_source(e))?;
            if bytes != *b"ok" {
                return Err(failure(Reason::Protocol).with_source(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "actual malformed actor reply",
                )));
            }
            Ok(())
        })
        .unwrap();
    pending.await.unwrap();
    (actor, malformed)
}
