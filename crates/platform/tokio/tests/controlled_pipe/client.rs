//! Both ends use the real provider; no regular-file client or Drop close.
use super::*;
use zero_platform_tokio::contract::scope::HostedScope;
fn hosted() -> HostedScope {
    TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1))
}
#[tokio::test]
async fn explicit_client_keeps_both_actual_pipe_originals_and_hosted_ack_separate() {
    let name = path();
    let mut server_scope = hosted();
    server_scope.enable_registration().unwrap();
    let mut client_scope = hosted();
    client_scope.enable_registration().unwrap();
    let server = ControlledPipeOwner::new(name.clone(), server_scope.registrar());
    let mut server_io = server.open().await.unwrap();
    pending(&server, 0).await;
    let client = ControlledPipeOwner::new_client(name, client_scope.registrar());
    let mut client_io = client.open().await.unwrap();
    client_io.connect().await.unwrap();
    server_io.connect().await.unwrap();
    client_io.write_all(b"same-request").await.unwrap();
    client_io.flush().await.unwrap();
    let mut request = [0; 12];
    server_io.read_exact(&mut request).await.unwrap();
    assert_eq!(&request, b"same-request");
    server_io.write_all(b"same-response").await.unwrap();
    server_io.flush().await.unwrap();
    let mut response = [0; 13];
    client_io.read_exact(&mut response).await.unwrap();
    assert_eq!(&response, b"same-response");
    pending(&client, 1).await;
    {
        let read = client_io.read_u8();
        tokio::pin!(read);
        tokio::select! {biased;result=&mut read=>panic!("real client read must be Pending: {result:?}"),_=tokio::task::yield_now()=>{}}
    }
    let witness = client.worker().unwrap();
    assert!(!witness.is_joined());
    assert!(client.closed().is_none());
    client.request_stop();
    drop(client_io);
    client_scope.begin_shutdown();
    let receipt = tokio::time::timeout_at(end(), client_scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let joined = receipt.joined().clone();
    let result = receipt.result();
    assert!(result.is_ok(), "{result:?}");
    assert!(witness.matches_joined(&joined));
    assert!(client.std_worker_joined());
    assert!(!joined.registration_released());
    assert!(!client_scope.is_drained());
    let closed = client.close(end()).await.unwrap();
    assert!(client.matches_closed(&closed) && closed.had_native_acquisition());
    assert!(!server.matches_closed(&closed));
    assert!(
        !joined.registration_released(),
        "native close is not Host ACK"
    );
    client_scope.acknowledge(&receipt).unwrap();
    assert!(joined.registration_released() && client_scope.is_drained());
    server.request_stop();
    drop(server_io);
    server_scope.begin_shutdown();
    let receipt = tokio::time::timeout_at(end(), server_scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let original = receipt.joined().clone();
    let _actual_result = receipt.result();
    let closed = server.close(end()).await.unwrap();
    assert!(server.matches_closed(&closed) && server.std_worker_joined());
    assert!(!original.registration_released());
    server_scope.acknowledge(&receipt).unwrap();
    assert!(server_scope.is_drained());
}
#[tokio::test]
async fn client_rejects_disk_namespace_but_retains_partial_real_event_handles_until_explicit_close()
{
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), b"unchanged").unwrap();
    let mut scope = hosted();
    scope.enable_registration().unwrap();
    let client = ControlledPipeOwner::new_client(file.path().into(), scope.registrar());
    assert!(client.closed().is_none());
    assert!(client.open().await.is_err());
    scope.begin_shutdown();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    let original = receipt.joined().clone();
    let source = receipt
        .result()
        .expect_err("actual namespace rejection reaches original worker");
    assert!(!original.registration_released());
    let closed = client.close(end()).await.unwrap();
    assert!(
        client.matches_closed(&closed) && closed.had_native_acquisition(),
        "the four real CreateEvent handles must not be called NoNative"
    );
    assert_eq!(std::fs::read(file.path()).unwrap(), b"unchanged");
    assert!(std::error::Error::source(source.as_ref()).is_some());
    scope.acknowledge(&receipt).unwrap();
    assert!(original.registration_released() && scope.is_drained());
}
