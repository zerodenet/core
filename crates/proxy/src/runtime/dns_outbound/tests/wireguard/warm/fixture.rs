//! Real accepted TCP and original Query registration observations.
use super::*;
use tokio::net::TcpStream;
pub(super) async fn connect(entry: std::net::SocketAddr, port: u16) -> TcpStream {
    let mut io = TcpStream::connect(entry).await.unwrap();
    io.write_all(&[5, 1, 0]).await.unwrap();
    let mut auth = [0; 2];
    io.read_exact(&mut auth).await.unwrap();
    assert_eq!(auth, [5, 0]);
    let mut request = vec![5, 1, 0, 1, 10, 0, 0, 2];
    request.extend_from_slice(&port.to_be_bytes());
    io.write_all(&request).await.unwrap();
    let mut answer = [0; 10];
    io.read_exact(&mut answer).await.unwrap();
    assert_eq!(&answer[..4], &[5, 0, 0, 1]);
    io
}
pub(super) async fn live(front: &mut TcpStream, remote: &mut TcpStream) {
    tokio::time::timeout_at(end(), async {
        front.write_all(b"live").await.unwrap();
        let mut bytes = [0; 4];
        remote.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"live");
        remote.write_all(b"same").await.unwrap();
        remote.flush().await.unwrap();
        front.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"same");
    })
    .await
    .unwrap();
}
pub(super) async fn idle(
    proxy: &Proxy,
    driver: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    queries: Vec<zero_platform_tokio::contract::scope::TaskJoinWitness>,
) {
    for task in queries {
        if task.same_registration(driver) {
            continue;
        }
        let joined = tokio::time::timeout_at(end(), task.wait_registration_released())
            .await
            .unwrap();
        assert!(task.matches_joined(&joined) && joined.registration_released());
        joined.result().unwrap();
    }
    tokio::time::timeout_at(end(), async {
        loop {
            if zero_dns::test_support::doh_pool_witness(&proxy.resolver, "query")
                .unwrap()
                .active_leases
                == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
pub(super) fn returned_driver(actual: &zero_dns::OwnedDnsQueryObservation) {
    returned(actual.original.as_ref().unwrap());
    let joined = actual.task.try_joined().unwrap();
    assert!(actual.task.matches_joined(&joined) && joined.registration_released());
    joined.result().unwrap();
}
