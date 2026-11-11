//! Actual standard MUX producers share one native owner and retain its receipts.
#![cfg(feature = "runtime")]
#[path = "mux/fixture.rs"]
mod fixture;
use fixture::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_core::{
    component::AssociationId,
    packet::Datagram,
    request::{DatagramTargets, Destination},
    Address,
};
use zero_platform_tokio::contract::{
    datagram::{recv, send, DatagramRequest},
    stream::ConnectRequest,
    CloseReason,
};

async fn echo(listener: tokio::net::TcpListener) {
    let (io, _) = listener.accept().await.unwrap();
    let profile = vmess::inbound::VmessInboundProfile::from_users(vec![
        vmess::inbound::VmessUser::from_config(
            ID,
            "aes-128-gcm",
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap(),
    ]);
    let (_, mut io) = profile
        .accept_request_stream(
            vmess::inbound::VmessInbound,
            zero_platform_tokio::TcpRelayStream::new(io),
        )
        .await
        .unwrap();
    loop {
        let length = match io.read_u16().await {
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("{error}"),
        };
        let mut meta = vec![0; length as usize];
        io.read_exact(&mut meta).await.unwrap();
        assert!(meta.len() >= 4);
        if meta[3] & 1 == 0 {
            continue;
        }
        let length = io.read_u16().await.unwrap();
        let mut payload = vec![0; length as usize];
        io.read_exact(&mut payload).await.unwrap();
        // Mux.Cool KEEP/DATA carries the exact submitted TCP or encoded UDP
        // body. Framing is independent from the producer's accounting report.
        io.write_all(&[0, 4, meta[0], meta[1], 2, 1]).await.unwrap();
        io.write_all(&length.to_be_bytes()).await.unwrap();
        io.write_all(&payload).await.unwrap();
        io.flush().await.unwrap();
    }
}
#[tokio::test]
async fn standard_mux_datagram_preserves_encoded_accounting_on_the_same_tcp_original_and_confirms_cleanup(
) {
    tokio::time::timeout(std::time::Duration::from_secs(12), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let peer = listener.local_addr().unwrap();
        let server = tokio::spawn(echo(listener));
        let mut actual = Assembly::start(peer).await;
        let stream_role = actual.role.roles.stream.clone().unwrap();
        let mut stream = stream_role.connect(ConnectRequest { destination: Destination::new(Address::Ipv4([1, 1, 1, 1]), 443) }, actual.context()).await.unwrap();
        stream.io.write_all(b"tcp").await.unwrap();stream.io.flush().await.unwrap();
        let mut echoed = [0; 3];stream.io.read_exact(&mut echoed).await.unwrap();assert_eq!(&echoed, b"tcp");
        let view = actual.registration.observe().unwrap();assert_eq!(view.retained(), 1);
        let physical = view.slots()[0].original().clone();
        let driver = view.slots()[0].driver().unwrap().1.clone();
        let role = actual.role.roles.datagram.clone().unwrap();
        let target = Destination::new(Address::Ipv4([1, 1, 1, 1]), 53);
        let mut session = role.open(DatagramRequest { association: AssociationId(4), targets: DatagramTargets::Fixed(target.clone()) }, actual.context()).await.unwrap();
        for payload in [b"".as_slice(), b"data".as_slice(), b"again".as_slice()] {
            let frame = Datagram { association: AssociationId(4), address: target.clone(), payload: payload.to_vec().into() };
            let sent = send(session.tx.as_mut(), session.control.as_mut(), &frame, &session.limits).await.unwrap();
            assert_eq!(sent.accepted_bytes, payload.len());
            let received = recv(session.rx.as_mut()).await.unwrap().unwrap();assert_eq!(received.payload.as_ref(), payload);
        }
        assert_eq!(*actual.counts.0.lock().unwrap(), [(None, 9), (None, 13), (None, 14)], "one protocol frame observation per successful packet; business receipts remain payload-only");
        let same = actual.registration.observe().unwrap();assert_eq!(same.retained(), 1);
        assert!(same.slots()[0].original().same_original(&physical));
        assert!(same.slots()[0].driver().unwrap().1.same_registration(&driver));
        session.control.cancel(CloseReason::Shutdown);
        let pending = session.control.wait_closed(tokio::time::Instant::now() + std::time::Duration::from_millis(20)).await.unwrap();assert!(!pending.is_complete());
        drop(session.tx);drop(session.rx);
        assert!(session.control.wait_closed(end()).await.unwrap().is_complete());drop(session.control);
        assert!(physical.closed().is_none(), "logical return never proves the shared native original closed");
        assert!(!driver.is_joined());
        stream.io.shutdown().await.unwrap();drop(stream);
        actual.role.lifecycle.revoke(CloseReason::Shutdown);actual.pool.lifecycle.revoke(CloseReason::Shutdown);
        let (protocol, tasks) = tokio::join!(actual.pool.lifecycle.shutdown(end()), actual.scope.shutdown(end()));
        assert!(protocol.unwrap().is_complete());assert!(tasks.is_complete());
        assert!(actual.role.lifecycle.shutdown(end()).await.unwrap().is_complete());
        assert!(actual.role_scope.shutdown(end()).await.is_complete());
        assert!(physical.matches_closed(&physical.closed().unwrap()));
        assert!(driver.matches_joined(&driver.try_joined().unwrap()));
        assert!(actual.registration.closed().unwrap().matches(&actual.registration));
        server.await.unwrap();
    }).await.unwrap();
}
