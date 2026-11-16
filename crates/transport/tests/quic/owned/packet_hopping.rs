//! Two actual native sockets, one queue identity, no timer or Drop close proof.
use super::*;
use crate::quic::owned::packet::{PacketHopMemory, PacketSegment, PacketSocket};
use std::{io::IoSliceMut, time::Duration};
use zero_platform_tokio::contract::{component::GrantedMetadataListener, scope::TaskScope};

#[tokio::test]
async fn native_hop_sends_current_receives_both_and_retains_old_original_until_loan_returns() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), bound(2));
        let tasks = scope.registrar();
        let old_peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let new_peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let logical = old_peer.local_addr().unwrap();
        let new_remote = new_peer.local_addr().unwrap();
        let mut old_native = native_owner();
        let mut new_native = native_owner();
        let mut old = old_native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut new = new_native.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let old_local = old.tx.local_addr().unwrap();
        let new_local = new.tx.local_addr().unwrap();
        let (old_issuer, old_claim) = old.control.claim_views(tasks.scope_identity()).unwrap();
        let (new_issuer, new_claim) = new.control.claim_views(tasks.scope_identity()).unwrap();
        let book = zero_platform_tokio::controlled_carrier::CarrierOriginalTotals::new(bound(2))
            .buffer_budget();
        let memory = PacketHopMemory::prepare(book.clone()).unwrap();
        let (socket, queues) = PacketSocket::prepare_with_budget(logical, book, &[]).unwrap();
        let mut first = PacketSegment::prepare(
            GrantedMetadataListener {
                scope: tasks.scope_identity(),
                rx: old.rx,
                tx: old.tx,
                observation: old.control.take_observation().unwrap(),
                users_claim: Some(old_claim),
            },
            queues,
            tasks.clone(),
            logical,
            logical,
        )
        .unwrap_or_else(|_| panic!("first original"));
        first.start().unwrap();
        let (paused, exit) = tokio::join!(first.pause_for_hop_original(), scope.next_exit());
        paused.unwrap();
        exit.unwrap().result.unwrap();
        let (queues, loan) = first
            .handoff_for_hop(memory)
            .unwrap_or_else(|_| panic!("joined current"));
        assert!(
            first.returned_wire_receipt().is_none(),
            "transferred receive loan is still a user"
        );
        let mut second = PacketSegment::prepare(
            GrantedMetadataListener {
                scope: tasks.scope_identity(),
                rx: new.rx,
                tx: new.tx,
                observation: new.control.take_observation().unwrap(),
                users_claim: Some(new_claim),
            },
            queues,
            tasks.clone(),
            new_remote,
            logical,
        )
        .unwrap_or_else(|_| panic!("second original"));
        second
            .attach_previous_wire(loan)
            .unwrap_or_else(|_| panic!("same unique queues"));
        second.start().unwrap();
        socket
            .socket
            .try_send(&quinn::udp::Transmit {
                destination: logical,
                ecn: None,
                contents: b"current sends",
                segment_size: None,
                src_ip: None,
            })
            .unwrap();
        let mut sent = [0; 64];
        let (size, source) = new_peer.recv_from(&mut sent).await.unwrap();
        assert_eq!(&sent[..size], b"current sends");
        assert_eq!(source, new_local);
        for (peer, local, expected) in [
            (&old_peer, old_local, b"old receive".as_slice()),
            (&new_peer, new_local, b"new receive".as_slice()),
        ] {
            peer.send_to(expected, local).await.unwrap();
            let mut bytes = [0; 128];
            let mut buffers = [IoSliceMut::new(&mut bytes)];
            let mut meta = [quinn::udp::RecvMeta::default()];
            let count =
                std::future::poll_fn(|cx| socket.socket.poll_recv(cx, &mut buffers, &mut meta))
                    .await
                    .unwrap();
            assert_eq!(count, 1);
            assert_eq!(meta[0].addr, logical);
            assert_eq!(&bytes[..meta[0].len], expected);
        }
        assert!(first.returned_wire_receipt().is_none());
        assert_eq!(old_native.occupied_main(), 1);
        let (retired, exit) = tokio::join!(second.retire_final_original(), scope.next_exit());
        exit.unwrap().result.unwrap();
        let (_queues, new_receipt) = retired.unwrap();
        let old_receipt = first
            .returned_wire_receipt()
            .expect("previous view and real lease returned");
        assert!(old_issuer.validates_users(&old_receipt, &tasks));
        assert_eq!(old_receipt.related_registrations().len(),1,
            "old original retains the actual next-wire receive driver, not only its own stopped worker");
        assert!(new_issuer.validates_users(&new_receipt, &tasks));
        // User/driver receipts are still distinct from the actual native close.
        assert_eq!(old_native.occupied_main(), 1);
        assert_eq!(new_native.occupied_main(), 1);
        let old_closed = old.control.close().unwrap();
        let new_closed = new.control.close().unwrap();
        assert_eq!(old_closed.reservation(), old.control.reservation());
        assert_eq!(new_closed.reservation(), new.control.reservation());
        assert_eq!(old_native.take_report().main.confirmed_closed, 1);
        assert_eq!(new_native.take_report().main.confirmed_closed, 1);
        scope.begin_shutdown();
        assert!(scope
            .shutdown(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .is_complete());
    })
    .await
    .unwrap();
}
