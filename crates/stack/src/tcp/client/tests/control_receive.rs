//! Existing real Stack workers and actual SYN/cache; no fabricated SDK commit.
use super::*;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    sync::Mutex as StdMutex,
    task::Poll,
};
use tokio::task::JoinSet;
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
struct Scope(StdMutex<JoinSet<()>>);
impl crate::TcpTaskOwner for Scope {
    fn spawn(&self, worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        self.0.lock().unwrap().spawn(worker);
        true
    }
}
#[tokio::test]
async fn dropping_actual_tuple_guard_keeps_handshake_pending_until_real_consume() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let limit = PacketMemoryLimit {
            frames: 4,
            bytes: 4096,
        };
        let root = Arc::new(PacketReturnTotals::new_with_transmit(
            limit,
            PacketMemoryLimit {
                frames: 2,
                bytes: usize::from(u16::MAX) + 128,
            },
            limit,
        ));
        let read = root.native_grant(128).unwrap();
        let prepaid_control = root.transmit_workspace(148).unwrap();
        let (send, mut packets) = tokio::sync::mpsc::channel::<zero_traits::PacketBuffer>(8);
        let send = crate::packet_output::PacketSender::from(send)
            .with_packet_memory_view(
                read.memory_view().unwrap(),
                crate::packet_output::memory::PacketWireMemory::PaddedEnvelope {
                    multiple: 16,
                    overhead: 32,
                },
            )
            .unwrap();
        let scope = Arc::new(Scope(StdMutex::new(JoinSet::new())));
        let local: IpAddr = "10.0.0.1".parse().unwrap();
        let remote: SocketAddr = "10.0.0.2:80".parse().unwrap();
        let client = Arc::new(
            ClientTcpStack::new_output_with_task_owner(vec![local], send, 1420, scope.clone())
                .unwrap(),
        );
        let opening = client.prepare_connect(local, remote, None);
        let original = opening.control();
        let connecting = opening.connect();
        tokio::pin!(connecting);
        let syn = tokio::select! {
            result=&mut connecting=>panic!("no peer SYNACK yet: {}",result.is_ok()),
            packet=packets.recv()=>packet.unwrap(),
        };
        let header = packet::parse_tcp(&syn).unwrap();
        let synack = packet::build_tcp(
            remote.ip(),
            local,
            remote.port(),
            header.src.port,
            500,
            header.seq.wrapping_add(1),
            tcp_flags::SYN | tcp_flags::ACK,
            &[],
        );
        drop(syn);
        assert_eq!(root.transmit().occupied.frames, 4);
        let prepared = client.prepare_correlated_control(&synack).await.unwrap();
        assert!(
            client.connections.try_lock().is_err(),
            "the original tuple is fenced, not an ID hint"
        );
        drop(prepared); // canceled authentication/admission has no Stack effect
        poll_fn(|cx| {
            assert!(connecting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(root.transmit().occupied.frames, 4);
        let mixed = packet::build_tcp(
            remote.ip(),
            local,
            remote.port(),
            packet::parse_tcp(&synack).unwrap().dst.port,
            500,
            packet::parse_tcp(&synack).unwrap().ack,
            tcp_flags::SYN | tcp_flags::ACK,
            b"payload",
        );
        assert!(
            client.prepare_correlated_control(&mixed).await.is_none(),
            "pure-control guard does not qualify mixed data"
        );
        let prepared = client.prepare_correlated_control(&synack).await.unwrap();
        assert!(prepared.consume()); // actual caller must commit SDK first
        let mut stream = connecting.await.unwrap();
        assert!(
            root.transmit().occupied.frames < 4,
            "actual peer ACK releases original SYN cache"
        );
        // A mixed ACK carrying actual application data also progresses with
        // all four TX credits occupied. RX lives on the same Native lane and
        // its original capacity remains charged through reads and connection retirement.
        let used = root.transmit().occupied.frames;
        let held = root.reserve_transmit(&vec![1; 4 - used]).unwrap();
        assert_eq!(root.transmit().occupied.frames, 4);
        let mixed = packet::build_tcp(
            remote.ip(),
            local,
            remote.port(),
            packet::parse_tcp(&synack).unwrap().dst.port,
            501,
            packet::parse_tcp(&synack).unwrap().ack,
            tcp_flags::ACK,
            b"reply",
        );
        let prepared = client
            .prepare_correlated_owned(&mixed, &read)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(root.native().occupied.frames, 2);
        drop(prepared); // no commit, no app visibility
        assert_eq!(
            root.native().occupied.frames,
            2,
            "same original receive ring remains allocated after canceled preparation"
        );
        use tokio::io::AsyncRead;
        poll_fn(|cx| {
            let mut byte = [0; 1];
            let mut buffer = tokio::io::ReadBuf::new(&mut byte);
            assert!(Pin::new(&mut stream)
                .poll_read(cx, &mut buffer)
                .is_pending());
            Poll::Ready(())
        })
        .await;
        let prepared = client
            .prepare_correlated_owned(&mixed, &read)
            .await
            .unwrap()
            .unwrap();
        assert!(prepared.consume());
        assert_eq!(root.native().occupied.frames, 2);
        use tokio::io::AsyncReadExt;
        let mut bytes = [0; 5];
        stream.read_exact(&mut bytes[..1]).await.unwrap();
        assert_eq!(
            root.native().occupied.frames,
            2,
            "partial app read retains whole actual backing"
        );
        stream.read_exact(&mut bytes[1..]).await.unwrap();
        assert_eq!(&bytes, b"reply");
        assert_eq!(
            root.native().occupied.frames,
            2,
            "emptying the ring is not freeing its actual backing"
        );
        drop(held);
        drop(stream);
        let closed = original.close().await;
        assert!(closed.matches(&original));
        client.shutdown().await;
        drop(client);
        drop(packets);
        let mut tasks = std::mem::take(&mut *scope.0.lock().unwrap());
        while let Some(joined) = tasks.join_next().await {
            joined.unwrap();
        }
        drop(prepaid_control);
        drop(read);
        assert_eq!(root.transmit().occupied.frames, 0);
        assert_eq!(root.native().occupied.frames, 0);
    })
    .await
    .unwrap();
}
#[path = "receive_capacity.rs"]
mod receive_capacity;
