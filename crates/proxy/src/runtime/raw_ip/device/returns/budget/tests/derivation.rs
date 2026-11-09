//! Derived payloads are debited before queue admission; queue heads never self-wait.
use super::*;
use std::{future::Future, num::NonZeroUsize, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskScope},
    ChannelError, Impact, Reason, Stage,
};
fn frame() -> PacketBuffer {
    zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "192.0.2.1".parse().unwrap(),
        40000,
        443,
        &[9; 16_000],
    )
    .into()
}
fn counts(packet: &PacketBuffer) -> (usize, usize, Vec<usize>) {
    let lengths = zero_stack::packet::fragment_packet_lengths(packet, 1500).unwrap();
    (
        1 + lengths.len(),
        packet.len() + lengths.iter().sum::<usize>(),
        lengths,
    )
}
#[tokio::test]
async fn permanently_impossible_derivation_returns_same_original_without_debit_or_allocation() {
    let original = frame();
    let pointer = original.as_ptr();
    let budget = ReturnBudget::new(1, 1_000_000);
    let rejected = budget.retain_for_return(original, 1500).unwrap_err();
    assert_eq!(
        rejected.cause,
        Some(zero_traits::PacketDeriveError::Capacity)
    );
    assert_eq!(rejected.original.as_ptr(), pointer);
    assert_eq!(*budget.0.occupied.lock().unwrap(), (0, 0));
}
#[tokio::test]
async fn temporary_derivation_pressure_keeps_original_and_waits_for_actual_old_buffers_to_return() {
    let original = frame();
    let pointer = original.as_ptr();
    let (frames, bytes, _) = counts(&original);
    let budget = ReturnBudget::new(frames, bytes);
    let occupied = budget.retain_for_return(frame(), 1500).unwrap();
    let pending = budget.retain_for_return_wait(original, 1500);
    tokio::pin!(pending);
    assert!(futures_util::poll!(&mut pending).is_pending());
    assert_eq!(*budget.0.occupied.lock().unwrap(), (frames, bytes));
    drop(occupied);
    let original = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.as_ptr(), pointer);
    assert_eq!(*budget.0.occupied.lock().unwrap(), (frames, bytes));
    drop(original);
    assert_eq!(*budget.0.occupied.lock().unwrap(), (0, 0));
}
#[tokio::test]
async fn full_actual_return_queue_derives_without_extra_credit_and_holds_original_through_native_send_pending(
) {
    let (frames, bytes, lengths) = counts(&frame());
    let root = Arc::new(PacketReturnTotals::default());
    let layout = zero_stack::packet_output::memory::PacketWireMemory::PaddedEnvelope {
        multiple: 16,
        overhead: 32,
    };
    let codec = lengths
        .iter()
        .flat_map(|length| layout.workspace_lengths(*length).unwrap())
        .collect::<Vec<_>>();
    let transmit_frames = frames + codec.len();
    let transmit_bytes = bytes + codec.iter().sum::<usize>();
    root.install_transmit(transmit_frames * 2, transmit_bytes * 2)
        .unwrap();
    let budget = ReturnBudget::with_totals(frames * 2, bytes * 2, root.clone());
    let original = frame();
    let mut first = root.transmit_packet(original.len(), 0, 0).unwrap();
    first.copy_from_slice(&original);
    let mut second = root.transmit_packet(original.len(), 0, 0).unwrap();
    second.copy_from_slice(&original);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PacketBuffer>(2);
    tx.try_send(budget.retain_transmit_return(first, 1500, layout).unwrap())
        .unwrap();
    tx.try_send(budget.retain_transmit_return(second, 1500, layout).unwrap())
        .unwrap();
    assert_eq!(
        root.transmit().occupied,
        zero_tun::packet_memory::PacketMemoryLimit {
            frames: transmit_frames * 2,
            bytes: transmit_bytes * 2,
        }
    );
    assert!(root.transmit_packet(1, 0, 0).is_err());
    assert_eq!(*budget.0.occupied.lock().unwrap(), (frames * 2, bytes * 2));
    let first = rx.recv().await.unwrap();
    let packets = zero_stack::packet::fragment_ip_packet_batch(first, 1500, 17)
        .expect("a full return queue head only splits its existing storage envelope");
    assert_eq!(packets.len(), lengths.len());
    assert_eq!(*budget.0.occupied.lock().unwrap(), (frames * 2, bytes * 2));
    let listener = tokio::net::TcpSocket::new_v4().unwrap();
    listener.set_recv_buffer_size(1024).unwrap();
    listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = listener.listen(1).unwrap();
    let client = tokio::net::TcpSocket::new_v4().unwrap();
    client.set_send_buffer_size(1024).unwrap();
    let mut writer = client
        .connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (mut reader, _) = listener.accept().await.unwrap();
    writer.set_nodelay(true).unwrap();
    let expected = lengths.iter().sum::<usize>();
    let (entered, pending) = tokio::sync::oneshot::channel();
    let mut scope = TaskScope::new(
        tokio::runtime::Handle::current(),
        NonZeroUsize::new(1).unwrap(),
    );
    let (task, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let mut entered = Some(entered);
            for packet in packets {
                let write = writer.write_all(&packet);
                tokio::pin!(write);
                std::future::poll_fn(|cx| {
                    let result = write.as_mut().poll(cx);
                    if result.is_pending() {
                        if let Some(entered) = entered.take() {
                            let _ = entered.send(());
                        }
                    }
                    result
                })
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Send, Reason::Io, Impact::Packet).with_source(source)
                })?;
            }
            Ok(())
        })
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !witness.is_joined(),
        "actual pending native write cannot be its Scope join"
    );
    let held = *budget.0.occupied.lock().unwrap();
    assert!(
        held.0 >= frames + 2,
        "second original plus first original and its in-flight derived view"
    );
    assert!(
        held.1 >= bytes + frame().len(),
        "original bytes stay debited through every derived send"
    );
    let mut native = vec![0; expected];
    tokio::time::timeout(Duration::from_secs(5), reader.read_exact(&mut native))
        .await
        .unwrap()
        .unwrap();
    let mut assembled = zero_stack::FragmentReassembler::new();
    let mut recovered = None;
    let mut cursor = 0;
    for length in &lengths {
        let end = cursor + *length;
        if let zero_stack::OwnedFragmentOutcome::Packet {
            packet,
            reassembled: true,
        } = assembled.process_buffer(
            native[cursor..end].to_vec().into(),
            std::time::Instant::now(),
        ) {
            recovered = Some(packet);
        }
        cursor = end;
    }
    let recovered = recovered.expect("actual derived bytes crossed the pending native send");
    assert_eq!(
        zero_stack::packet::parse_udp(&recovered).unwrap().payload,
        &[9; 16_000]
    );
    let exit = tokio::time::timeout(Duration::from_secs(2), scope.next_exit())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exit.id, task);
    exit.result.unwrap();
    assert!(witness.is_joined());
    assert_eq!(
        *budget.0.occupied.lock().unwrap(),
        (frames, bytes),
        "only the unchanged queued original and its own prepared fragments remain"
    );
    assert_eq!(
        root.transmit().occupied,
        zero_tun::packet_memory::PacketMemoryLimit {
            frames: transmit_frames,
            bytes: transmit_bytes,
        }
    );
    drop(rx.recv().await.unwrap());
    assert_eq!(*budget.0.occupied.lock().unwrap(), (0, 0));
    assert_eq!(
        root.transmit().occupied,
        zero_tun::packet_memory::PacketMemoryLimit {
            frames: 0,
            bytes: 0,
        }
    );
}
