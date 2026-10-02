//! Default TUN I/O bridge. Successful device I/O, not queue admission, is metered.
use crate::{TunDevice, TunPacketReceiver, TunPacketSender};
use std::{io, sync::Arc};
use zero_traits::IoObserver;

pub(crate) fn bridge<D: TunDevice + 'static>(
    device: D,
    observer: Option<Arc<dyn IoObserver>>,
) -> io::Result<(TunPacketSender, TunPacketReceiver)> {
    let (read_tx, read_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let (write_tx, mut write_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(256);
    let (mut reader, mut writer) = tokio::io::split(device);
    let (close_tx, mut close_rx) = tokio::sync::watch::channel(false);
    let reads = observer.clone();
    tokio::spawn(async move {
        let mut buf = vec![0; 65536];
        loop {
            let result = tokio::select! {
                read = tokio::io::AsyncReadExt::read(&mut reader, &mut buf) => read,
                _ = close_rx.changed() => break,
            };
            match result {
                Ok(0) => break,
                Ok(n) => {
                    if let Some(observer) = &reads {
                        observer.received(n);
                    }
                    if read_tx.send(buf[..n].to_vec()).await.is_err() {
                        if let Some(observer) = &reads {
                            observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
                        }
                        break;
                    }
                }
                Err(_) => {
                    if let Some(observer) = &reads {
                        observer.error();
                    }
                    break;
                }
            }
        }
    });
    tokio::spawn(async move {
        while let Some(packet) = write_rx.recv().await {
            if tokio::io::AsyncWriteExt::write_all(&mut writer, &packet)
                .await
                .is_err()
            {
                if let Some(observer) = &observer {
                    observer.error();
                    observer.dropped_reason(zero_traits::PacketDropReason::IoFailure);
                }
                break;
            }
            if let Some(observer) = &observer {
                observer.sent(packet.len());
            }
        }
        // The queue can contain already accepted packets after a device failure.
        write_rx.close();
        while write_rx.recv().await.is_some() {
            if let Some(observer) = &observer {
                observer.dropped_reason(zero_traits::PacketDropReason::QueueClosed);
            }
        }
        let _ = close_tx.send(true);
    });
    Ok((write_tx, read_rx))
}
