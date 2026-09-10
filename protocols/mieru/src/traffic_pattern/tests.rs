use mieru_config::{
    MieruNoncePatternConfig, MieruNonceType, MieruTcpFragmentConfig, MieruTrafficPatternConfig,
};

use super::*;
use crate::{
    crypto::MieruCipher,
    metadata::{SessionMetadata, OPEN_SESSION_REQUEST},
    segment::build_session_segment_with_padding,
};

#[test]
fn fixed_seed_generates_official_locked_defaults() {
    let pattern = TrafficPattern::from_config(Some(&MieruTrafficPatternConfig {
        seed: Some(42),
        ..Default::default()
    }))
    .unwrap();
    assert_eq!(
        pattern.tcp_fragment,
        TcpFragmentPattern {
            enable: false,
            max_sleep_ms: 0,
        }
    );
    assert_eq!(
        pattern.nonce,
        NoncePatternRuntime {
            pattern: NoncePattern::Printable {
                min_len: 12,
                max_len: 12,
            },
            apply_to_all_udp_packet: false,
        }
    );
}

#[test]
fn fixed_seed_generates_official_unlocked_defaults() {
    let pattern = TrafficPattern::from_config(Some(&MieruTrafficPatternConfig {
        seed: Some(42),
        unlock_all: Some(true),
        ..Default::default()
    }))
    .unwrap();
    assert_eq!(
        pattern.tcp_fragment,
        TcpFragmentPattern {
            enable: true,
            max_sleep_ms: 55,
        }
    );
    assert_eq!(
        pattern.nonce,
        NoncePatternRuntime {
            pattern: NoncePattern::PrintableSubset {
                min_len: 9,
                max_len: 12,
            },
            apply_to_all_udp_packet: false,
        }
    );
}

#[test]
fn explicit_fields_override_generated_values_individually() {
    let pattern = TrafficPattern::from_config(Some(&MieruTrafficPatternConfig {
        seed: Some(42),
        unlock_all: Some(false),
        tcp_fragment: Some(MieruTcpFragmentConfig {
            enable: Some(true),
            max_sleep_ms: Some(7),
        }),
        nonce: Some(MieruNoncePatternConfig {
            kind: Some(MieruNonceType::Fixed),
            apply_to_all_udp_packet: Some(true),
            custom_hex_strings: vec!["0011aa".into()],
            ..Default::default()
        }),
    }))
    .unwrap();
    assert_eq!(pattern.tcp_fragment.max_sleep_ms, 7);
    assert_eq!(
        pattern.nonce,
        NoncePatternRuntime {
            pattern: NoncePattern::Fixed {
                prefixes: vec![vec![0x00, 0x11, 0xaa]],
            },
            apply_to_all_udp_packet: true,
        }
    );
}

#[test]
fn fixed_without_prefixes_is_random_as_officially_documented() {
    let pattern = TrafficPattern::from_config(Some(&MieruTrafficPatternConfig {
        seed: Some(0),
        nonce: Some(MieruNoncePatternConfig {
            kind: Some(MieruNonceType::Fixed),
            ..Default::default()
        }),
        ..Default::default()
    }))
    .unwrap();
    assert_eq!(
        pattern.nonce.pattern,
        NoncePattern::Fixed {
            prefixes: Vec::new(),
        }
    );
}

#[test]
fn generated_minimum_is_clamped_when_only_a_small_maximum_is_explicit() {
    let pattern = TrafficPattern::from_config(Some(&MieruTrafficPatternConfig {
        seed: Some(42),
        nonce: Some(MieruNoncePatternConfig {
            max_len: Some(3),
            ..Default::default()
        }),
        ..Default::default()
    }))
    .unwrap();
    assert_eq!(
        pattern.nonce.pattern,
        NoncePattern::Printable {
            min_len: 3,
            max_len: 3,
        }
    );
}

#[test]
fn padding_lengths_stay_within_the_supplied_wire_budget() {
    for budget in [0, 1, 17, 255, 512] {
        let (prefix, suffix) = data_padding_lengths(budget);
        assert!(prefix as usize + suffix as usize <= budget.min(255));
        assert!(session_padding(budget, 88, "alice").len() <= budget.min(255));
    }
}

#[test]
fn selected_session_padding_bytes_are_written_to_the_wire_tail() {
    let padding = [0x20, 0x41, 0x7e, 0x42];
    let mut metadata = SessionMetadata::new(OPEN_SESSION_REQUEST);
    metadata.suffix_length = padding.len() as u8;
    let mut cipher = MieruCipher::with_nonce(&[9; 32], [7; 24]);
    let wire =
        build_session_segment_with_padding(&metadata, &[], &mut cipher, true, &padding).unwrap();
    assert_eq!(&wire[wire.len() - padding.len()..], &padding);
}

#[tokio::test]
async fn enabled_tcp_fragmentation_uses_multiple_writes() {
    let mut writes = CountingWriter::default();
    write_with_fragmentation(
        &mut writes,
        &[7; 256],
        TcpFragmentPattern {
            enable: true,
            max_sleep_ms: 0,
        },
    )
    .await
    .unwrap();
    assert!(writes.calls > 1);
    assert_eq!(writes.bytes, 256);
}

#[tokio::test]
async fn polled_fragmentation_survives_cancelled_flush_short_writes_and_pending() {
    use core::{future::Future, pin::Pin, task::Poll};

    let pattern = TcpFragmentPattern {
        enable: true,
        max_sleep_ms: 1,
    };
    let first = vec![3; 127];
    let second = vec![9; 93];
    let mut state = FragmentedWriteState::new(pattern);
    let mut writer = PendingShortWriter::default();

    state.start(first.clone(), 11);
    let mut flush = Box::pin(std::future::poll_fn(|cx| {
        state.poll_flush(Pin::new(&mut writer), cx)
    }));
    let waker = std::task::Waker::noop();
    let mut cx = core::task::Context::from_waker(waker);
    assert!(matches!(flush.as_mut().poll(&mut cx), Poll::Pending));
    drop(flush);

    std::future::poll_fn(|cx| state.poll_ready_for_write(Pin::new(&mut writer), cx))
        .await
        .unwrap();
    state.start(second.clone(), 7);
    std::future::poll_fn(|cx| state.poll_shutdown(Pin::new(&mut writer), cx))
        .await
        .unwrap();

    assert!(state.is_idle());
    assert_eq!(writer.flushes, 0);
    assert_eq!(writer.shutdowns, 1);
    assert_eq!(writer.bytes, [first, second].concat());
    assert!(writer.pending_calls > 0);
}

#[tokio::test]
async fn fragment_sleep_does_not_deadlock_bidirectional_small_duplex() {
    let (left, right) = tokio::io::duplex(8);
    let left_wire = vec![0x2a; 127];
    let right_wire = vec![0x6b; 93];
    let pattern = TcpFragmentPattern {
        enable: true,
        max_sleep_ms: 1,
    };

    let (left_received, right_received) = tokio::join!(
        exchange_fragmented(left, left_wire.clone(), right_wire.len(), pattern),
        exchange_fragmented(right, right_wire.clone(), left_wire.len(), pattern),
    );
    assert_eq!(left_received.unwrap(), right_wire);
    assert_eq!(right_received.unwrap(), left_wire);
}

async fn exchange_fragmented(
    mut socket: tokio::io::DuplexStream,
    outgoing: Vec<u8>,
    incoming_len: usize,
    pattern: TcpFragmentPattern,
) -> std::io::Result<Vec<u8>> {
    use core::{pin::Pin, task::Poll};
    use tokio::io::AsyncRead;

    let mut state = FragmentedWriteState::new(pattern);
    state.start(outgoing, 1);
    let mut incoming = vec![0; incoming_len];
    let mut position = 0;
    while position < incoming.len() {
        let read = std::future::poll_fn(|cx| {
            if let Poll::Ready(Err(error)) = state.poll_drain(Pin::new(&mut socket), cx) {
                return Poll::Ready(Err(error));
            }
            let mut read_buf = tokio::io::ReadBuf::new(&mut incoming[position..]);
            match Pin::new(&mut socket).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) if read_buf.filled().is_empty() => Poll::Ready(Err(
                    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "duplex closed"),
                )),
                Poll::Ready(Ok(())) => Poll::Ready(Ok(read_buf.filled().len())),
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await?;
        position += read;
    }
    std::future::poll_fn(|cx| state.poll_drain(Pin::new(&mut socket), cx)).await?;
    Ok(incoming)
}

#[derive(Default)]
struct CountingWriter {
    calls: usize,
    bytes: usize,
}

#[derive(Default)]
struct PendingShortWriter {
    calls: usize,
    pending_calls: usize,
    flushes: usize,
    shutdowns: usize,
    bytes: Vec<u8>,
}

impl tokio::io::AsyncWrite for PendingShortWriter {
    fn poll_write(
        mut self: core::pin::Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
        buf: &[u8],
    ) -> core::task::Poll<std::io::Result<usize>> {
        self.calls += 1;
        if self.calls.is_multiple_of(2) {
            self.pending_calls += 1;
            cx.waker().wake_by_ref();
            return core::task::Poll::Pending;
        }
        let written = buf.len().min(3);
        self.bytes.extend_from_slice(&buf[..written]);
        core::task::Poll::Ready(Ok(written))
    }

    fn poll_flush(
        mut self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        self.flushes += 1;
        core::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        self.shutdowns += 1;
        core::task::Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for CountingWriter {
    fn poll_write(
        mut self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
        buf: &[u8],
    ) -> core::task::Poll<std::io::Result<usize>> {
        self.calls += 1;
        self.bytes += buf.len();
        core::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: core::pin::Pin<&mut Self>,
        _cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<std::io::Result<()>> {
        core::task::Poll::Ready(Ok(()))
    }
}
