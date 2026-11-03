//! Logical byte I/O retains chunks and never claims native resource closure.
use std::{
    future::{poll_fn, Future},
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Poll,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_transport::stream::ChannelStream;

#[tokio::test]
async fn logical_stream_preserves_cancelled_read_chunks_frame_limits_and_single_end() {
    let (input, receive) = tokio::sync::mpsc::unbounded_channel();
    let output = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let ended = Arc::new(AtomicUsize::new(0));
    let mut io = ChannelStream::new(
        receive,
        {
            let output = output.clone();
            move |bytes: &[u8]| {
                output.lock().unwrap().push(bytes.to_vec());
                Ok(bytes.len())
            }
        },
        {
            let ended = ended.clone();
            move || {
                ended.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
        NonZeroUsize::new(2).unwrap(),
        b"prefix".to_vec(),
    );
    let mut prefix = [0; 6];
    io.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, b"prefix");
    let mut bytes = [0; 2];
    {
        let read = io.read(&mut bytes);
        tokio::pin!(read);
        poll_fn(|cx| {
            assert!(
                read.as_mut().poll(cx).is_pending(),
                "actual channel receive remains pending"
            );
            Poll::Ready(())
        })
        .await;
    }
    input.send(b"chunk".to_vec()).unwrap();
    io.read_exact(&mut bytes).await.unwrap();
    assert_eq!(&bytes, b"ch");
    let mut tail = [0; 3];
    io.read_exact(&mut tail).await.unwrap();
    assert_eq!(&tail, b"unk");
    io.write_all(b"ABCDE").await.unwrap();
    assert_eq!(
        *output.lock().unwrap(),
        vec![b"AB".to_vec(), b"CD".to_vec(), b"E".to_vec()]
    );
    io.write_all(b"").await.unwrap();
    assert_eq!(ended.load(Ordering::SeqCst), 0);
    io.shutdown().await.unwrap();
    io.shutdown().await.unwrap();
    drop(io);
    assert_eq!(
        ended.load(Ordering::SeqCst),
        1,
        "one logical end request, not a native receipt"
    );
}

#[tokio::test]
async fn logical_upload_eof_preserves_reverse_direction_until_explicit_shutdown() {
    let (input, receive) = tokio::sync::mpsc::unbounded_channel();
    let output = Arc::new(Mutex::new(Vec::<u8>::new()));
    let mut io = ChannelStream::new(
        receive,
        {
            let output = output.clone();
            move |bytes: &[u8]| {
                output.lock().unwrap().extend(bytes);
                Ok(bytes.len())
            }
        },
        || Ok(()),
        NonZeroUsize::new(64).unwrap(),
        Vec::new(),
    );
    drop(input);
    assert_eq!(io.read(&mut [0]).await.unwrap(), 0);
    io.write_all(b"reverse").await.unwrap();
    assert_eq!(*output.lock().unwrap(), b"reverse");
    io.shutdown().await.unwrap();
    assert_eq!(
        io.write_all(b"late").await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
}

#[tokio::test]
async fn explicit_logical_end_closes_both_directions_instead_of_waiting_for_remote_eof() {
    let (input, receive) = tokio::sync::mpsc::unbounded_channel();
    let ended = Arc::new(AtomicUsize::new(0));
    let mut io = ChannelStream::new(
        receive,
        |bytes: &[u8]| Ok(bytes.len()),
        {
            let ended = ended.clone();
            move || {
                ended.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        },
        NonZeroUsize::new(64).unwrap(),
        Vec::new(),
    );
    input.send(Vec::new()).unwrap();
    assert_eq!(
        io.read(&mut [0]).await.unwrap_err().kind(),
        std::io::ErrorKind::ConnectionAborted
    );
    assert_eq!(
        io.write_all(b"late").await.unwrap_err().kind(),
        std::io::ErrorKind::BrokenPipe
    );
    drop(io);
    assert_eq!(ended.load(Ordering::SeqCst), 1);
}
