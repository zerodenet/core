// gRPC transport.
//
// Bidirectional streaming over HTTP/2 with gRPC wire format.
// Data framing: [compressed flag][length][protobuf hunk].
//
// Max frame payload: 16384 bytes before protobuf wrapping.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Method, Request, Response};
use rand::seq::IndexedRandom;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::sync::mpsc;

use crate::RuntimeError;
use zero_traits::AsyncSocket;

use zero_platform_tokio::ClientStream;

const GRPC_HEADER_LEN: usize = 5;
const GRPC_MAX_PAYLOAD: usize = 16384;

/// gRPC frame header: [compressed(1)] [length(4 BE)]
fn grpc_frame_header(len: usize) -> [u8; GRPC_HEADER_LEN] {
    let mut header = [0u8; GRPC_HEADER_LEN];
    header[0] = 0; // uncompressed
    header[1..5].copy_from_slice(&(len as u32).to_be_bytes());
    header
}

fn parse_grpc_frame_header(header: &[u8; GRPC_HEADER_LEN]) -> (bool, usize) {
    let compressed = header[0] != 0;
    let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    (compressed, len)
}

fn require_grpc_service_names(
    service_names: &[String],
    context: &'static str,
) -> Result<(), RuntimeError> {
    if service_names.is_empty() {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("grpc {context} requires at least one service name"),
        )));
    }
    Ok(())
}

fn choose_grpc_service_name(service_names: &[String]) -> Result<&str, RuntimeError> {
    require_grpc_service_names(service_names, "outbound")?;
    Ok(service_names
        .choose(&mut rand::rng())
        .expect("non-empty grpc service names")
        .as_str())
}

mod client;
mod codec;
mod keepalive;
mod options;
mod owned;
pub use owned::{
    Demand, NativeGrpcStream, NativePoolFactory, NativePoolOptions, NativePoolSettings,
};
mod pool;
mod prepared;
mod prepared_stream;
mod single;
pub use single::{validate_single_client_profile, PreparedSingleClient, SingleClientDriver};
mod progress;
pub use pool::GrpcPool;
mod read;
mod response;
mod server;
mod stream;
mod write;
pub use client::{connect_grpc, connect_grpc_with_profile, connect_grpc_with_settings};
use codec::{decode_grpc_hunk, encode_grpc_hunk};
use read::read_grpc_hunks;
pub use server::{
    accept_grpc, accept_grpc_connection, accept_grpc_with_profile, serve_grpc, GrpcIncoming,
};
pub use stream::GrpcStream;
use write::spawn_grpc_write_relay;
fn build_grpc_stream(
    send_stream: h2::SendStream<Bytes>,
    recv_stream: h2::RecvStream,
    multi: bool,
) -> Result<GrpcStream, RuntimeError> {
    let (write_tx, write_rx) = mpsc::channel::<Vec<u8>>(16);
    let (read_tx, read_rx) = mpsc::channel::<io::Result<Vec<u8>>>(64);

    let progress = std::sync::Arc::new(progress::Progress::default());
    spawn_grpc_write_relay(send_stream, write_rx, progress.clone(), true);
    spawn_grpc_read_relay(recv_stream, read_tx, multi);

    Ok(GrpcStream::new(
        read_rx,
        tokio_util::sync::PollSender::new(write_tx),
        progress,
    ))
}

fn build_grpc_client_stream(
    send_stream: h2::SendStream<Bytes>,
    resp_future: h2::client::ResponseFuture,
    multi: bool,
) -> GrpcStream {
    let prepared_stream::PreparedStream {
        stream,
        read,
        write,
    } = prepared_stream::prepare_stream(send_stream, resp_future, multi);
    tokio::spawn(write);
    tokio::spawn(read);
    stream
}

fn spawn_grpc_read_relay(
    recv_stream: h2::RecvStream,
    read_tx: mpsc::Sender<io::Result<Vec<u8>>>,
    multi: bool,
) {
    tokio::spawn(read_grpc_hunks(recv_stream, read_tx, multi, false));
}

#[cfg(test)]
#[path = "../tests/grpc/mod.rs"]
mod tests;
