//! One ordered cipher and bounded send queue for the entire TCP underlay.
use super::stream::Status;
use crate::{
    crypto::MieruCipher,
    metadata::*,
    segment::{build_data_segment, build_session_segment_with_padding},
    session::MieruSession,
    traffic_pattern::{
        data_padding_lengths, session_padding, write_with_fragmentation, TrafficPattern,
    },
};
use std::{collections::BTreeMap, io, string::String, sync::Arc};
use tokio::{
    io::{AsyncWrite, AsyncWriteExt},
    sync::{mpsc, oneshot},
};

pub(crate) enum Command {
    Open(u32),
    OpenClient(u32),
    Retire(u32),
    Data {
        id: u32,
        payload: Vec<u8>,
        status: Arc<Status>,
    },
    Close {
        id: u32,
        response: bool,
    },
    Barrier {
        id: u32,
        close: bool,
        status: Arc<Status>,
        done: oneshot::Sender<io::Result<()>>,
    },
}
pub(crate) async fn run_with_pattern<W: AsyncWrite + Unpin>(
    socket: W,
    cipher: MieruCipher,
    first_id: u32,
    commands: mpsc::Receiver<Command>,
    pattern: TrafficPattern,
    username: String,
) -> io::Result<()> {
    run_direction_with_pattern(socket, cipher, first_id, commands, false, pattern, username).await
}
pub(crate) async fn run_direction_with_pattern<W: AsyncWrite + Unpin>(
    mut socket: W,
    mut cipher: MieruCipher,
    first_id: u32,
    mut commands: mpsc::Receiver<Command>,
    client: bool,
    pattern: TrafficPattern,
    username: String,
) -> io::Result<()> {
    let mut sequences = BTreeMap::from([(first_id, 1u32)]);
    while let Some(command) = commands.recv().await {
        match command {
            Command::Retire(id) => {
                sequences.remove(&id);
            }
            Command::Open(id) => {
                control(
                    &mut socket,
                    &mut cipher,
                    OPEN_SESSION_RESPONSE,
                    id,
                    0,
                    &pattern,
                    &username,
                )
                .await?;
                sequences.insert(id, 1);
            }
            Command::OpenClient(id) => {
                control(
                    &mut socket,
                    &mut cipher,
                    OPEN_SESSION_REQUEST,
                    id,
                    0,
                    &pattern,
                    &username,
                )
                .await?;
                sequences.insert(id, 1);
            }
            Command::Data {
                id,
                payload,
                status,
            } => {
                if status.closed() {
                    continue;
                }
                let seq = sequences
                    .get_mut(&id)
                    .ok_or_else(|| io::Error::other("mieru unknown send session"))?;
                let mut meta = DataMetadata::new(if client {
                    DATA_CLIENT_TO_SERVER
                } else {
                    DATA_SERVER_TO_CLIENT
                });
                meta.timestamp = MieruSession::timestamp_minutes();
                meta.session_id = id;
                meta.sequence_number = *seq;
                *seq = seq.wrapping_add(1);
                meta.window_size = 1024;
                meta.payload_length = payload.len() as u16;
                let (prefix, suffix) = data_padding_lengths(u8::MAX as usize);
                meta.prefix_length = prefix;
                meta.suffix_length = suffix;
                let wire = build_data_segment(&meta, &payload, &mut cipher, false)
                    .map_err(io::Error::other)?;
                write_with_fragmentation(&mut socket, &wire, pattern.tcp_fragment).await?;
            }
            Command::Close { id, response } => {
                let seq = sequences.remove(&id).unwrap_or(0);
                control(
                    &mut socket,
                    &mut cipher,
                    if response {
                        CLOSE_SESSION_RESPONSE
                    } else {
                        CLOSE_SESSION_REQUEST
                    },
                    id,
                    seq,
                    &pattern,
                    &username,
                )
                .await?;
            }
            Command::Barrier {
                id,
                close,
                status,
                done,
            } => {
                let result = async {
                    if status.closed() {
                        return Err(io::ErrorKind::BrokenPipe.into());
                    }
                    if close {
                        let seq = sequences.remove(&id).unwrap_or(0);
                        control(
                            &mut socket,
                            &mut cipher,
                            CLOSE_SESSION_REQUEST,
                            id,
                            seq,
                            &pattern,
                            &username,
                        )
                        .await?;
                    }
                    socket.flush().await?;
                    if close {
                        status.close(None);
                    }
                    Ok(())
                }
                .await;
                let failed = result.is_err();
                let _ = done.send(result);
                if failed && !status.closed() {
                    return Err(io::Error::other("mieru underlay flush failed"));
                }
            }
        }
    }
    Ok(())
}
async fn control<W: AsyncWrite + Unpin>(
    socket: &mut W,
    cipher: &mut MieruCipher,
    kind: u8,
    id: u32,
    seq: u32,
    pattern: &TrafficPattern,
    username: &str,
) -> io::Result<()> {
    let mut meta = SessionMetadata::new(kind);
    meta.timestamp = MieruSession::timestamp_minutes();
    meta.session_id = id;
    meta.sequence_number = seq;
    let padding = session_padding(u8::MAX as usize, 64, username);
    meta.suffix_length = padding.len() as u8;
    let wire = build_session_segment_with_padding(&meta, &[], cipher, false, &padding)
        .map_err(io::Error::other)?;
    write_with_fragmentation(socket, &wire, pattern.tcp_fragment).await?;
    socket.flush().await
}
