use super::super::protocol::{IpcRequest, IpcResponse, serialize_frame};
use std::io;
// ── Client ────────────────────────────────────────────────────────────

use std::io::{BufRead as StdBufRead, BufReader as StdBufReader, Write};

/// Connect to the named pipe and send a single request.
pub fn send_request(pipe_name: &str, request: &IpcRequest) -> io::Result<IpcResponse> {
    let stream = open_pipe_client(pipe_name)?;
    send_frame(&stream, request)?;
    read_response(&stream)
}

/// Connect and stream events.
pub fn stream_events(
    pipe_name: &str,
    request: &IpcRequest,
    mut on_event: impl FnMut(serde_json::Value),
) -> io::Result<()> {
    let stream = open_pipe_client(pipe_name)?;
    send_frame(&stream, request)?;

    let reader = StdBufReader::new(&stream);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(&line).map_err(io::Error::other)?;
        on_event(value);
    }
    Ok(())
}

fn open_pipe_client(pipe_name: &str) -> io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(pipe_name)
}

fn send_frame(mut stream: &std::fs::File, request: &IpcRequest) -> io::Result<()> {
    let frame = serialize_frame(request).map_err(io::Error::other)?;
    stream.write_all(&frame)?;
    Ok(())
}

fn read_response(stream: &std::fs::File) -> io::Result<IpcResponse> {
    let reader = StdBufReader::new(stream);
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        return serde_json::from_str(&line).map_err(io::Error::other);
    }
    Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "no response from ipc pipe",
    ))
}
