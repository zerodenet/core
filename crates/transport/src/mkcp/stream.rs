use super::{config::Settings, state::Connection};
use bytes::Bytes;
use std::future::Future;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf},
    sync::{mpsc, oneshot},
};
use zero_platform_tokio::ClientStream;

pub struct MkcpStream {
    io: DuplexStream,
    local: SocketAddr,
    peer: SocketAddr,
    close: Option<oneshot::Sender<()>>,
}
impl Drop for MkcpStream {
    fn drop(&mut self) {
        if let Some(close) = self.close.take() {
            let _ = close.send(());
        }
    }
}
impl AsyncRead for MkcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buffer)
    }
}
impl AsyncWrite for MkcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buffer)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for MkcpStream {
    type Error = io::Error;
    async fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buffer).await
    }
    async fn write_all(&mut self, buffer: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buffer).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl ClientStream for MkcpStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local)
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
}
pub(super) enum Input {
    Socket(zero_platform_tokio::PacketSocket, SocketAddr),
    Packets(mpsc::Receiver<Vec<u8>>),
}
impl Input {
    async fn receive(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Socket(socket, peer) => loop {
                let (n, source) = socket.recv_from(buffer).await?;
                if source == *peer {
                    return Ok(n);
                }
            },
            Self::Packets(receiver) => {
                let packet = receiver.recv().await.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "mKCP listener closed")
                })?;
                let n = packet.len().min(buffer.len());
                buffer[..n].copy_from_slice(&packet[..n]);
                Ok(n)
            }
        }
    }
}
pub(super) fn connection(
    socket: zero_platform_tokio::PacketSocket,
    peer: SocketAddr,
    conv: u16,
    settings: Settings,
    input: Input,
) -> io::Result<(
    MkcpStream,
    impl Future<Output = io::Result<()>> + Send + 'static,
)> {
    let local = socket.local_addr()?;
    let (client, server) =
        tokio::io::duplex(settings.write_buffer_bytes.clamp(65536, 2 * 1024 * 1024) as usize);
    let (close_tx, close_rx) = oneshot::channel();
    Ok((
        MkcpStream {
            io: client,
            local,
            peer,
            close: Some(close_tx),
        },
        drive(socket, peer, conv, settings, input, server, close_rx),
    ))
}
async fn drive(
    socket: zero_platform_tokio::PacketSocket,
    peer: SocketAddr,
    conv: u16,
    settings: Settings,
    mut input: Input,
    stream: DuplexStream,
    mut closed: oneshot::Receiver<()>,
) -> io::Result<()> {
    let mut state = Connection::new(conv, settings);
    let started = tokio::time::Instant::now();
    let mut ticker = tokio::time::interval(Duration::from_millis(u64::from(settings.tti_ms)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let (mut reader, mut writer) = tokio::io::split(stream);
    let mut packet = vec![0; 65536];
    let mut application = vec![0; settings.mss()];
    let mut offset = 0;
    let mut local_closed = false;
    while !state.finished() {
        let ready = state.receiver.front().unwrap_or_default();
        tokio::select! {
            _=&mut closed,if !local_closed=>{local_closed=true;state.close(started.elapsed().as_millis() as u32);}
            incoming=input.receive(&mut packet)=>{let n=incoming?;state.input(&packet[..n],started.elapsed().as_millis() as u32);}
            read=reader.read(&mut application),if !local_closed && state.writable()=>{
                match read {
                    Ok(0)|Err(_)=>{local_closed=true;state.close(started.elapsed().as_millis() as u32);}
                    Ok(n)=>state.write(Bytes::copy_from_slice(&application[..n])),
                }
            }
            written=writer.write(&ready[offset..]),if !ready.is_empty() && state.can_read() && !local_closed=>{
                match written {
                    Ok(0)|Err(_)=>{local_closed=true;state.close(started.elapsed().as_millis() as u32);}
                    Ok(n)=>{offset+=n;if offset==ready.len() {offset=0;state.receiver.consume();}}
                }
            }
            _=ticker.tick()=>{
                for packet in state.flush(started.elapsed().as_millis() as u32) {socket.send_to(&packet,peer).await?;}
            }
        }
    }
    Ok(())
}
