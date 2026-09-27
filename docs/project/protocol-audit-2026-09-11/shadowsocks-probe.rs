use std::{io, pin::Pin, sync::{Arc,Mutex}, task::{Context,Poll}};
use tokio::io::{AsyncRead,AsyncWrite,ReadBuf};
use zero_traits::AsyncSocket;
use zero_core::{Address,Network,ProtocolType,Session};
struct Socket { input: io::Cursor<Vec<u8>>, output: Arc<Mutex<Vec<u8>>> }
impl Socket { fn new(data: Vec<u8>) -> Self { Self { input: io::Cursor::new(data), output: Default::default() } } }
impl AsyncSocket for Socket {
 type Error=io::Error;
 async fn read(&mut self,b:&mut [u8])->io::Result<usize>{io::Read::read(&mut self.input,b)}
 async fn write_all(&mut self,b:&[u8])->io::Result<()>{self.output.lock().unwrap().extend_from_slice(b);Ok(())}
 async fn shutdown(&mut self)->io::Result<()>{Ok(())}
}
impl AsyncRead for Socket { fn poll_read(mut self:Pin<&mut Self>,_:&mut Context<'_>,b:&mut ReadBuf<'_>)->Poll<io::Result<()>>{let mut bytes=vec![0;b.remaining()];let n=io::Read::read(&mut self.input,&mut bytes)?;b.put_slice(&bytes[..n]);Poll::Ready(Ok(()))} }
impl AsyncWrite for Socket {
 fn poll_write(self:Pin<&mut Self>,_:&mut Context<'_>,b:&[u8])->Poll<io::Result<usize>>{self.output.lock().unwrap().extend_from_slice(b);Poll::Ready(Ok(b.len()))}
 fn poll_flush(self:Pin<&mut Self>,_:&mut Context<'_>)->Poll<io::Result<()>>{Poll::Ready(Ok(()))}
 fn poll_shutdown(self:Pin<&mut Self>,_:&mut Context<'_>)->Poll<io::Result<()>>{Poll::Ready(Ok(()))}
}
fn target()->Session{Session::new(0,Address::Domain("example.com".into()),443,Network::Tcp,ProtocolType::new("audit"))}

#[tokio::test]
async fn audit_classic_ss_handshake_is_accepted_twice(){
 let mut socket=Socket::new(vec![]);
 shadowsocks::ShadowsocksOutbound.send_request(&mut socket,&target(),shadowsocks::CipherKind::Aes128Gcm,b"audit-secret").await.unwrap();
 let wire=socket.output.lock().unwrap().clone();
 let profile=shadowsocks::ShadowsocksInboundProfile::from_config_users("aes-128-gcm",[shadowsocks::transport::ShadowsocksInboundUserRef{password:"audit-secret",principal_key:None,up_bps:None,down_bps:None,device_limit:None,quota_remaining_bytes:None,policy_revision:None}]).unwrap();
 let acceptor=shadowsocks::ShadowsocksInboundTcpAcceptor::new(profile);
 for i in 1..=2 {let accepted=acceptor.accept_stream(Socket::new(wire.clone())).await;assert!(accepted.is_ok());println!("Shadowsocks aes-128-gcm identical captured handshake attempt {i}: ACCEPTED");}
}

#[test]
fn audit_ss2022_replay_window_accepts_zero_and_left_edge_duplicates(){
 let mut zero=shadowsocks::ReplayWindow::new();
 assert!(zero.check_and_update(0));assert!(zero.check_and_update(0));
 println!("SS2022 packet id 0: accepted twice");
 let mut edge=shadowsocks::ReplayWindow::new();
 assert!(edge.check_and_update(4096));
 assert!(edge.check_and_update(2048));assert!(edge.check_and_update(2048));
 println!("SS2022 left boundary packet id 2048 after 4096: accepted twice");
 let mut control=shadowsocks::ReplayWindow::new();assert!(control.check_and_update(1));assert!(!control.check_and_update(1));
 println!("Control ordinary packet id 1: duplicate correctly rejected");
}
