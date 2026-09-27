use std::{io, pin::Pin, sync::{Arc,Mutex}, task::{Context,Poll}};
use tokio::io::{AsyncRead,AsyncWrite,ReadBuf};
use zero_traits::AsyncSocket;
use zero_core::{Address,Network,ProtocolType,Session};
struct Socket { input: io::Cursor<Vec<u8>>, output: Arc<Mutex<Vec<u8>>>, reads:usize, fragment:bool, slow:bool }
impl Socket { fn new(data: Vec<u8>) -> Self { Self { input: io::Cursor::new(data), output: Default::default(), reads:0, fragment:false, slow:false } } }
impl AsyncSocket for Socket {
 type Error=io::Error;
 async fn read(&mut self,b:&mut [u8])->io::Result<usize>{self.reads+=1; if self.slow {tokio::time::sleep(std::time::Duration::from_secs(1)).await;} let n=if self.fragment && self.reads==2 {1} else {b.len()}; io::Read::read(&mut self.input,&mut b[..n])}
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
async fn variable_header_fragmentation_is_rejected() {
 let password=b"MDEyMzQ1Njc4OWFiY2RlZg==";
 let mut socket=Socket::new(vec![]);
 shadowsocks::ShadowsocksOutbound.send_request(&mut socket,&target(),shadowsocks::CipherKind::Blake3Aes128Gcm,password).await.unwrap();
 let wire=socket.output.lock().unwrap().clone();
 let profile=shadowsocks::ShadowsocksInboundProfile::from_config_users("2022-blake3-aes-128-gcm",[shadowsocks::transport::ShadowsocksInboundUserRef{password:std::str::from_utf8(password).unwrap(),principal_key:None,up_bps:None,down_bps:None,device_limit:None,quota_remaining_bytes:None,policy_revision:None}]).unwrap();
 let acceptor=shadowsocks::ShadowsocksInboundTcpAcceptor::new(profile);
 let mut fragmented=Socket::new(wire.clone()); fragmented.fragment=true;
 let err=acceptor.accept_stream(fragmented).await.err().expect("expected current defect");
 println!("fragmented variable header rejected: {err}");
 assert!(err.to_string().contains("variable header too short"));
 assert!(acceptor.accept_stream(Socket::new(wire)).await.is_ok());
 println!("same bytes unfragmented: accepted");
}
#[tokio::test(start_paused=true)]
async fn drain_has_no_total_two_second_deadline() {
 let profile=shadowsocks::ShadowsocksInboundProfile::from_config_users("2022-blake3-aes-128-gcm",[shadowsocks::transport::ShadowsocksInboundUserRef{password:"MDEyMzQ1Njc4OWFiY2RlZg==",principal_key:None,up_bps:None,down_bps:None,device_limit:None,quota_remaining_bytes:None,policy_revision:None}]).unwrap();
 let acceptor=shadowsocks::ShadowsocksInboundTcpAcceptor::new(profile);
 let mut socket=Socket::new(vec![0;20_000]); socket.slow=true;
 let start=tokio::time::Instant::now();
 assert!(acceptor.accept_stream(socket).await.is_err());
 let elapsed=start.elapsed();
 println!("failed handshake with one read per second completed after {elapsed:?}");
 assert!(elapsed.as_secs()>=7);
}
