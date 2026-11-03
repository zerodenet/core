use super::*;
use std::{
    net::SocketAddr,
    sync::{atomic::AtomicUsize, Arc},
};
use zero_platform_tokio::contract::CancellationToken;
pub(super) struct Donor {
    pub address: SocketAddr,
    pub accepts: Arc<AtomicUsize>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Donor {
    pub async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let count = accepts.clone();
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        let task = tokio::spawn(async move {
            let mut streams = tokio::task::JoinSet::new();
            loop {
                let (mut io, _) = tokio::select! {biased;_=stop.cancelled()=>break,result=listener.accept()=>result.unwrap()};
                count.fetch_add(1, Ordering::SeqCst);
                let stop = stop.clone();
                streams.spawn(async move {
                    tokio::select! {biased;_=stop.cancelled()=>{},_=async {
                        let mut greeting = [0; 2]; io.read_exact(&mut greeting).await.unwrap();
                        assert_eq!(greeting[0],5);
                        let mut methods = vec![0;greeting[1] as usize];io.read_exact(&mut methods).await.unwrap();
                        assert!(methods.contains(&2));io.write_all(&[5,2]).await.unwrap();
                        let mut auth=[0;2];io.read_exact(&mut auth).await.unwrap();assert_eq!(auth[0],1);
                        let mut username=vec![0;auth[1] as usize];io.read_exact(&mut username).await.unwrap();
                        let len=io.read_u8().await.unwrap();let mut password=vec![0;len as usize];io.read_exact(&mut password).await.unwrap();
                        assert_eq!(username,b"u");assert!(password==b"p"||password==b"changed");io.write_all(&[1,0]).await.unwrap();
                        let mut connect=[0;4];io.read_exact(&mut connect).await.unwrap();assert_eq!(&connect[..3],&[5,1,0]);
                        let ip = match connect[3] {
                            1 => {let mut bytes=[0;4];io.read_exact(&mut bytes).await.unwrap();std::net::IpAddr::from(bytes)},
                            4 => {let mut bytes=[0;16];io.read_exact(&mut bytes).await.unwrap();std::net::IpAddr::from(bytes)},
                            3 => {let len=io.read_u8().await.unwrap();let mut bytes=vec![0;len as usize];io.read_exact(&mut bytes).await.unwrap();String::from_utf8(bytes).unwrap().parse::<std::net::IpAddr>().expect("literal fixture endpoint in its real named wire form")},
                            _ => panic!("invalid SOCKS address type"),
                        };
                        let port=io.read_u16().await.unwrap();
                        let mut peer=tokio::net::TcpStream::connect(std::net::SocketAddr::new(ip,port)).await.unwrap();
                        io.write_all(&[5,0,0,1,127,0,0,1,0,0]).await.unwrap();
                        let _=tokio::io::copy_bidirectional(&mut io,&mut peer).await;
                    }=>{}}
                });
            }
            while let Some(result) = streams.join_next().await {
                result.unwrap();
            }
        });
        Self {
            address,
            accepts,
            cancel,
            task,
        }
    }
    pub async fn close(self) {
        self.cancel.cancel();
        self.task.await.unwrap();
    }
}
