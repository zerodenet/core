use super::fixtures::*;
use mieru::client::ClientConnection;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
};
#[tokio::test]
async fn native_udp_recovers_loss_reordering_duplicates_and_preserves_sessions() {
    tokio::time::timeout(Duration::from_secs(30),async {
        let(server,server_task)=udp_server().await;
        let relay=Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());let endpoint=relay.local_addr().unwrap();
        let faults=Arc::new(AtomicUsize::new(0));let observed=faults.clone();
        let relay_task=tokio::spawn(async move {
            let mut client=None;let mut count=0;let mut buffer=[0;1501];let mut delayed=tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    received=relay.recv_from(&mut buffer)=>{
                        let(n,from)=received.unwrap();let target=if from==server {client.unwrap()} else {client=Some(from);server};count+=1;
                        if count==1 || count%17==0 {observed.fetch_add(1,Ordering::SeqCst);continue;}
                        let data=buffer[..n].to_vec();
                        if count%7==0 {let socket=relay.clone();observed.fetch_add(1,Ordering::SeqCst);delayed.spawn(async move {tokio::time::sleep(Duration::from_millis(30)).await;let _=socket.send_to(&data,target).await;});}
                        else {relay.send_to(&data,target).await.unwrap();if count%11==0 {observed.fetch_add(1,Ordering::SeqCst);relay.send_to(&data,target).await.unwrap();}}
                    }
                    _=delayed.join_next(),if !delayed.is_empty()=>{}
                }
            }
        });
        let socket=Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let connection=ClientConnection::udp(socket,endpoint,"u","p").await.unwrap();let mut tasks=tokio::task::JoinSet::new();
        for byte in 0..4u8 {
            let connection=connection.clone();tasks.spawn(async move {
                let stream=connection.open().await.unwrap();let (mut read,mut write)=tokio::io::split(stream);let payload=vec![byte;65537];let mut output=vec![0;payload.len()];
                let (sent,received)=tokio::join!(async {write.write_all(&payload).await?;write.flush().await},read.read_exact(&mut output));sent.unwrap();received.unwrap();assert_eq!(output,payload);
                write.shutdown().await.unwrap();
            });
        }
        while let Some(result)=tasks.join_next().await {result.unwrap();}
        let mut later=connection.open().await.unwrap();later.write_all(b"later").await.unwrap();later.flush().await.unwrap();let mut result=[0;5];later.read_exact(&mut result).await.unwrap();assert_eq!(&result,b"later");
        assert!(faults.load(Ordering::SeqCst)>10);relay_task.abort();server_task.abort();
    }).await.unwrap();
}
