//! The per-connection caller advances original H2 and both hunk futures itself.
use super::*;
#[path = "inline_wire.rs"]
mod wire;
use crate::profile::OwnedGrpcProfile;
use tokio::time::{timeout, Duration};
#[cfg(any(unix, windows))]
#[tokio::test]
async fn inline_single_and_multi_clients_drain_upload_without_application_reads_and_keep_reverse_half_close(
) {
    use std::num::NonZeroUsize;
    use zero_platform_tokio::{
        contract::{component::ResourceDeclaration, lifecycle::ChannelControl, CancellationToken},
        AcceptedTcpUsers, ControlledTcpListenerOwner, EgressInterfaceControl, PendingTcpConnect,
    };
    let end = tokio::time::Instant::now() + Duration::from_secs(8);
    let phase = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let peer_phase = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let received = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let native = std::sync::Arc::new(std::sync::Mutex::new(Vec::<
        std::sync::Arc<wire::Observation>,
    >::new()));
    let clock = std::time::Instant::now();
    timeout(Duration::from_secs(8),async {
        for multi_mode in [false,true] {
            received.store(0,std::sync::atomic::Ordering::SeqCst);
            let one=NonZeroUsize::new(1).unwrap();
            let destination=zero_core::request::Destination::new(zero_core::Address::Ipv4([127,0,0,1]),0);
            let mut peer_original=ControlledTcpListenerOwner::new_with_accepted_limit(&ResourceDeclaration::TcpListener{destination,accepted:None,max_bind_attempts:one},one,CancellationToken::new(),one).unwrap();
            let mut listener=peer_original.bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
            let address=listener.local_addr().unwrap();
            let profile=OwnedGrpcProfile{service_names:vec!["inline/Tun".into()],multi_mode,..Default::default()};
            let server_profile=profile.clone();let(done,finished)=tokio::sync::oneshot::channel();let peer_observation=peer_phase.clone();
            let peer_native=native.clone();
            let server=tokio::spawn(async move {
                let(accepted,_)=listener.accept_owned().await.unwrap();
                let claim=accepted.claim();
                let(io,_)=accepted.into_parts();
                let nodelay=io.nodelay().expect("same accepted original native option");
                assert!(nodelay,"actual production accepted provider must configure TCP_NODELAY");
                let(io,observation)=wire::Wire::new(io,nodelay,multi_mode,"peer",clock);peer_native.lock().unwrap().push(observation);
                let incoming=accept_grpc_connection(io,&server_profile).unwrap();
                let mut io=incoming.accept().await.unwrap();peer_observation.store(1,std::sync::atomic::Ordering::SeqCst);
                let mut body=Vec::new();io.read_to_end(&mut body).await.unwrap();peer_observation.store(2,std::sync::atomic::Ordering::SeqCst);
                assert_eq!(body,vec![0x79;2*1024*1024+13]);AsyncWriteExt::write_all(&mut io,&body).await.unwrap();peer_observation.store(3,std::sync::atomic::Ordering::SeqCst);
                AsyncWriteExt::shutdown(&mut io).await.unwrap();peer_observation.store(4,std::sync::atomic::Ordering::SeqCst);finished.await.unwrap();incoming.close();
                drop(io);drop(incoming);
                let native=peer_original.close_accepted(&claim,end).await.unwrap().expect("same actual accepted native close");
                assert!(native.matches(&claim));
                drop(listener);
                let report=peer_original.wait_closed(end).await.unwrap();
                assert_eq!(peer_original.confirmed_close_count(),1);
                assert!(!peer_original.native_unconfirmed());
                let evidence=peer_original.accepted_evidence().unwrap();
                assert!(evidence.native_confirmed());
                // The existing server library owns its own driver API. A native
                // descriptor receipt is deliberately not a fabricated Scope join.
                assert!(matches!(evidence.users,AcceptedTcpUsers::Unaccounted));
                assert!(!report.is_complete());
            });
            let selection=EgressInterfaceControl::default().select_for_peer_with_policy(address,&Default::default()).unwrap();
            assert_eq!(selection.binding_reason(),zero_platform_tokio::EgressBindingReason::Loopback);
            assert_eq!(selection.route_lookup_status(),zero_platform_tokio::EgressRouteLookupStatus::Skipped);
            // The exact original control precedes the first connect await.
            let(mut pending,mut client_original)=PendingTcpConnect::prepare(address,&Default::default(),&selection,CancellationToken::new(),None).unwrap();
            let raw=pending.connect(end).await.unwrap();
            let nodelay=raw.nodelay().expect("same connected original native option");
            assert!(nodelay,"actual production PendingTcpConnect must configure TCP_NODELAY");
            let(raw,observation)=wire::Wire::new(raw,nodelay,multi_mode,"client",clock);native.lock().unwrap().push(observation);
            let io=connect_grpc_inline_with_settings(raw,&profile,"localhost",None).await.unwrap();
            phase.store(if multi_mode{11}else{1},std::sync::atomic::Ordering::SeqCst);
            assert!(io.inline.is_some() && io.driver.is_none() && io.scoped.is_none());
            let mut io=Counted{io,received:received.clone()};
            AsyncWriteExt::write_all(&mut io,&vec![0x79;2*1024*1024+13]).await.unwrap();phase.store(if multi_mode{12}else{2},std::sync::atomic::Ordering::SeqCst);
            AsyncWriteExt::shutdown(&mut io).await.unwrap();phase.store(if multi_mode{13}else{3},std::sync::atomic::Ordering::SeqCst);
            let mut body=Vec::new();io.read_to_end(&mut body).await.unwrap();phase.store(if multi_mode{14}else{4},std::sync::atomic::Ordering::SeqCst);
            assert_eq!(body,vec![0x79;2*1024*1024+13]);done.send(()).unwrap();drop(io);
            let native=client_original.wait_closed(end).await.unwrap();
            assert!(native.is_complete());assert_eq!(native.closed_io.len(),1);
            server.await.unwrap();
        }
    }).await.unwrap_or_else(|_|panic!("actual inline phase {} peer {} received {} of 2097165 (1 connected/2 uploaded/3 halfclosed/4 reverseEOF; +10 multi; peer 1 accepted/2 EOF/3 uploaded/4 halfclosed) native={:#?}",phase.load(std::sync::atomic::Ordering::SeqCst),peer_phase.load(std::sync::atomic::Ordering::SeqCst),received.load(std::sync::atomic::Ordering::SeqCst),native.lock().unwrap().iter().map(|state|state.snapshot()).collect::<Vec<_>>()));
    // Actual per-connection wire diagnostics only, no native/Scope close claim.
}

struct Counted {
    io: GrpcStream,
    received: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        out: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = out.filled().len();
        let result = Pin::new(&mut self.io).poll_read(cx, out);
        self.received.fetch_add(
            out.filled().len() - before,
            std::sync::atomic::Ordering::SeqCst,
        );
        result
    }
}
impl AsyncWrite for Counted {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

#[derive(Default)]
struct Wakes(std::sync::atomic::AtomicUsize, tokio::sync::Notify);
impl futures_util::task::ArcWake for Wakes {
    fn wake_by_ref(this: &std::sync::Arc<Self>) {
        this.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        this.1.notify_one();
    }
}
#[tokio::test]
async fn split_real_capacity_wakes_the_writer_after_a_distinct_pending_reader_is_cancelled() {
    use futures_util::task::waker_ref;
    use std::{
        future::Future,
        sync::{atomic::Ordering, Arc},
        task::Context,
    };
    timeout(Duration::from_secs(8),async {
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
        let profile=OwnedGrpcProfile{service_names:vec!["split/Tun".into()],..Default::default()};let server_profile=profile.clone();
        let(full,filled)=tokio::sync::oneshot::channel();let(release,released)=tokio::sync::oneshot::channel();let(done,finished)=tokio::sync::oneshot::channel();
        let server=tokio::spawn(async move {
            let(io,_)=listener.accept().await.unwrap();let incoming=accept_grpc_connection(io,&server_profile).unwrap();let mut io=incoming.accept().await.unwrap();
            // The actual bounded peer hunk queue has consumed every credit.
            timeout(Duration::from_secs(3),async {while io.read_rx.len()<64 {tokio::task::yield_now().await;}}).await.unwrap();
            full.send(()).unwrap();released.await.unwrap();let mut received=Vec::new();io.read_to_end(&mut received).await.unwrap();
            assert_eq!(received,vec![0x63;8*1024*1024+7]);AsyncWriteExt::write_all(&mut io,b"done").await.unwrap();AsyncWriteExt::shutdown(&mut io).await.unwrap();
            finished.await.unwrap();incoming.close();
        });
        let raw=tokio::net::TcpStream::connect(address).await.unwrap();let stream=connect_grpc_inline_with_settings(raw,&profile,"localhost",None).await.unwrap();
        let(mut reader,mut writer)=tokio::io::split(stream);let payload=vec![0x63;8*1024*1024+7];
        let mut writing=Box::pin(AsyncWriteExt::write_all(&mut writer,&payload));
        tokio::select! {result=&mut writing=>panic!("peer capacity was held, upload finished early: {result:?}"),_=filled=>{}}
        let writer_wakes=Arc::new(Wakes::default());let writer_waker=waker_ref(&writer_wakes);
        assert!(writing.as_mut().poll(&mut Context::from_waker(&writer_waker)).is_pending());
        let before=writer_wakes.0.load(Ordering::SeqCst);
        let reader_wakes=Arc::new(Wakes::default());let reader_waker=waker_ref(&reader_wakes);let mut byte=[0];
        {let reading=AsyncReadExt::read(&mut reader,&mut byte);tokio::pin!(reading);assert!(reading.as_mut().poll(&mut Context::from_waker(&reader_waker)).is_pending());}
        // This read's registered task no longer polls. Only the actual peer's
        // released H2 credit can unblock the still-held upload. Do not repoll
        // writing until its previously registered real waker has been called.
        release.send(()).unwrap();
        timeout(Duration::from_secs(3),async {
            loop {let changed=writer_wakes.1.notified();tokio::pin!(changed);changed.as_mut().enable();
                if writer_wakes.0.load(Ordering::SeqCst)>before {break;}changed.await;}
        }).await.expect("original capacity must wake the writer independently of the cancelled reader");
        writing.await.unwrap();AsyncWriteExt::shutdown(&mut writer).await.unwrap();let mut response=Vec::new();reader.read_to_end(&mut response).await.unwrap();assert_eq!(response,b"done");
        drop(reader.unsplit(writer));done.send(()).unwrap();server.await.unwrap();
    }).await.unwrap();
}
