//! Source query quota is released only by the two actual DNS originals.
use super::*;
use crate::runtime::inbound_owners::InboundOwnerRegistry;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::DnsResolver;

#[tokio::test]
async fn source_resolution_waits_real_queries_and_preserves_pending_tcp_caller() {
    timeout(Duration::from_secs(10), async {
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
            "runtime":{"native_dns_max_originals":2,"dns":{
                "servers":{"wire":{"type":"udp","host":"127.0.0.1",
                    "port":peer.local_addr().unwrap().port()}},"default_server":"wire"}},
            "route":{"final":{"type":"direct"}}
        }).to_string()).unwrap();
        let proxy = crate::Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()), proxy.clone());
        handle.apply_runtime_config_and_wait(config, Duration::from_secs(5)).await.unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (reader, writer) = tokio::join!(
            tokio::net::TcpStream::connect(listener.local_addr().unwrap()), listener.accept());
        let mut reader = reader.unwrap();
        let mut writer = writer.unwrap().0;
        let registry = Arc::new(InboundOwnerRegistry::default());
        let (stop, _) = watch::channel(false);
        let installed = registry.install("dns-source", &proxy.protocols, 1, stop).unwrap();
        let host = installed.tasks();
        let owner = installed.owner();
        let tasks = installed.child_tasks(NonZeroUsize::new(1).unwrap()).unwrap();
        let native = NativeObligations::new(1, 1, 1);
        native.bind_source(installed.source_registrar().scope_identity()).unwrap();
        installed.register(native.resource()).unwrap();
        let mut tcp = native.reserve(ChildKind::Tcp).unwrap();
        let caller = tcp.registration();
        let witness = caller.clone();
        let (stop_parent, stopped) = oneshot::channel();
        owner.spawn_driver(async move {
            stopped.await.map_err(std::io::Error::other)?;
            Ok(())
        }).unwrap();
        let (send, pending) = oneshot::channel();
        tasks.spawn_bound(&host, move |binding| async move {
            binding.wait_bound().await.map_err(std::io::Error::other)?;
            tcp.bind_caller(binding).map_err(std::io::Error::other)?;
            tcp.enter().map_err(std::io::Error::other)?;
            let mut byte = [0];
            let read = reader.read_exact(&mut byte);
            tokio::pin!(read);
            std::future::poll_fn(|cx| {
                assert!(read.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            }).await;
            send.send(witness.caller_witness().unwrap()).unwrap();
            read.await?;
            drop(tcp);
            Ok(())
        }).unwrap();
        let actual = pending.await.unwrap();

        let call = proxy.resolver.resolution_call();
        let work = call.scope(proxy.resolver.resolve("source.actual.example"));
        let permit = caller.bind_dns_resolution(call.clone(), Instant::now() + Duration::from_secs(3)).await.unwrap();
        tokio::pin!(work);
        let mut types = Vec::new();
        for _ in 0..2 {
            let mut bytes = [0; 512];
            let (size, _) = tokio::select! {
                packet = peer.recv_from(&mut bytes) => packet.unwrap(),
                result = &mut work => panic!("lookup returned before upstream response: {result:?}"),
            };
            let question = zero_dns::udp::parse_dns_question(&bytes[..size]).unwrap();
            assert_eq!(question.domain, "source.actual.example");
            types.push(question.query_type);
        }
        types.sort_unstable();
        assert_eq!(types, [1, 28]);
        let originals = loop {
            let pending = proxy.resolver.native_query_observations().into_iter()
                .filter(|entry| entry.io_pending && entry.original.is_some()).collect::<Vec<_>>();
            if pending.len() == 2 { break pending; }
            tokio::task::yield_now().await;
        };
        let dns = permit.registration();
        drop(permit);
        assert!(dns.settle_dns(Instant::now()).await.is_err());
        assert_eq!(native.reserve(ChildKind::Dns).err().unwrap().reason, Reason::Capacity);
        assert!(!actual.is_joined());
        call.cancel();
        assert_eq!(work.await.unwrap_err().kind(), std::io::ErrorKind::Interrupted);
        native.settle_joined(Instant::now() + Duration::from_secs(3)).await.unwrap();
        let closed = call.closed().unwrap();
        assert!(closed.matches(&call));
        assert_eq!(closed.queries().iter().filter(|q| q.query().is_some()).count(), 2);
        for entry in originals {
            let original = entry.original.unwrap();
            assert!(original.closed().unwrap().matches(&original));
            assert!(entry.task.try_joined().unwrap().registration_released());
        }
        assert!(native.reserve(ChildKind::Dns).is_ok());
        assert!(!actual.is_joined(), "query return cannot join the actual pending TCP read");
        assert_eq!(native.0.lock().unwrap().records.len(), 1, "TCP native debt stays charged");
        writer.write_all(&[1]).await.unwrap();
        stop_parent.send(()).unwrap();
        assert!(registry.confirm_shutdown("dns-source", Instant::now() + Duration::from_secs(2)).await.is_err());
        assert!(actual.is_joined());
        assert_eq!(native.0.lock().unwrap().records.len(), 1, "real Scope join cannot fabricate TCP native close");
        running.shutdown().await.unwrap();
    }).await.unwrap();
}
