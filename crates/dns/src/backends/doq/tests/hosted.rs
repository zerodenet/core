use super::*;
use crate::{
    DnsConnectContext, DnsOutboundCarrierFuture, DnsOutboundConnectFuture, DnsOutboundConnector,
    DnsQuicBidiConnector, DnsQuicBidiRequest,
};
use std::sync::Mutex;
struct Refused {
    calls: Mutex<Vec<(DnsQuicBidiRequest, DnsConnectContext)>>,
    source: Arc<io::Error>,
}
impl std::fmt::Debug for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ActualProviderRefusal")
    }
}
impl DnsQuicBidiConnector for Refused {
    fn connect_bidi(
        &self,
        request: DnsQuicBidiRequest,
        context: DnsConnectContext,
    ) -> DnsOutboundCarrierFuture {
        self.calls.lock().unwrap().push((request, context));
        let source = self.source.clone();
        Box::pin(async move { Err(io::Error::other(source)) })
    }
}
impl DnsOutboundConnector for Refused {
    fn quic_bidi(&self) -> Option<&dyn DnsQuicBidiConnector> {
        Some(self)
    }
    fn connect(&self, _: String, _: SocketAddr) -> DnsOutboundConnectFuture {
        Box::pin(async {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "no TCP detour grant",
            ))
        })
    }
}
#[tokio::test]
async fn selected_host_quic_failure_keeps_actual_candidates_and_source_without_naked_udp_fallback()
{
    let (server, mut resolver) = fixture();
    let first = resolver.addrs[0];
    let second = SocketAddr::new(first.ip(), first.port().checked_add(1).unwrap_or(1));
    resolver.addrs.push(second);
    let source = Arc::new(io::Error::new(
        io::ErrorKind::Unsupported,
        "actual selected provider has no canonical metadata grant",
    ));
    let connector = Refused {
        calls: Mutex::default(),
        source: source.clone(),
    };
    let query = crate::message::build_query("hosted.doq.example", crate::message::TYPE_A).unwrap();
    let backend = crate::backends::ResolverBackend::Doq(resolver);
    let error = backend
        .exchange(
            &query,
            None,
            Some(&connector),
            zero_traits::AddressFamily::Auto,
        )
        .await
        .unwrap_err();
    let returned = error
        .get_ref()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(returned, &source));
    {
        let calls = connector.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        for ((request, context), endpoint) in calls.iter().zip([first, second]) {
            assert_eq!(request.endpoint, endpoint);
            assert_eq!(request.server_name, "localhost");
            assert_eq!(request.alpn, vec![b"doq".to_vec()]);
            assert!(context.deadline.is_some());
            assert!(
                context.cancel.is_cancelled(),
                "completed attempt drops only its query cancellation guard"
            );
        }
    }
    let crate::backends::ResolverBackend::Doq(resolver) = backend else {
        unreachable!()
    };
    assert!(
        resolver.owners.lock().unwrap().doq_snapshot().is_empty(),
        "selected capability failure never constructed a legacy raw UDP owner"
    );
    server.close(0u32.into(), b"test finished");
    server.wait_idle().await;
}
