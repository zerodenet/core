//! Deadline while actual original driver capacity is occupied, without fake IO.
use super::*;
use std::{future::Future, num::NonZeroUsize, sync::Mutex, task::Poll};
use zero_platform_tokio::{
    contract::{packet::BoxPacketSession, scope::TaskScope, BoxFuture},
    controlled_datagram::metadata::{
        users::MetadataUsersClosedReceipt, MetadataDatagramControl, MetadataLimits, MetadataOwner,
        MetadataViewIssuer,
    },
};
fn n(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
struct Provider {
    native: Mutex<MetadataOwner>,
    control: Mutex<Option<MetadataDatagramControl>>,
    issuer: Mutex<Option<MetadataViewIssuer>>,
    received: Mutex<Option<MetadataUsersClosedReceipt>>,
    scope: zero_platform_tokio::contract::scope::TaskScopeIdentity,
}
impl ResourceAccess for Provider {
    fn listen_metadata_datagram(
        &self,
        request: MetadataDatagramRequest,
    ) -> BoxFuture<'_, Result<GrantedMetadataListener, ChannelError>> {
        Box::pin(async move {
            let grant = self
                .native
                .lock()
                .unwrap()
                .bind(request.source)
                .map_err(|e| failure(Reason::Io).with_source(e))?;
            let mut holder = self.control.lock().unwrap();
            assert!(holder.is_none());
            *holder = Some(grant.control);
            let control = holder.as_mut().unwrap();
            let (issuer, claim) = control.claim_views(self.scope.clone()).unwrap();
            *self.issuer.lock().unwrap() = Some(issuer);
            let observation = control.take_observation().unwrap();
            Ok(GrantedMetadataListener {
                scope: self.scope.clone(),
                rx: grant.rx,
                tx: grant.tx,
                observation,
                users_claim: Some(claim),
            })
        })
    }
    fn confirm_metadata_users(
        &self,
        ordinal: usize,
        receipt: &MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> Result<(), ChannelError> {
        assert_eq!(ordinal, 0);
        assert!(self
            .issuer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .validates_users(receipt, tasks));
        *self.received.lock().unwrap() = Some(receipt.clone());
        Ok(())
    }
    fn read_file(
        &self,
        _: std::path::PathBuf,
        _: usize,
    ) -> BoxFuture<'_, Result<Vec<u8>, ChannelError>> {
        Box::pin(async { Err(failure(Reason::Unsupported)) })
    }
    fn listen_tcp(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, ChannelError>> {
        Box::pin(async { Err(failure(Reason::Unsupported)) })
    }
    fn listen_datagram(
        &self,
        _: zero_core::request::Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, ChannelError>> {
        Box::pin(async { Err(failure(Reason::Unsupported)) })
    }
    fn open_device(&self, _: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, ChannelError>> {
        Box::pin(async { Err(failure(Reason::Unsupported)) })
    }
}
#[tokio::test]
async fn full_original_driver_quota_still_honors_accept_deadline_and_closes_real_native() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut scope = TaskScope::new(tokio::runtime::Handle::current(), n(1));
        let tasks = scope.registrar();
        let limits = MetadataLimits {
            main_sockets: n(1),
            probe_sockets: n(2),
            probe_attempts_per_initialization: n(8),
            concurrent_send_calls: n(1),
            write_pollers: n(2),
        };
        let provider = Arc::new(Provider {
            native: Mutex::new(MetadataOwner::new(limits).unwrap()),
            control: Mutex::new(None),
            issuer: Mutex::new(None),
            received: Mutex::new(None),
            scope: tasks.scope_identity(),
        });
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let server = quinn::ServerConfig::with_single_cert(
            vec![cert.cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
        )
        .unwrap();
        let (mut owner, mut source) = source(
            MetadataDatagramRequest {
                ordinal: 0,
                source: "127.0.0.1:0".parse().unwrap(),
                limits,
            },
            server,
            Vec::new(),
            OwnedEndpointLimits {
                drivers: n(1),
                users: n(2),
            },
            provider.clone(),
            tasks,
            CancellationToken::new(),
            |_| -> PendingInbound { panic!("full quota cannot deliver a fabricated incoming") },
        );
        owner
            .start(Instant::now() + std::time::Duration::from_secs(1))
            .await
            .unwrap();
        let mut waiting = Box::pin(source.next(AcceptContext {
            cancel: CancellationToken::new(),
            deadline: Some(Instant::now() + std::time::Duration::from_millis(30)),
        }));
        std::future::poll_fn(|cx| {
            assert!(waiting.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("finite accept budget includes full-capacity waits");
        assert!(matches!(result,Err(error) if error.reason==Reason::Timeout));
        scope.begin_shutdown();
        let (closed, count) = tokio::join!(
            owner.shutdown(Instant::now() + std::time::Duration::from_secs(2)),
            async {
                let mut count = 0;
                while let Some(exit) = scope.next_exit().await {
                    assert!(exit.result.is_ok());
                    count += 1;
                }
                count
            }
        );
        assert!(closed.unwrap().is_complete());
        assert_eq!(count, 1, "the actual endpoint driver joined");
        assert!(provider.received.lock().unwrap().is_some());
        assert_eq!(
            provider.native.lock().unwrap().occupied_main(),
            1,
            "user return has not closed native IO"
        );
        provider
            .control
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .close()
            .unwrap();
        let report = provider.native.lock().unwrap().take_report();
        assert_eq!(report.main.confirmed_closed, 1);
        assert!(report.is_complete());
    })
    .await
    .unwrap();
}
