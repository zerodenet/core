//! Ordinary connect-only target work and cold maintenance fault recovery.
use super::*;
use crate::runtime::Proxy;
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    time::{timeout, Duration, Instant},
};
fn owns(
    error: &(dyn std::error::Error + 'static),
    original: &Arc<zero_platform_tokio::contract::ChannelError>,
) -> bool {
    if let Some(fault) = error.downcast_ref::<zero_runtime::ProbeOwnershipFailure>() {
        if Arc::ptr_eq(fault.original(), original) {
            return true;
        }
    }
    if let Some(actual) = error.downcast_ref::<Arc<zero_platform_tokio::contract::ChannelError>>() {
        if Arc::ptr_eq(actual, original) {
            return true;
        }
    }
    if let Some(zero_engine::EngineError::Io(source)) =
        error.downcast_ref::<zero_engine::EngineError>()
    {
        if owns(source, original) {
            return true;
        }
    }
    if let Some(source) = error.downcast_ref::<std::io::Error>() {
        if source
            .get_ref()
            .is_some_and(|source| owns(source, original))
        {
            return true;
        }
    }
    error.source().is_some_and(|source| owns(source, original))
}
#[tokio::test]
async fn actual_cold_target_panic_keeps_native_source_and_recovers_through_same_public_maintenance_owner(
) {
    timeout(Duration::from_secs(5),async{
  let listener=TcpListener::bind("127.0.0.1:0").await.unwrap();let address=listener.local_addr().unwrap();
  let config=zero_config::RuntimeConfig::parse(r#"{"runtime":{"native_stream_max_originals":1,"carrier_max_total_originals":1,"diagnostic_max_operations":1},"route":{"final":{"type":"direct"}}}"#).unwrap();
  let proxy=Proxy::new(config).unwrap();let actual=proxy.clone();
  let (entered,installed)=tokio::sync::oneshot::channel();let(release,pending)=tokio::sync::oneshot::channel::<()>();
  let first=tokio::spawn(async move{let owner=actual.clone();actual.with_diagnostic_service(async move{
   let services=owner.tcp_runtime_services();let host=services.protocols().runtime_host();let config=services.snapshot().config().clone();
   host.run_probe(&config,None,zero_runtime::reserve_probe().await.unwrap(),
    zero_core::request::ServicePurpose::DiagnosticProbe,Instant::now()+Duration::from_secs(2),move|call|async move{
     crate::component::direct_bridge::connect_target(&services,address.ip().to_string(),address.port(),call.clone()).await.unwrap();
     entered.send(call).unwrap();pending.await.unwrap();panic!("actual cold target task panic");#[allow(unreachable_code)] ()
    }).await
  }).await});
  let(mut peer,_)=listener.accept().await.unwrap();let call=installed.await.unwrap();let witness=call.witness().unwrap();
  let config=proxy.engine().runtime_snapshot().config().clone();
  let old=proxy.protocols.prepare_components(config.clone()).unwrap();let slot=old.direct(None).unwrap();
  let carrier=old.original_carrier_owner(slot.instance_id(),0).unwrap();
  let observation=carrier.observe().unwrap();assert_eq!(observation.retained(),1);
  let original=observation.slots()[0].original().clone();drop(observation);
  assert!(!witness.is_joined());
  release.send(()).unwrap();let returned=first.await.unwrap().unwrap_err();
  let joined=witness.try_joined().unwrap();let source=joined.result().unwrap_err();
  assert!(joined.registration_released());assert!(owns(&returned,&source),"actual receiver lost the same task source: {returned:?}");
  let closed=original.closed().unwrap();assert!(original.matches_closed(&closed));
  assert_eq!(carrier.observe().unwrap().retained(),0);
  let mut bytes=[0;1];assert_eq!(peer.read(&mut bytes).await.unwrap(),0,"fixed target performs TCP connect only");
  assert!(proxy.protocols.current_components(&config).is_none());
  let fresh=proxy.protocols.prepare_components(config.clone()).unwrap();assert!(!Arc::ptr_eq(&old,&fresh));
  let actual=proxy.clone();let next=tokio::spawn(async move{let owner=actual.clone();actual.with_diagnostic_service(async move{
   OutboundProbeRuntime::new(owner.tcp_runtime_services()).probe_server_endpoint(address.ip().to_string(),address.port(),None).await
  }).await});
  let(mut peer,_)=listener.accept().await.unwrap();assert!(next.await.unwrap().unwrap().is_ok());
  assert_eq!(peer.read(&mut bytes).await.unwrap(),0);assert!(proxy.protocols.current_components(&config).is_none());
 }).await.unwrap();
}
