use super::*;
use crate::{
    contract::{
        component::{GrantedMetadataListener, ResourceDeclaration},
        lifecycle::{BoxChannelControl, CloseReport, DynChannelControl},
        network::{CarrierContext, UpstreamBinding, UpstreamOpener},
        scope::{TaskJoinWitness, TaskRegistrar, TaskScope},
        CancellationToken, CloseReason,
    },
    controlled_carrier::CarrierOriginalTotals,
    controlled_datagram::metadata::{MetadataLimits, MetadataTransmit},
};
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::Poll,
    time::Duration,
};

struct Retained {
    permit: MetadataPoolPermit,
    tasks: TaskRegistrar,
    wire: Mutex<Option<GrantedMetadataListener>>,
    released: AtomicBool,
    changed: tokio::sync::Notify,
}
struct Control {
    original: Arc<Retained>,
    fault: Option<ChannelError>,
}
impl DynChannelControl for Control {
    fn cancel(&self, _: CloseReason) {}
    fn wait_closed<'a>(
        &'a mut self,
        _: tokio::time::Instant,
    ) -> crate::contract::BoxFuture<'a, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            if let Some(source) = self.fault.take() {
                return Err(source);
            }
            loop {
                let changed = self.original.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.original.released.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
            let mut wire = self.original.wire.lock().unwrap().take();
            let mut claim = wire.as_mut().and_then(|wire| wire.users_claim.take());
            drop(wire);
            self.original
                .permit
                .return_unbound(&mut claim, &self.original.tasks)?;
            Ok(CloseReport::default())
        })
    }
}
fn limits() -> MetadataLimits {
    let one = NonZeroUsize::new(1).unwrap();
    MetadataLimits {
        main_sockets: one,
        probe_sockets: one,
        probe_attempts_per_initialization: NonZeroUsize::new(4).unwrap(),
        concurrent_send_calls: one,
        write_pollers: one,
    }
}
fn instance() -> ComponentInstanceId {
    use zero_core::component::{ComponentId, ResourceGeneration};
    ComponentInstanceId {
        component: ComponentId(993),
        generation: ResourceGeneration(2),
    }
}
fn os_source(path: &std::path::Path) -> Arc<io::Error> {
    Arc::new(std::fs::File::open(path).unwrap_err())
}

#[path = "metadata_cohort/roundtrip.rs"]
mod roundtrip;

#[path = "metadata_cohort/poison.rs"]
mod poison;
