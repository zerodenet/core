//! Real typed Direct role, actual Engine/accepted/raw callers and registered C.
#![cfg(feature = "vmess")]
use crate::{
    component::{
        business_carrier::BusinessCarrierRequest, catalog::supervision::testing::TestContext,
        direct_bridge::network::DirectOpener, instance::ComponentSlot, ComponentRecipe,
        DirectFactory, DirectOptions,
    },
    Proxy,
};
use std::{
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::{timeout, Instant},
};
use zero_core::{
    component::*, request::Destination, Address, Network, ProtocolType, Session, TargetHostSource,
};
use zero_platform_tokio::{
    contract::{
        component::{
            BuildContext, ComponentFactory, ComponentInstance, ComponentSpec,
            GrantedDatagramListener, ResourceAccess, ResourceDeclaration,
        },
        context::{ConnectFact, ConnectObserver, OpenContext},
        lifecycle::BuildError,
        network::{UpstreamBinding, UpstreamTransport},
        packet::BoxPacketSession,
        scope::TaskRegistrar,
        stream::{ConnectRequest, PeerConfirmation},
        BoxFuture,
    },
    controlled_carrier::{CarrierOwnerBinding, CarrierOwnerRegistration},
};
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(3)
}
#[derive(Default)]
struct Captured(Mutex<Option<CarrierOwnerRegistration>>);
struct Factory(Arc<Captured>);
impl ComponentFactory for Factory {
    type Options = DirectOptions;
    fn describe(&self, options: &DirectOptions) -> ComponentSpec {
        DirectFactory.describe(options)
    }
    fn build(
        &self,
        options: Arc<DirectOptions>,
        mut context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        context.resources = Arc::new(Access {
            original: context.resources,
            captured: self.0.clone(),
        });
        DirectFactory.build(options, context)
    }
}
struct Access {
    original: Arc<dyn ResourceAccess>,
    captured: Arc<Captured>,
}
impl ResourceAccess for Access {
    fn register_controlled_carrier(
        &self,
        ordinal: usize,
        actual: CarrierOwnerRegistration,
        tasks: &TaskRegistrar,
    ) -> Result<CarrierOwnerBinding, BuildError> {
        let bound = self
            .original
            .register_controlled_carrier(ordinal, actual.clone(), tasks)?;
        *self.captured.0.lock().unwrap() = Some(actual);
        Ok(bound)
    }
    fn read_file(&self, path: PathBuf, max: usize) -> BoxFuture<'_, Result<Vec<u8>, BuildError>> {
        self.original.read_file(path, max)
    }
    fn listen_tcp(
        &self,
        destination: Destination,
    ) -> BoxFuture<'_, Result<zero_platform_tokio::ControlledTcpListener, BuildError>> {
        self.original.listen_tcp(destination)
    }
    fn listen_datagram(
        &self,
        destination: Destination,
    ) -> BoxFuture<'_, Result<GrantedDatagramListener, BuildError>> {
        self.original.listen_datagram(destination)
    }
    fn open_device(&self, name: Arc<str>) -> BoxFuture<'_, Result<BoxPacketSession, BuildError>> {
        self.original.open_device(name)
    }
}
#[derive(Default, Debug)]
struct Facts {
    facts: Mutex<Vec<(Option<SessionId>, ConnectFact)>>,
    outer: AtomicU64,
}
impl ConnectObserver for Facts {
    fn record(&self, session: Option<SessionId>, fact: ConnectFact) {
        self.facts.lock().unwrap().push((session, fact));
    }
}
impl zero_traits::IoObserver for Facts {
    fn received(&self, bytes: usize) {
        self.outer.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn sent(&self, bytes: usize) {
        self.outer.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
}
mod dns;
mod wire;
