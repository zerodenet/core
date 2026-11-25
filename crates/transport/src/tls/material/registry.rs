//! Weak runtime census of client TLS CA recipes and bound ECH resolvers.

use super::{component_error, ClientMaterial};
use crate::tls::ech::EchConfigResolver;
use std::{
    collections::HashMap,
    fmt, io,
    sync::{Arc, Mutex, Weak},
};
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::ChannelError;

#[derive(Clone, Default)]
pub struct ClientMaterialRegistry(Arc<Mutex<HashMap<Vec<u8>, Weak<Census>>>>);

impl fmt::Debug for ClientMaterialRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientMaterialRecipes")
            .finish_non_exhaustive()
    }
}

type Resolver = dyn EchConfigResolver;

struct Record {
    instance: ComponentInstanceId,
    material: Weak<ClientMaterial>,
    resolver: Option<Weak<Resolver>>,
}

pub(super) struct Census {
    records: Mutex<Vec<Record>>,
    pub(super) cold: Mutex<HashMap<Vec<u8>, Weak<ClientMaterial>>>,
}

pub struct BoundClientMaterial {
    pub material: Arc<ClientMaterial>,
    pub resolver: Option<Arc<Resolver>>,
}

impl ClientMaterialRegistry {
    /// Lookup the bound immutable material and optional ECH resolver for `instance`.
    pub fn bound(&self, instance: ComponentInstanceId) -> io::Result<Option<BoundClientMaterial>> {
        let roots = self
            .0
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let mut found = None;
        for root in roots {
            let record = root
                .records
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.instance == instance)
                .map(|record| {
                    (
                        record.material.upgrade(),
                        record.resolver.as_ref().map(Weak::upgrade),
                    )
                });
            if let Some((Some(material), resolver)) = record {
                let resolver = match resolver {
                    Some(Some(resolver)) => Some(resolver),
                    Some(None) => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotConnected,
                            "original client ECH owner retired",
                        ))
                    }
                    None => None,
                };
                if found.is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "conflicting client material instances",
                    ));
                }
                found = Some(BoundClientMaterial { material, resolver });
            }
        }
        Ok(found)
    }

    pub(super) fn access(&self, key: &[u8]) -> Arc<Census> {
        let mut roots = self.0.lock().unwrap();
        roots.retain(|_, root| root.strong_count() != 0);
        if let Some(root) = roots.get(key).and_then(Weak::upgrade) {
            return root;
        }
        let root = Arc::new(Census {
            records: Mutex::default(),
            cold: Mutex::default(),
        });
        roots.insert(key.to_vec(), Arc::downgrade(&root));
        root
    }
}

impl Census {
    pub(super) fn previous(&self, instance: ComponentInstanceId) -> Option<Arc<ClientMaterial>> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .find(|record| record.instance == instance)
            .and_then(|record| record.material.upgrade())
    }

    pub(super) fn record(
        &self,
        instance: ComponentInstanceId,
        material: &Arc<ClientMaterial>,
    ) -> Result<(), ChannelError> {
        self.record_bound(instance, material, None)
    }

    pub(super) fn record_bound(
        &self,
        instance: ComponentInstanceId,
        material: &Arc<ClientMaterial>,
        resolver: Option<&Arc<Resolver>>,
    ) -> Result<(), ChannelError> {
        let mut records = self.records.lock().unwrap();
        records.retain(|record| record.material.strong_count() != 0);
        if let Some(previous) = records.iter().find(|record| record.instance == instance) {
            return if previous.material.ptr_eq(&Arc::downgrade(material))
                && match (&previous.resolver, resolver) {
                    (None, None) => true,
                    (Some(previous), Some(actual)) => previous.ptr_eq(&Arc::downgrade(actual)),
                    _ => false,
                } {
                Ok(())
            } else {
                Err(component_error(instance))
            };
        }
        records
            .try_reserve(1)
            .map_err(|source| component_error(instance).with_source(source))?;
        records.push(Record {
            instance,
            material: Arc::downgrade(material),
            resolver: resolver.map(Arc::downgrade),
        });
        Ok(())
    }
}
