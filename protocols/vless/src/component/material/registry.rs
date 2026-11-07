//! Existing protocol runtime metadata; weak entries grant no ownership or IO.
use super::*;
use std::{collections::HashMap, sync::Weak};
#[derive(Clone, Default)]
pub(crate) struct Registry(Arc<Mutex<HashMap<Vec<u8>, Weak<Census>>>>);
impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VlessOriginalMaterialRecipes")
            .finish_non_exhaustive()
    }
}
type Resolver = dyn zero_transport::tls::ech::EchConfigResolver;
struct Record {
    instance: ComponentInstanceId,
    material: Weak<Material>,
    resolver: Option<Weak<Resolver>>,
}
pub(super) struct Census {
    records: Mutex<Vec<Record>>,
    pub(super) cold: Mutex<HashMap<Vec<u8>, Weak<Material>>>,
}
pub(crate) struct BoundMaterial {
    pub(crate) material: Arc<Material>,
    pub(crate) resolver: Option<Arc<Resolver>>,
}
impl Registry {
    /// The original runtime's exact installed instance, including callers
    /// which reconstruct a compatibility leaf over that same native MUX map.
    pub(crate) fn bound(
        &self,
        instance: ComponentInstanceId,
    ) -> std::io::Result<Option<BoundMaterial>> {
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
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::NotConnected,
                            "original VLESS ECH owner retired",
                        ))
                    }
                    None => None,
                };
                if found.is_some() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "conflicting VLESS material instances",
                    ));
                }
                found = Some(BoundMaterial { material, resolver });
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
    pub(super) fn previous(&self, instance: ComponentInstanceId) -> Option<Arc<Material>> {
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
        material: &Arc<Material>,
    ) -> Result<(), ChannelError> {
        self.record_bound(instance, material, None)
    }
    pub(super) fn record_bound(
        &self,
        instance: ComponentInstanceId,
        material: &Arc<Material>,
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
                Err(error(instance))
            };
        }
        records
            .try_reserve(1)
            .map_err(|source| error(instance).with_source(source))?;
        records.push(Record {
            instance,
            material: Arc::downgrade(material),
            resolver: resolver.map(Arc::downgrade),
        });
        Ok(())
    }
}
