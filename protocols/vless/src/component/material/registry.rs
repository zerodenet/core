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
pub(super) struct Census {
    records: Mutex<Vec<(ComponentInstanceId, Weak<Material>)>>,
    pub(super) cold: Mutex<HashMap<Vec<u8>, Weak<Material>>>,
}
impl Registry {
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
            .find(|(actual, _)| *actual == instance)
            .and_then(|(_, material)| material.upgrade())
    }
    pub(super) fn record(
        &self,
        instance: ComponentInstanceId,
        material: &Arc<Material>,
    ) -> Result<(), ChannelError> {
        let mut records = self.records.lock().unwrap();
        records.retain(|(_, material)| material.strong_count() != 0);
        if let Some((_, previous)) = records.iter().find(|(actual, _)| *actual == instance) {
            return if previous.ptr_eq(&Arc::downgrade(material)) {
                Ok(())
            } else {
                Err(error(instance))
            };
        }
        records
            .try_reserve(1)
            .map_err(|source| error(instance).with_source(source))?;
        records.push((instance, Arc::downgrade(material)));
        Ok(())
    }
}
