use super::Portal;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Shared by authenticated listener profiles and prepared virtual outbounds.
#[derive(Clone, Default, Debug)]
pub struct PortalRegistry(Arc<Mutex<HashMap<String, Portal>>>);
impl PortalRegistry {
    pub fn portal(&self, tag: &str) -> Portal {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(tag.to_owned())
            .or_default()
            .clone()
    }
    /// Retired leaves cannot attach workers or recover a replacement pool.
    pub fn retire(&self) {
        for (_, portal) in self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .drain()
        {
            portal.retire();
        }
    }
}

impl PortalRegistry {
    pub(super) fn install_component(
        &self,
        tag: &str,
        instance: zero_core::component::ComponentInstanceId,
    ) -> Result<Portal, zero_platform_tokio::contract::ChannelError> {
        let mut portals = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let portal = portals.entry(tag.to_owned()).or_default();
        let mut retired = None;
        let old = *portal.0.generation.lock().unwrap();
        if old.is_some_and(|old| old != instance) {
            if (portal.0.ready.load(std::sync::atomic::Ordering::Acquire)
                && !portal.0.retired.load(std::sync::atomic::Ordering::Acquire))
                || !portal.0.workers.lock().unwrap().is_empty()
            {
                return Err(super::component::error(
                    zero_platform_tokio::contract::Stage::Prepare,
                    zero_platform_tokio::contract::Reason::StalePlan,
                ));
            }
            retired = Some(std::mem::take(portal));
        } else if old.is_none() && !portal.0.workers.lock().unwrap().is_empty() {
            return Err(super::component::error(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::StalePlan,
            ));
        }
        *portal.0.generation.lock().unwrap() = Some(instance);
        portal
            .0
            .managed
            .store(true, std::sync::atomic::Ordering::Release);
        let current = portal.clone();
        drop(portals);
        drop(retired);
        Ok(current)
    }
    pub fn retire_legacy(&self) {
        let snapshot = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .map(|(tag, portal)| (tag.clone(), portal.clone()))
            .collect::<Vec<_>>();
        let retire = snapshot
            .into_iter()
            .filter(|(_, portal)| {
                !portal.0.managed.load(std::sync::atomic::Ordering::Acquire)
                    || (portal.0.retired.load(std::sync::atomic::Ordering::Acquire)
                        && portal.0.workers.lock().unwrap().is_empty()
                        && portal
                            .0
                            .provider
                            .get()
                            .is_some_and(|provider| provider.returned().is_some()))
            })
            .collect::<Vec<_>>();
        let removed = {
            let mut portals = self.0.lock().unwrap_or_else(|error| error.into_inner());
            retire
                .into_iter()
                .filter_map(|(tag, original)| {
                    if portals
                        .get(&tag)
                        .is_some_and(|actual| Arc::ptr_eq(&actual.0, &original.0))
                    {
                        portals.remove(&tag)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };
        for portal in removed {
            portal.retire();
        }
    }
}
