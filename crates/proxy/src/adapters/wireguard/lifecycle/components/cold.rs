//! Same-pass WG donor lookup, scoped to cold preparation and never IO authority.
use super::super::recipe::carrier::Graph;
use super::*;
pub(in crate::adapters::wireguard) struct ColdGraphs {
    candidate: std::sync::Weak<zero_config::RuntimeConfig>,
    graphs: HashMap<String, Graph>,
}
pub(super) struct Guard<'a>(&'a WireguardAdapter);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let retired = self.0.cold_graphs.lock().unwrap().take();
        drop(retired);
    }
}
impl WireguardAdapter {
    pub(super) fn begin_cold_graphs<'a>(
        &'a self,
        config: &Arc<zero_config::RuntimeConfig>,
    ) -> Result<Guard<'a>, EngineError> {
        let mut current = self.cold_graphs.lock().unwrap();
        if current.is_some() {
            return Err(invalid("another WG cold graph pass is active"));
        }
        *current = Some(ColdGraphs {
            candidate: Arc::downgrade(config),
            graphs: HashMap::new(),
        });
        Ok(Guard(self))
    }
    pub(super) fn remember_cold_graph(&self, tag: &str, graph: Graph) -> Result<(), EngineError> {
        let mut current = self.cold_graphs.lock().unwrap();
        let pass = current
            .as_mut()
            .ok_or_else(|| invalid("WG cold pass is absent"))?;
        if pass.graphs.contains_key(tag) {
            return Err(invalid("duplicate same-pass WG donor"));
        }
        pass.graphs.insert(tag.into(), graph);
        Ok(())
    }
    pub(in crate::adapters::wireguard::lifecycle) fn cold_graph(
        &self,
        tag: &str,
        resources: &crate::component::ResourcePreparation<'_>,
    ) -> Option<Graph> {
        let current = self.cold_graphs.lock().unwrap();
        let pass = current.as_ref()?;
        let candidate = pass.candidate.upgrade()?;
        if !resources
            .candidate()
            .is_some_and(|actual| std::ptr::eq(actual, candidate.as_ref()))
        {
            return None;
        }
        pass.graphs.get(tag).cloned()
    }
}
