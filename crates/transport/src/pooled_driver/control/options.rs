//! Explicit owning setup metadata; no grant or fabricated caller witness.
use super::PoolOptions;
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::network::UpstreamBinding;
impl PoolOptions {
    /// Owning protocol accepts actual failed-auth IO into a finite noncached
    /// job on this same original. It is never a usable cached connection.
    pub fn with_preparation_jobs(mut self) -> Self {
        self.preparation_jobs = true;
        self.scoped_preparation = true;
        self.rebuild_identity();
        self
    }

    /// Only the actual owning protocol assembly opts into a real initializer
    /// in this same Scope. Fixed library callers retain inline preparation.
    pub fn with_scoped_preparation(mut self) -> Self {
        self.scoped_preparation = true;
        self.rebuild_identity();
        self
    }
    fn rebuild_identity(&mut self) {
        let mut identity = self.base_identity.to_vec();
        if self.scoped_preparation {
            identity.extend_from_slice(b"actual-scoped-preparation");
        }
        if self.preparation_jobs {
            identity.extend_from_slice(b"same-original-noncached-preparation-job-v1");
        }
        for dependency in &self.dependencies {
            identity.extend_from_slice(b"actual-node-dependency");
            identity.extend_from_slice(&dependency.0.to_le_bytes());
        }
        if let Some(dependency) = self.stream_dependency {
            identity.extend_from_slice(b"actual-stream-loan-dependency");
            identity.extend_from_slice(&dependency.0.to_le_bytes());
        }
        self.identity = identity.into();
    }
    /// Actual Node/ECH source prepared by the owning protocol adapter. Cold
    /// identity metadata never grants IO or substitutes for an actual provider.
    pub fn with_node_dependency(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.node_dependency = Some(dependency);
        self.rebuild_dependencies();
        self.rebuild_identity();
        self
    }
    /// Receive the lower stream from this exact actual dependency, including
    /// for an empty relay prefix. The owning adapter supplies its real lender;
    /// this declaration neither opens IO nor substitutes for a StreamLoan.
    pub fn with_stream_dependency(mut self, dependency: zero_core::component::ComponentId) -> Self {
        self.stream_dependency = Some(dependency);
        self.rebuild_dependencies();
        self.rebuild_identity();
        self
    }
    fn rebuild_dependencies(&mut self) {
        self.dependencies = self
            .node_dependency
            .into_iter()
            .chain(self.stream_dependency)
            .collect();
        self.dependencies.dedup();
    }
    /// Protocol-private cache reclamation runs only after actual closure. The
    /// callback grants no I/O authority and supplies no closure evidence.
    pub fn on_confirmed_close(
        mut self,
        reclaim: impl Fn(zero_core::component::ComponentInstanceId) + Send + Sync + 'static,
    ) -> Self {
        self.on_closed = Some(Arc::new(reclaim));
        self
    }
    pub fn endpoint(&self) -> &Destination {
        &self.endpoint
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
}
#[cfg(test)]
#[path = "../../../tests/pooled_driver/preparation.rs"]
mod tests;
