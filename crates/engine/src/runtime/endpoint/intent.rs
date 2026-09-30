//! Immutable endpoint policy for one published engine snapshot.

use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zero_api::{ApiError, ApiErrorCode, EndpointDirections, EndpointStateSource};
use zero_config::{EndpointBindingConfig, RuntimeConfig};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(in crate::runtime) struct Override {
    pub enabled: Option<bool>,
    pub directions: Option<EndpointDirections>,
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct Intent {
    pub binding: EndpointBindingConfig,
    pub overlay: Override,
    pub revision: u64,
}

impl Intent {
    pub fn enabled(&self) -> bool {
        self.overlay.enabled.unwrap_or(self.binding.enabled)
    }

    pub fn directions(&self) -> EndpointDirections {
        self.overlay.directions.unwrap_or(self.binding.directions)
    }

    pub fn source(&self) -> EndpointStateSource {
        if self.overlay == Override::default() {
            EndpointStateSource::Config
        } else {
            EndpointStateSource::RuntimeOverride
        }
    }

    pub fn correlated_returns(&self) -> bool {
        (self.binding.canonical || self.overlay.directions.is_some()) && !self.directions().inbound
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct EndpointIntents {
    pub entries: BTreeMap<String, Intent>,
    // Historical snapshots share only the allocator. Rollback must not reuse
    // a token that a reader could have observed during reconciliation.
    revisions: Arc<AtomicU64>,
    inbound_ids: BTreeMap<String, String>,
    outbound_ids: BTreeMap<String, String>,
}

impl EndpointIntents {
    pub fn for_config(config: &RuntimeConfig, previous: Option<&Self>) -> Result<Self, ApiError> {
        let mut next = Self {
            revisions: previous.map_or_else(Default::default, |state| state.revisions.clone()),
            ..Self::default()
        };
        for binding in config.endpoint_bindings() {
            let old = previous.and_then(|state| state.entries.get(&binding.endpoint_id));
            let overlay = old.map_or_else(Override::default, |entry| entry.overlay.clone());
            if !binding
                .supported_directions
                .permits(overlay.directions.unwrap_or(binding.directions))
            {
                return Err(ApiError::new(
                    ApiErrorCode::InvalidArgument,
                    format!(
                        "endpoint `{}` reload cannot satisfy its retained direction override",
                        binding.endpoint_id
                    ),
                ));
            }
            let revision = if let Some(old) = old.filter(|old| old.binding == binding) {
                old.revision
            } else {
                next.allocate_revision()?
            };
            for tag in &binding.inbound_tags {
                next.inbound_ids
                    .insert(tag.clone(), binding.endpoint_id.clone());
            }
            for tag in &binding.outbound_tags {
                next.outbound_ids
                    .insert(tag.clone(), binding.endpoint_id.clone());
            }
            next.entries.insert(
                binding.endpoint_id.clone(),
                Intent {
                    binding,
                    overlay,
                    revision,
                },
            );
        }
        Ok(next)
    }

    pub fn inbound(&self, tag: &str) -> Option<&Intent> {
        self.inbound_ids
            .get(tag)
            .and_then(|id| self.entries.get(id))
    }

    pub fn outbound(&self, tag: &str) -> Option<&Intent> {
        self.outbound_ids
            .get(tag)
            .and_then(|id| self.entries.get(id))
    }

    pub fn allocate_revision(&mut self) -> Result<u64, ApiError> {
        self.revisions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            })
            .map(|previous| previous + 1)
            .map_err(|_| {
                ApiError::new(ApiErrorCode::Internal, "endpoint intent revision exhausted")
            })
    }

    pub fn replace_override(
        &mut self,
        id: &str,
        overlay: Override,
        force: bool,
    ) -> Result<(), ApiError> {
        let entry = self.entries.get(id).ok_or_else(|| {
            ApiError::new(
                ApiErrorCode::NotFound,
                format!("endpoint `{id}` was not found"),
            )
        })?;
        if entry.overlay == overlay && !force {
            return Ok(());
        }
        let revision = self.allocate_revision()?;
        let entry = self.entries.get_mut(id).expect("validated endpoint");
        entry.overlay = overlay;
        entry.revision = revision;
        Ok(())
    }
}
