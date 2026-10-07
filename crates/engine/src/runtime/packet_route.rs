//! Executed path facts and cancellation, registered by the owning runtime.
use super::Engine;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
};
mod signal;
use signal::Signal;
use zero_api::*;
const MAX_ROUTES: usize = 4_096;
#[derive(Debug, Default)]
pub(super) struct PacketRoutes {
    next: AtomicU64,
    entries: Mutex<BTreeMap<String, Weak<Control>>>,
}
#[derive(Debug)]
struct Control {
    snapshot: PacketRouteSnapshot,
    close: Signal,
    released: Signal,
    responses: Signal,
}
/// Owning lease. Dropping it releases execution and its public fact.
pub struct PacketRouteLease {
    control: Arc<Control>,
}
#[derive(Clone)]
pub struct PacketRouteControl {
    control: Arc<Control>,
}
impl PacketRouteLease {
    pub fn control(&self) -> PacketRouteControl {
        PacketRouteControl {
            control: self.control.clone(),
        }
    }
}
impl Drop for PacketRouteLease {
    fn drop(&mut self) {
        self.control.close.set(true);
        self.control.released.set(true);
    }
}
impl PacketRouteControl {
    pub fn is_closed(&self) -> bool {
        self.control.close.get()
    }
    pub async fn cancelled(&self) {
        self.control.close.wait().await;
    }
    pub fn responses_started(&self) {
        self.control.responses.set(false);
    }
    pub fn responses_stopped(&self) {
        self.control.close.set(true);
        self.control.responses.set(true);
    }
    pub async fn wait_released(&self) {
        self.control.released.wait().await;
        self.control.responses.wait().await;
    }
}
impl Engine {
    pub fn register_packet_route(
        &self,
        mut snapshot: PacketRouteSnapshot,
    ) -> ApiResult<PacketRouteLease> {
        let mut entries = self
            .packet_routes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, e| {
            e.upgrade()
                .is_some_and(|c| !(c.released.get() && c.responses.get()))
        });
        if entries.len() >= MAX_ROUTES {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "packet route limit reached",
            ));
        }
        snapshot.route_id = format!(
            "{}:packet:{}",
            self.core_instance_id(),
            self.packet_routes.next.fetch_add(1, Ordering::Relaxed)
        );
        snapshot.core_instance_id = self.core_instance_id().to_owned();
        let basis = self.runtime_snapshot();
        snapshot.config_revision = basis.config_revision();
        snapshot.started_at_unix_ms = super::started_at_unix_ms();
        snapshot.endpoints = basis
            .config()
            .endpoint_bindings()
            .into_iter()
            .filter(|b| {
                b.inbound_tags.contains(&snapshot.inbound_tag)
                    || b.outbound_tags.contains(&snapshot.outbound_tag)
            })
            .map(|b| {
                let generation = self
                    .endpoint_snapshot_in(
                        &basis,
                        &EndpointGetQuery {
                            endpoint_id: b.endpoint_id.clone(),
                        },
                    )
                    .ok()
                    .and_then(|e| e.generation);
                PacketRouteEndpointRef {
                    endpoint_id: b.endpoint_id,
                    generation,
                }
            })
            .collect();
        let control = Arc::new(Control {
            snapshot,
            close: Signal::new(false),
            released: Signal::new(false),
            responses: Signal::new(true),
        });
        entries.insert(control.snapshot.route_id.clone(), Arc::downgrade(&control));
        Ok(PacketRouteLease { control })
    }
    pub fn packet_routes_snapshot(&self, query: &PacketRouteListQuery) -> PacketRouteListSnapshot {
        let mut entries = self
            .packet_routes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        entries.retain(|_, e| {
            e.upgrade()
                .is_some_and(|c| !(c.released.get() && c.responses.get()))
        });
        let matched: Vec<_> = entries
            .values()
            .filter_map(Weak::upgrade)
            .filter(|c| {
                query
                    .inbound_tag
                    .as_ref()
                    .is_none_or(|tag| tag == &c.snapshot.inbound_tag)
                    && query
                        .outbound_tag
                        .as_ref()
                        .is_none_or(|tag| tag == &c.snapshot.outbound_tag)
                    && query
                        .endpoint_id
                        .as_ref()
                        .is_none_or(|id| c.snapshot.endpoints.iter().any(|e| &e.endpoint_id == id))
            })
            .collect();
        let total = matched.len();
        let limit = query.limit.unwrap_or(100).clamp(1, 1000);
        let routes: Vec<_> = matched
            .into_iter()
            .skip(query.offset)
            .take(limit)
            .map(|c| project(&c))
            .collect();
        let end = query.offset.saturating_add(routes.len());
        PacketRouteListSnapshot {
            core_instance_id: self.core_instance_id().into(),
            config_revision: self.config_revision(),
            observed_at_unix_ms: super::started_at_unix_ms(),
            routes,
            total,
            next_offset: (end < total).then_some(end),
        }
    }
    fn packet_route_control(&self, id: &str) -> ApiResult<Arc<Control>> {
        self.packet_routes
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .and_then(Weak::upgrade)
            .filter(|c| !(c.released.get() && c.responses.get()))
            .ok_or_else(|| ApiError::new(ApiErrorCode::NotFound, "packet route no longer active"))
    }
    pub fn packet_route_snapshot(
        &self,
        query: &PacketRouteGetQuery,
    ) -> ApiResult<PacketRouteSnapshot> {
        self.packet_route_control(&query.route_id)
            .map(|c| project(&c))
    }
    pub fn begin_close_packet_route(
        &self,
        command: &PacketRouteCloseCommand,
    ) -> ApiResult<(PacketRouteSnapshot, PacketRouteControl)> {
        if command.expected_core_instance_id != self.core_instance_id() {
            return Err(ApiError::new(ApiErrorCode::Conflict, "stale core instance"));
        }
        if command
            .expected_config_revision
            .is_some_and(|r| r != self.config_revision())
        {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "stale configuration revision",
            ));
        }
        let control = self.packet_route_control(&command.route_id)?;
        if !control.close.claim() {
            return Err(ApiError::new(
                ApiErrorCode::Conflict,
                "packet route already closing; query current state",
            ));
        }
        Ok((project(&control), PacketRouteControl { control }))
    }
    pub fn record_packet_route_closed(
        &self,
        mut snapshot: PacketRouteSnapshot,
    ) -> PacketRouteSnapshot {
        snapshot.state = PacketRouteState::Closed;
        self.event_log.push_generated(ApiEvent::new(
            format!("{}:closed", snapshot.route_id),
            event_type::PACKET_ROUTE_CLOSED,
            super::started_at_unix_ms(),
            serde_json::json!({ "route": snapshot }),
        ));
        snapshot
    }
}
fn project(control: &Control) -> PacketRouteSnapshot {
    let mut s = control.snapshot.clone();
    s.state = if control.close.get() {
        PacketRouteState::Closing
    } else {
        PacketRouteState::Active
    };
    s
}
