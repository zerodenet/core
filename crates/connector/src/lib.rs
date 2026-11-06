//! Optional node-side event delivery for panels and external controllers.
//! Receivers use Zero's event envelope; business policy and control commands
//! remain outside this delivery module.

#[cfg(feature = "event-dispatcher")]
mod dispatcher;
mod error;
#[cfg(feature = "event-dispatcher")]
mod jsonl;
#[cfg(feature = "event-dispatcher")]
mod network;
#[cfg(feature = "event-dispatcher")]
pub use network::EventNetworkLimits;
#[cfg(feature = "event-dispatcher")]
mod registry;
#[cfg(feature = "event-dispatcher")]
mod state;
#[cfg(feature = "webhook")]
mod webhook;

#[cfg(feature = "event-dispatcher")]
pub use dispatcher::{
    DispatcherRetirementFailure, EventDispatcherContinuation, EventDispatcherHandle,
    EventDispatcherOptions, EventDispatcherStatusHandle,
    prepare_event_dispatcher_paused_with_network, resume_event_dispatcher_with_network,
    spawn_event_dispatcher, spawn_event_dispatcher_with_engine_started,
    spawn_event_dispatcher_with_engine_started_and_network, spawn_event_dispatcher_with_network,
};
pub use error::{ConnectorError, ConnectorResult};
#[cfg(feature = "event-dispatcher")]
pub use network::{
    EventDispatcherNetwork, EventSinkTcpConnectFuture, EventSinkTcpDialer, EventSinkTcpStream,
};
#[cfg(feature = "event-dispatcher")]
pub use state::{
    CONNECTOR_STATE_REPORT_SCHEMA, ConnectorStateFile, ConnectorStateReport, ConnectorStateStatus,
    ConnectorStateInspection, inspect_persistent_state, inspect_persistent_state_with_source,
};

#[cfg(feature = "event-dispatcher")]
mod encoding;

#[cfg(feature = "event-dispatcher")]
mod resources;
