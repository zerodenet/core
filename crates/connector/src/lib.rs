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
mod registry;
#[cfg(feature = "event-dispatcher")]
mod state;
#[cfg(feature = "webhook")]
mod webhook;

#[cfg(feature = "event-dispatcher")]
pub use dispatcher::{
    spawn_event_dispatcher, spawn_event_dispatcher_with_engine_started,
    spawn_event_dispatcher_with_engine_started_and_network, spawn_event_dispatcher_with_network,
    EventDispatcherHandle, EventDispatcherOptions, EventDispatcherStatusHandle,
};
pub use error::{ConnectorError, ConnectorResult};
#[cfg(feature = "event-dispatcher")]
pub use network::{
    EventDispatcherNetwork, EventSinkTcpConnectFuture, EventSinkTcpDialer, EventSinkTcpStream,
};
#[cfg(feature = "event-dispatcher")]
pub use state::{
    inspect_persistent_state, ConnectorStateFile, ConnectorStateReport, ConnectorStateStatus,
    CONNECTOR_STATE_REPORT_SCHEMA,
};
