//! Only the explicitly deferred process socket connector remains.
mod connector;
pub(crate) use connector::{
    ManagedDatagramSocketConnectorFlow, ManagedDatagramSocketResumeConnector,
};
