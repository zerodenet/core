//! Per-establishment diagnostics. Never infer a dial address from a hostname.
use std::error::Error;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use zero_engine::EngineError;

#[derive(Clone, Default)]
pub(crate) struct DialTrace(Arc<Mutex<Option<DialObservation>>>);

#[derive(Debug, Clone)]
pub(crate) struct DialObservation {
    pub(crate) stage: &'static str,
    pub(crate) remote: Option<SocketAddr>,
    pub(crate) local: Option<SocketAddr>,
    pub(crate) candidates: Vec<SocketAddr>,
    pub(crate) attempts: Vec<zero_engine::FlowConnectionAttemptObservation>,
    pub(crate) os_error: Option<i32>,
}

impl DialTrace {
    pub(super) fn record(&self, observation: DialObservation) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = Some(observation);
    }

    pub(crate) fn snapshot(&self) -> Option<DialObservation> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

#[derive(Debug)]
pub(crate) struct EstablishmentFailure {
    pub(crate) tag: Option<String>,
    pub(crate) protocol: String,
    pub(crate) endpoint: Option<(String, u16)>,
    pub(crate) stage: &'static str,
    pub(crate) dial: Option<DialObservation>,
    source: EngineError,
}

impl EstablishmentFailure {
    pub(crate) fn wrap(
        source: EngineError,
        tag: Option<String>,
        protocol: String,
        endpoint: Option<(String, u16)>,
        stage: &'static str,
        dial: Option<DialObservation>,
    ) -> EngineError {
        // A failed relay carrier already identifies the hop that actually failed.
        if establishment_failure(&source).is_some() {
            return source;
        }
        let stage = dial.as_ref().map_or(stage, |dial| {
            if dial.stage == "connected" {
                stage
            } else {
                dial.stage
            }
        });
        EngineError::Io(std::io::Error::other(Self {
            tag,
            protocol,
            endpoint,
            stage,
            dial,
            source,
        }))
    }
}

impl fmt::Display for EstablishmentFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [outbound_tag={} protocol={} stage={}",
            self.source,
            self.tag.as_deref().unwrap_or("-"),
            self.protocol,
            self.stage
        )?;
        if let Some((server, port)) = &self.endpoint {
            write!(f, " upstream_server={server} upstream_port={port}")?;
        }
        if let Some(remote) = self.dial.as_ref().and_then(|dial| dial.remote) {
            write!(f, " dial_ip={} dial_port={}", remote.ip(), remote.port())?;
        }
        write!(f, "]")
    }
}

impl Error for EstablishmentFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Inspect boxed io errors too: io::Error::source may skip its immediate payload.
pub(crate) fn establishment_failure<'a>(
    mut error: &'a (dyn Error + 'static),
) -> Option<&'a EstablishmentFailure> {
    loop {
        if let Some(failure) = error.downcast_ref::<EstablishmentFailure>() {
            return Some(failure);
        }
        error = if let Some(EngineError::Io(inner)) = error.downcast_ref::<EngineError>() {
            inner
        } else if let Some(zero_transport::RuntimeError::Io(inner)) =
            error.downcast_ref::<zero_transport::RuntimeError>()
        {
            inner
        } else if let Some(inner) = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
        {
            inner
        } else {
            error.source()?
        };
    }
}
