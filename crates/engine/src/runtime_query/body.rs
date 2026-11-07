//! Flat body views; one actual frozen row is reused for all legacy/new fields.
use super::*;
use serde::ser::{SerializeSeq, SerializeStruct};
pub(super) struct Body<'a>(pub &'a EngineRuntimeQuery<'a>);
impl Serialize for Body<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = self.0;
        match v.guard.kind {
            Kind::Runtime => Runtime(v).serialize(s),
            Kind::Stats => v
                .stats
                .as_ref()
                .expect("stats query froze original header")
                .serialize(s),
            Kind::Diagnostics => {
                let stats = &v
                    .stats
                    .as_ref()
                    .expect("diagnostics froze original header")
                    .header;
                let mut m = s.serialize_struct("Diagnostics", 5)?;
                m.serialize_field("healthy", &true)?;
                m.serialize_field("active_sessions", &stats.active_sessions)?;
                m.serialize_field("completed_sessions", &stats.completed_sessions)?;
                m.serialize_field("failed_sessions", &stats.failed_sessions)?;
                m.serialize_field("udp_upstream", &stats.udp_upstream)?;
                m.end()
            }
            Kind::Active(_) => Active(v).serialize(s),
            Kind::Recent(_) => Recent(v).serialize(s),
            Kind::Flow { id, .. } => {
                let row = v
                    .active
                    .as_ref()
                    .and_then(|a| a.iter().next())
                    .or_else(|| {
                        v.guard
                            .completed
                            .as_ref()
                            .and_then(|h| h.iter(None, 1, id).next())
                    })
                    .expect("same guards retained selected flow");
                legacy::Legacy {
                    row,
                    completed: false,
                }
                .serialize(s)
            }
        }
    }
}
struct Active<'a>(&'a EngineRuntimeQuery<'a>);
impl Serialize for Active<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let rows = self
            .0
            .active
            .as_ref()
            .expect("active query froze original rows");
        let mut seq = s.serialize_seq(Some(rows.len()))?;
        for row in rows.iter() {
            seq.serialize_element(&legacy::Legacy {
                row,
                completed: false,
            })?;
        }
        seq.end()
    }
}
struct Recent<'a>(&'a EngineRuntimeQuery<'a>);
impl Serialize for Recent<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (filter, limit) = match self.0.guard.kind {
            Kind::Recent(q) => (Some(&q.filter), q.limit.unwrap_or(usize::MAX)),
            _ => (None, usize::MAX),
        };
        let rows = self
            .0
            .guard
            .completed
            .as_ref()
            .expect("recent query retained original history");
        let mut seq = s.serialize_seq(None)?;
        for row in rows.iter(filter, limit, None) {
            seq.serialize_element(&legacy::Legacy {
                row,
                completed: true,
            })?;
        }
        seq.end()
    }
}
struct Runtime<'a>(&'a EngineRuntimeQuery<'a>);
impl Serialize for Runtime<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = self.0;
        let engine = v.guard.engine;
        let config = v.guard.snapshot.config();
        let mut m = s.serialize_map(None)?;
        m.serialize_entry("core_instance_id", engine.core_instance_id())?;
        m.serialize_entry("config_revision", &v.guard.snapshot.config_revision())?;
        m.serialize_entry("stats", &v.stats)?;
        m.serialize_entry(
            "udp_upstream_idle_timeout_seconds",
            &engine.udp_upstream_idle_timeout().as_secs(),
        )?;
        m.serialize_entry("udp_enabled", &config.runtime.udp.enabled)?;
        m.serialize_entry("log_level", &config.runtime.log.level)?;
        m.serialize_entry("log_files", &LogFiles(&config.runtime.log.files))?;
        m.serialize_entry("pid", &engine.pid)?;
        if let Some(path) = engine.config_path() {
            m.serialize_entry("config_path", &Displayed(path.display()))?;
        }
        m.serialize_entry("started_at_unix_ms", &engine.started_at_unix_ms())?;
        m.serialize_entry("active_sessions", &Active(v))?;
        m.serialize_entry("recent_completed_sessions", &Recent(v))?;
        m.end()
    }
}
struct LogFiles<'a>(&'a [zero_config::LogFileConfig]);
impl Serialize for LogFiles<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for file in self.0 {
            seq.serialize_element(&file.path)?;
        }
        seq.end()
    }
}
struct Displayed<T>(T);
impl<T: std::fmt::Display> Serialize for Displayed<T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&self.0)
    }
}
