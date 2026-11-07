//! Finite existing endpoint operation facts, without owned maps or Strings.
use super::*;
pub(super) struct Capabilities<'a>(pub(super) &'a Observed<'a>);
const READ: [&str; 3] = ["list", "get", "details"];
const CONTROL: [&str; 4] = ["set_state", "set_directions", "restart", "clear_overrides"];
struct Operations {
    observed: bool,
    control: bool,
}
impl Serialize for Operations {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(if self.observed {
            3 + usize::from(self.control) * 4
        } else {
            0
        }))?;
        if self.observed {
            for op in READ {
                out.serialize_element(op)?;
            }
            if self.control {
                for op in CONTROL {
                    out.serialize_element(op)?;
                }
            }
        }
        out.end()
    }
}
struct Persistence {
    source: bool,
}
impl Serialize for Persistence {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_seq(Some(1 + usize::from(self.source)))?;
        out.serialize_element(&zero_api::EndpointPersistence::RuntimeOnly)?;
        if self.source {
            out.serialize_element(&zero_api::EndpointPersistence::SourceFile)?;
        }
        out.end()
    }
}
struct Operation {
    source: bool,
    contraction: Option<EndpointDirections>,
}
impl Serialize for Operation {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(None)?;
        out.serialize_entry(
            "persistence",
            &Persistence {
                source: self.source,
            },
        )?;
        out.serialize_entry(
            "preconditions",
            &["expected_core_instance_id", "expected_intent_revision"],
        )?;
        if let Some(direction) = self.contraction {
            out.serialize_entry("live_direction_contraction", &direction)?;
        }
        out.end()
    }
}
struct OperationMap<'a>(&'a Observed<'a>);
impl Serialize for OperationMap<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut out = s.serialize_map(None)?;
        if let Some(observed) = self.0.observation.filter(|facts| facts.control) {
            for op in ["clear_overrides", "restart", "set_directions", "set_state"] {
                out.serialize_entry(
                    op,
                    &Operation {
                        source: matches!(op, "set_state" | "set_directions")
                            && self.0.row.intent.binding.canonical
                            && self.0.row.path,
                        contraction: matches!(op, "set_directions" | "clear_overrides")
                            .then_some(observed.live_contraction),
                    },
                )?;
            }
        }
        out.end()
    }
}
impl Serialize for Capabilities<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let row = self.0.row;
        let binding = &row.intent.binding;
        let observed = self.0.observation.is_some();
        let control = self.0.observation.is_some_and(|facts| facts.control);
        let mut out = s.serialize_struct("EndpointCapabilities", 9)?;
        out.serialize_field(
            "directions",
            &if observed {
                binding.supported_directions
            } else {
                EndpointDirections::default()
            },
        )?;
        out.serialize_field("packet", &observed)?;
        for field in ["stream", "datagram", "derived_stream", "derived_datagram"] {
            out.serialize_field(field, &(observed && !binding.outbound_tags.is_empty()))?;
        }
        out.serialize_field(
            "peer_address_learning",
            &(observed && binding.supported_directions.inbound),
        )?;
        out.serialize_field("operations", &Operations { observed, control })?;
        out.serialize_field("operation_capabilities", &OperationMap(self.0))?;
        out.end()
    }
}
