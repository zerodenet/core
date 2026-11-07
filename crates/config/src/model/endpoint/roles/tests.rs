//! The borrowed facts match the pre-existing canonical/legacy role projection.
use super::*;
#[test]
fn canonical_and_shared_legacy_endpoint_roles_preserve_exact_union_without_new_strings() {
    let key = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=";
    // Metadata only: this exercises the same binding projection, no native grant.
    let config:RuntimeConfig=serde_json::from_value(serde_json::json!({
  "endpoints":[{"tag":"canon-雪","listen":{"host":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.1/32"],"peers":[]}}],
  "inbounds":[{"tag":"shared","listen":{"host":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.2/32"],"peers":[]}},
   {"tag":"alone","listen":{"host":"127.0.0.1","port":0},"protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.3/32"],"peers":[]}}],
  "outbounds":[{"tag":"a","protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.4/32"],"peers":[],"inbound_tag":"shared"}},
   {"tag":"b","protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.5/32"],"peers":[],"inbound_tag":"shared"}},
   {"tag":"solo","protocol":{"type":"wireguard","private_key":key,"addresses":["10.0.0.6/32"],"peers":[]}}]
 })).unwrap();
    let bindings = config.endpoint_bindings();
    for id in [
        "endpoint:canon-雪",
        "legacy:inbound:shared",
        "legacy:inbound:alone",
        "legacy:outbound:solo",
        "absent",
    ] {
        for tag in [
            "canon-雪",
            "endpoint/canon-雪",
            "shared",
            "alone",
            "a",
            "b",
            "solo",
            "absent",
        ] {
            for inbound in [false, true] {
                let expected = bindings.iter().any(|binding| {
                    binding.endpoint_id == id
                        && if inbound {
                            binding.inbound_tags.iter().any(|s| s == tag)
                        } else {
                            binding.outbound_tags.iter().any(|s| s == tag)
                        }
                });
                let actual = config
                    .borrowed_endpoint_roles()
                    .any(|role| role.matches_endpoint(id) && role.matches_role(inbound, tag));
                assert_eq!(
                    actual, expected,
                    "same actual endpoint role {id} / {inbound} / {tag}"
                );
            }
        }
    }
}
