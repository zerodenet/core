//! Admission describes an executable Browser provider, not a dropped WS flag.
use super::*;
use serde_json::json;
fn input(kind: &str) -> OutboundConfig {
    let mut protocol = json!({"type":kind,"server":"example.test","port":443,"ws":{"path":"/wire","browser_dialer":{}}});
    protocol[if kind == "trojan" { "password" } else { "id" }] =
        json!("11111111-2222-3333-4444-555555555555");
    serde_json::from_value(json!({"tag":"node","protocol":protocol})).unwrap()
}
#[test]
fn browser_selection_retains_unqualified_profiles_instead_of_silently_dropping_their_options() {
    for kind in ["trojan", "vmess"] {
        let original = input(kind);
        assert!(eligible(&original));
        let mut custom = original.clone();
        match &mut custom.protocol {
            OutboundProtocolConfig::Trojan { ws, .. }
            | OutboundProtocolConfig::Vmess { ws, .. } => {
                ws.as_mut().unwrap().host = Some("front.example".into())
            }
            _ => unreachable!(),
        }
        assert!(!eligible(&custom));
        assert!(native_compatibility(&custom));
        assert!(!native_compatibility(&original));
        match &custom.protocol {
            OutboundProtocolConfig::Trojan { ws, .. }
            | OutboundProtocolConfig::Vmess { ws, .. } => {
                assert_eq!(ws.as_ref().unwrap().host.as_deref(), Some("front.example"))
            }
            _ => unreachable!(),
        }
        let mut mux = original.clone();
        match &mut mux.protocol {
            OutboundProtocolConfig::Trojan {
                mux_concurrency, ..
            }
            | OutboundProtocolConfig::Vmess {
                mux_concurrency, ..
            } => *mux_concurrency = Some(2),
            _ => unreachable!(),
        }
        assert!(!eligible(&mux));
        assert!(mux_eligible(&mux));
        assert!(eligible(&original));
    }
}
