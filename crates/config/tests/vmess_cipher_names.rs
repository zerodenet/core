use zero_config::RuntimeConfig;

fn config(cipher: &str, inbound: bool) -> String {
    if inbound {
        format!(
            r#"{{"inbounds":[{{"tag":"in","listen":{{"address":"127.0.0.1","port":1080}},"protocol":{{"type":"vmess","users":[{{"id":"11111111-2222-3333-4444-555555555555","cipher":"{cipher}"}}],"tls":{{"cert_path":"certs/server.crt","key_path":"certs/server.key"}}}}}}],"route":{{"rules":[],"final":{{"type":"direct"}}}}}}"#
        )
    } else {
        format!(
            r#"{{"outbounds":[{{"tag":"out","protocol":{{"type":"vmess","server":"example.com","port":443,"id":"11111111-2222-3333-4444-555555555555","cipher":"{cipher}"}}}}],"route":{{"rules":[],"final":{{"type":"route","outbound":"out"}}}}}}"#
        )
    }
}

#[test]
fn explicit_private_cipher_is_accepted_in_both_directions() {
    for inbound in [true, false] {
        RuntimeConfig::parse(&config("zero-plus", inbound)).expect("private cipher is explicit");
    }
}

#[test]
fn standard_xray_zero_is_accepted_in_both_directions() {
    for inbound in [true, false] {
        RuntimeConfig::parse(&config("zero", inbound)).expect("standard Xray zero cipher");
    }
}
