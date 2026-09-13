use super::*;
#[derive(Clone)]
pub(super) struct Case {
    pub name: &'static str,
    pub carrier: &'static str,
    pub native: Value,
    pub reference: Value,
    pub mtu: u32,
}
pub(super) fn all() -> Vec<Case> {
    let custom_native = json!({"type":"header_custom","clients":[[{"content":{"type":"bytes","bytes":[67,76]}}]],"servers":[[{"content":{"type":"bytes","bytes":[83,86]}}]]});
    let custom_reference = json!({"type":"header-custom","settings":{"clients":[[{"type":"str","packet":"CL"}]],"servers":[[{"type":"str","packet":"SV"}]]}});
    let sudoku_native = json!({"type":"sudoku","password":"reference","custom_tables":["xxppvvvv","xpxpvvvv"],"padding_min":10,"padding_max":20});
    let sudoku_reference = json!({"type":"sudoku","settings":{"password":"reference","customTables":["xxppvvvv","xpxpvvvv"],"paddingMin":10,"paddingMax":20}});
    let mut headers = Vec::new();
    let mut reference = Vec::new();
    for (native, official) in [
        ("header_dtls", "header-dtls"),
        ("header_srtp", "header-srtp"),
        ("header_utp", "header-utp"),
        ("header_wechat", "header-wechat"),
        ("header_wireguard", "header-wireguard"),
    ] {
        headers.push(json!({"type":native}));
        reference.push(json!({"type":official}));
    }
    headers.extend([json!({"type":"header_dns","domain":"example.com"}),json!({"type":"header_custom","client":[{"type":"bytes","bytes":[1,2]}],"server":[{"type":"bytes","bytes":[3,4]}]}),json!({"type":"mkcp_original"}),json!({"type":"mkcp_aes128_gcm","password":"key"}),json!({"type":"salamander","password":"secret"})]);
    reference.extend([json!({"type":"header-dns","settings":{"domain":"example.com"}}),json!({"type":"header-custom","settings":{"client":[{"type":"hex","packet":"0102"}],"server":[{"type":"hex","packet":"0304"}]}}),json!({"type":"mkcp-original"}),json!({"type":"mkcp-aes128gcm","settings":{"password":"key"}}),json!({"type":"salamander","settings":{"password":"secret"}})]);
    let noise_native = json!({"type":"noise","items":[{"packet":[78,79,73,83,69],"delay_ms":{"minimum":1,"maximum":1}},{"random_length":{"minimum":5,"maximum":8},"minimum_byte":70,"maximum_byte":90}]});
    let noise_reference = json!({"type":"noise","settings":{"noise":[{"type":"str","packet":"NOISE","delay":"1"},{"rand":"5-8","randRange":"70-90"}]}});
    vec![
        Case {
            name: "mkcp-tls",
            carrier: "mkcp_tls",
            native: json!({"udp":[{"type":"salamander","password":"secret"}]}),
            reference: json!({"udp":[{"type":"salamander","settings":{"password":"secret"}}]}),
            mtu: 1200,
        },
        Case {
            name: "mkcp-xdns",
            carrier: "mkcp",
            native: json!({"udp":[{"type":"xdns","domain":"t.example.com"},{"type":"mkcp_original"}]}),
            reference: json!({"udp":[{"type":"xdns","settings":{"domain":"t.example.com"}},{"type":"mkcp-original"}]}),
            mtu: 100,
        },
        Case {
            name: "mkcp-noise",
            carrier: "mkcp",
            native: json!({"udp":[noise_native,{"type":"mkcp_original"}]}),
            reference: json!({"udp":[noise_reference,{"type":"mkcp-original"}]}),
            mtu: 1200,
        },
        Case {
            name: "hysteria-noise",
            carrier: "hysteria",
            native: json!({"udp":[noise_native,{"type":"salamander","password":"secret"}]}),
            reference: json!({"udp":[noise_reference,{"type":"salamander","settings":{"password":"secret"}}]}),
            mtu: 1350,
        },
        Case {
            name: "tcp-custom-sudoku",
            carrier: "tcp",
            native: json!({"tcp":[custom_native,sudoku_native]}),
            reference: json!({"tcp":[custom_reference,sudoku_reference]}),
            mtu: 1350,
        },
        Case {
            name: "tls-record-fragment",
            carrier: "tls",
            native: json!({"tcp":[{"type":"fragment","packets":{"minimum":0,"maximum":1},"length":{"minimum":1,"maximum":3},"max_splits":{"minimum":0,"maximum":0}}]}),
            reference: json!({"tcp":[{"type":"fragment","settings":{"packets":"tlshello","length":"1-3"}}]}),
            mtu: 1350,
        },
        Case {
            name: "mkcp-header-crypto-chain",
            carrier: "mkcp",
            native: json!({"udp":headers}),
            reference: json!({"udp":reference}),
            mtu: 1200,
        },
        Case {
            name: "mkcp-sudoku",
            carrier: "mkcp",
            native: json!({"udp":[sudoku_native]}),
            reference: json!({"udp":[sudoku_reference]}),
            mtu: 1000,
        },
        Case {
            name: "hysteria-header-crypto-chain",
            carrier: "hysteria",
            native: json!({"udp":headers}),
            reference: json!({"udp":reference}),
            mtu: 1350,
        },
    ]
}
