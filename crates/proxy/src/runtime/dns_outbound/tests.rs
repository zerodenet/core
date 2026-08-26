use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zero_traits::IpAddress;

use crate::runtime::Proxy;

#[tokio::test]
async fn proxy_installs_dns_detour_connector_without_recursive_resolution() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind DNS TCP server");
    let endpoint = listener.local_addr().expect("DNS TCP endpoint");
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept DNS TCP");
        let size = stream.read_u16().await.expect("read DNS query size") as usize;
        let mut query = vec![0_u8; size];
        stream
            .read_exact(&mut query)
            .await
            .expect("read DNS query");
        let response =
            zero_dns::udp::build_dns_response(&query, &[IpAddress::V4([198, 51, 100, 53])]);
        stream
            .write_u16(response.len() as u16)
            .await
            .expect("write DNS response size");
        stream
            .write_all(&response)
            .await
            .expect("write DNS response");
    });
    let config = zero_config::RuntimeConfig::parse(&format!(
        r#"{{
            "runtime": {{
                "dns": {{
                    "servers": {{
                        "bootstrap": {{ "type": "udp", "host": "1.1.1.1" }},
                        "proxy": {{
                            "type": "udp",
                            "host": "{}",
                            "port": {},
                            "detour": "dns-out"
                        }}
                    }},
                    "default_server": "proxy",
                    "policy": {{ "node_server": "bootstrap" }}
                }}
            }},
            "outbounds": [{{ "tag": "dns-out", "protocol": {{ "type": "direct" }} }}],
            "route": {{ "rules": [], "final": {{ "type": "direct" }} }}
        }}"#,
        endpoint.ip(),
        endpoint.port()
    ))
    .expect("parse DNS detour config");
    let proxy = Proxy::new(config).expect("build proxy");

    assert_eq!(
        proxy
            .resolver
            .resolve_real_type("detour.example", 1)
            .await
            .unwrap(),
        vec![IpAddress::V4([198, 51, 100, 53])]
    );
    let attempts = proxy.resolver.recent_query_attempts(
        "detour.example",
        zero_dns::DnsQueryRole::Default,
        1,
    );
    assert_eq!(attempts[0].outbound, "dns-out");
    server.await.unwrap();
}
