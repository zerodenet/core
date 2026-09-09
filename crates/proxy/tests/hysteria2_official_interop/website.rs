use super::website_support::{http1, http1_host, Site};
use super::*;
use tokio::net::TcpStream;

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn official_and_zero_website_http_tls_http2_redirect_and_alt_svc_match() {
    timeout(Duration::from_secs(30),async {
        let binary=std::env::var("HY2_BIN").expect("HY2_BIN must point to official Hysteria");
        let material=TempMaterial::new("hy2-official-web");
        for redirect in [false,true] {
            let site=Site::new();
            let config=site.config(redirect);
            let running=Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap().spawn();
            wait_for_listener(site.http).await;
            let zero_http=http1_host(TcpStream::connect(("127.0.0.1",site.http)).await.unwrap(),"localhost").await;
            let zero_https=http1(site.tls(b"http/1.1").await).await;
            let zero_h2=site.http2().await;
            running.shutdown().await.unwrap();
            let protocol=&config["inbounds"][0]["protocol"];
            let official=serde_json::json!({"listen":format!("127.0.0.1:{}",site.quic),
                "tls":{"cert":protocol["cert_path"],"key":protocol["key_path"]},
                "auth":{"type":"password","password":"test-password"},
                "masquerade":{"type":"string","string":{"content":"website","statusCode":200,"headers":{"Content-Type":"text/html; charset=utf-8"}},
                    "listenHTTP":format!("127.0.0.1:{}",site.http),"listenHTTPS":format!("127.0.0.1:{}",site.https),"forceHTTPS":redirect}});
            let path=material.path("official.json");std::fs::write(&path,official.to_string()).unwrap();
            let mut process=ExternalProcess::start(binary.clone(),&["server","--config",path.to_str().unwrap(),"--disable-update-check"],&material,"website");
            wait_for_listener(site.http).await;
            let official_http=http1_host(TcpStream::connect(("127.0.0.1",site.http)).await.unwrap(),"localhost").await;
            let official_https=http1(site.tls(b"http/1.1").await).await;
            let official_h2=site.http2().await;
            compare(&zero_http,&official_http,!redirect);
            compare(&zero_https,&official_https,true);
            assert_eq!(zero_h2,official_h2);
            process.kill();
        }
    }).await.unwrap();
}
fn compare(zero: &str, official: &str, body: bool) {
    assert_eq!(
        zero.split_whitespace().nth(1),
        official.split_whitespace().nth(1)
    );
    for header in ["location:", "alt-svc:"] {
        let value = |s: &str| {
            s.lines()
                .find(|line| line.to_lowercase().starts_with(header))
                .map(|v| v.split_once(':').unwrap().1.trim().to_owned())
        };
        assert_eq!(
            value(zero),
            value(official),
            "{header}:\nZero: {zero}\nOfficial: {official}"
        );
    }
    if body {
        assert_eq!(
            zero.split_once("\r\n\r\n").unwrap().1,
            official.split_once("\r\n\r\n").unwrap().1
        );
    }
}
