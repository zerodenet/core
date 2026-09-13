use super::*;
use crate::http_server::HttpExchange;
use crate::hysteria::masquerade::Appearance;
mod listing;
mod sniff;
#[derive(Default)]
struct Exchange {
    response: Option<http::Response<()>>,
    body: Vec<u8>,
    finished: bool,
}
#[async_trait::async_trait]
impl HttpExchange for Exchange {
    async fn recv_data(&mut self) -> io::Result<Option<Bytes>> {
        Ok(None)
    }
    async fn send_response(&mut self, response: http::Response<()>) -> io::Result<()> {
        self.response = Some(response);
        Ok(())
    }
    async fn send_data(&mut self, data: Bytes) -> io::Result<()> {
        self.body.extend_from_slice(&data);
        Ok(())
    }
    async fn finish(&mut self) -> io::Result<()> {
        self.finished = true;
        Ok(())
    }
}
#[tokio::test]
async fn custom_headers_status_head_and_default_not_found_match_http_contract() {
    let Masquerade::Appearance(site) =
        Masquerade::content("<HTML>site</HTML>", 202, [("x-site", "example")]).unwrap()
    else {
        panic!("static site")
    };
    for method in ["GET", "HEAD"] {
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder()
                .method(method)
                .uri("https://site/")
                .body(())
                .unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        let response = exchange.response.unwrap();
        assert_eq!(response.status(), 202);
        assert_eq!(response.headers()["x-site"], "example");
        assert_eq!(
            response.headers()["content-type"],
            "text/html; charset=utf-8"
        );
        assert_eq!(
            exchange.body,
            if method == "HEAD" {
                b"".as_slice()
            } else {
                b"<HTML>site</HTML>"
            }
        );
        assert!(exchange.finished);
    }
    let mut exchange = Exchange::default();
    Appearance::NotFound
        .serve(http::Request::default(), &mut exchange)
        .await
        .unwrap();
    assert_eq!(
        exchange.response.unwrap().headers()["x-content-type-options"],
        "nosniff"
    );
    assert_eq!(exchange.body, b"404 page not found\n");
}
#[tokio::test]
async fn file_site_preserves_head_ranges_and_canonical_root() {
    let root = std::env::temp_dir().join(format!("zero-hysteria-site-{}", rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("file.txt"), b"0123456789").unwrap();
    let Masquerade::Appearance(site) = Masquerade::file(root.to_str().unwrap(), None).unwrap()
    else {
        panic!("static site")
    };
    let mut exchange = Exchange::default();
    site.serve(
        http::Request::builder()
            .uri("/file.txt")
            .header("range", "bytes=2-5")
            .body(())
            .unwrap(),
        &mut exchange,
    )
    .await
    .unwrap();
    let response = exchange.response.unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(response.headers()["content-range"], "bytes 2-5/10");
    assert_eq!(exchange.body, b"2345");
    let mut exchange = Exchange::default();
    site.serve(
        http::Request::builder()
            .method("HEAD")
            .uri("/file.txt")
            .body(())
            .unwrap(),
        &mut exchange,
    )
    .await
    .unwrap();
    assert_eq!(exchange.response.unwrap().headers()["content-length"], "10");
    assert!(exchange.body.is_empty());
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", root.join("escape")).unwrap();
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder().uri("/escape").body(()).unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        assert_eq!(exchange.response.unwrap().status(), 404);
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn file_preconditions_multipart_and_relative_redirects_follow_http_contract() {
    let root = std::env::temp_dir().join(format!(
        "zero-hysteria-conditions-{}",
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(root.join("dir")).unwrap();
    std::fs::write(root.join("file.txt"), b"0123456789").unwrap();
    let Masquerade::Appearance(site) = Masquerade::file(root.to_str().unwrap(), None).unwrap()
    else {
        panic!()
    };
    let date = httpdate::fmt_http_date(
        std::fs::metadata(root.join("file.txt"))
            .unwrap()
            .modified()
            .unwrap(),
    );
    for (method, name, value, status, expected) in [
        (
            "GET",
            "if-modified-since",
            date.as_str(),
            304,
            b"".as_slice(),
        ),
        ("GET", "if-match", "\"absent\"", 412, b"".as_slice()),
        ("GET", "if-none-match", "*", 304, b"".as_slice()),
        ("POST", "if-none-match", "*", 412, b"".as_slice()),
        (
            "GET",
            "if-none-match",
            "\"absent\"",
            200,
            b"0123456789".as_slice(),
        ),
        ("POST", "range", "bytes=2-5", 206, b"2345".as_slice()),
        ("GET", "range", "bytes=100-200,2-3", 206, b"23".as_slice()),
        (
            "GET",
            "range",
            "bytes=0-9,0-9",
            200,
            b"0123456789".as_slice(),
        ),
    ] {
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder()
                .method(method)
                .uri("/file.txt")
                .header(name, value)
                .body(())
                .unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        let response = exchange.response.unwrap();
        assert_eq!(response.status(), status, "{name} {value}");
        assert_eq!(exchange.body, expected);
        if status == 304 {
            assert!(!response.headers().contains_key("content-type"));
        }
    }
    for (value, expected) in [
        (date.as_str(), b"23".as_slice()),
        ("\"absent\"", b"0123456789".as_slice()),
    ] {
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder()
                .uri("/file.txt")
                .header("range", "bytes=2-3")
                .header("if-range", value)
                .body(())
                .unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        assert_eq!(exchange.body, expected);
    }
    let mut exchange = Exchange::default();
    site.serve(
        http::Request::builder()
            .uri("/file.txt")
            .header("range", "bytes=0-1,8-9")
            .body(())
            .unwrap(),
        &mut exchange,
    )
    .await
    .unwrap();
    let response = exchange.response.unwrap();
    assert_eq!(response.status(), 206);
    assert_eq!(
        response.headers()["content-length"]
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        exchange.body.len()
    );
    let boundary = response.headers()["content-type"]
        .to_str()
        .unwrap()
        .split("boundary=")
        .nth(1)
        .unwrap();
    let expected = format!("--{boundary}\r\nContent-Range: bytes 0-1/10\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n01\r\n--{boundary}\r\nContent-Range: bytes 8-9/10\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n89\r\n--{boundary}--\r\n");
    assert_eq!(exchange.body, expected.as_bytes());
    for (path, location) in [
        ("/dir?x=1", "dir/?x=1"),
        ("/file.txt/", "../file.txt"),
        ("/missing/index.html?x=1", "./?x=1"),
    ] {
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder().uri(path).body(()).unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        let response = exchange.response.unwrap();
        assert_eq!(response.status(), 301);
        assert_eq!(response.headers()["location"], location);
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink("/etc/passwd", root.join("dir/index.html")).unwrap();
        let mut exchange = Exchange::default();
        site.serve(
            http::Request::builder().uri("/dir/").body(()).unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
        assert_eq!(exchange.response.unwrap().status(), 404);
    }
    std::fs::remove_dir_all(root).unwrap();
}
