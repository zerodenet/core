use super::*;

#[tokio::test]
async fn file_extensions_take_precedence_over_sniffing_in_both_cases() {
    let root = std::env::temp_dir().join(format!("zero-mime-{}", rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    for (extension, mime) in [
        ("JPG", "image/jpeg"),
        ("jpg", "image/jpeg"),
        ("CSV", "text/csv; charset=utf-8"),
        ("avif", "image/avif"),
        ("ZIP", "application/zip"),
        ("gz", "application/gzip"),
        ("ico", "image/vnd.microsoft.icon"),
        ("svg", "image/svg+xml"),
        ("not-a-registered-extension", "image/gif"),
    ] {
        let file = format!("file.{extension}");
        std::fs::write(root.join(&file), b"GIF89a").unwrap();
        let mut exchange = Exchange::default();
        Appearance::File(root.clone())
            .serve(
                http::Request::builder()
                    .uri(format!("/{file}"))
                    .body(())
                    .unwrap(),
                &mut exchange,
            )
            .await
            .unwrap();
        assert_eq!(
            exchange.response.unwrap().headers()["content-type"],
            mime,
            "{extension}"
        );
        assert_eq!(exchange.body, b"GIF89a");
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn inferred_response_types_match_pinned_go_for_signatures_and_boundaries() {
    for vector in include_str!("../../fixtures/http-sniff-go1.26.1.tsv").lines() {
        let (input, mime) = vector.split_once('\t').unwrap();
        let body: Vec<_> = input
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let site = Appearance::String {
            body: Bytes::from(body),
            status: http::StatusCode::OK,
            headers: http::HeaderMap::new(),
        };
        let mut exchange = Exchange::default();
        site.serve(http::Request::default(), &mut exchange)
            .await
            .unwrap();
        assert_eq!(
            exchange.response.unwrap().headers()["content-type"],
            mime,
            "input={input}"
        );
    }
}
