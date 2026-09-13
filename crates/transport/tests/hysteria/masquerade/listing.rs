use super::*;

#[cfg(unix)]
#[tokio::test]
async fn directory_links_preserve_relative_path_semantics_and_html_escaping() {
    let root = std::env::temp_dir().join(format!("zero-listing-{}", rand::random::<u64>()));
    std::fs::create_dir(&root).unwrap();
    for name in [
        "a&b.txt",
        "x y?#.txt",
        "name:part",
        "汉字.txt",
        "a'b\"<>.txt",
    ] {
        std::fs::write(root.join(name), b"file").unwrap();
    }
    std::fs::create_dir(root.join("folder-name")).unwrap();
    let mut exchange = Exchange::default();
    Appearance::File(root.clone())
        .serve(
            http::Request::builder().uri("/").body(()).unwrap(),
            &mut exchange,
        )
        .await
        .unwrap();
    let body = String::from_utf8(exchange.body).unwrap();
    for expected in [
        "<a href=\"a&b.txt\">a&amp;b.txt</a>",
        "<a href=\"x%20y%3F%23.txt\">x y?#.txt</a>",
        "<a href=\"./name:part\">name:part</a>",
        "<a href=\"%E6%B1%89%E5%AD%97.txt\">汉字.txt</a>",
        "<a href=\"a%27b%22%3C%3E.txt\">a&#39;b&#34;&lt;&gt;.txt</a>",
        "<a href=\"folder-name/\">folder-name/</a>",
    ] {
        assert!(body.contains(expected), "missing {expected}: {body}");
    }
    std::fs::remove_dir_all(root).unwrap();
}
