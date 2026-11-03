use super::*;
mod conditions;
mod content;
mod listing;
mod range;
pub(super) fn initialize_mime() {
    content::mime::initialize();
}
pub(super) async fn serve(
    root: &Path,
    request: http::Request<()>,
    exchange: &mut dyn HttpExchange,
) -> io::Result<()> {
    serve_using(root, request, exchange, &mut Access::Legacy).await
}
pub(super) async fn serve_scoped(
    root: &Path,
    request: http::Request<()>,
    exchange: &mut dyn HttpExchange,
    files: &mut zero_platform_tokio::file::tree::FileTreeSession,
) -> io::Result<()> {
    serve_using(root, request, exchange, &mut Access::Owned(files)).await
}
async fn serve_using(
    root: &Path,
    request: http::Request<()>,
    exchange: &mut dyn HttpExchange,
    files: &mut Access<'_>,
) -> io::Result<()> {
    let head = request.method() == http::Method::HEAD;
    let decoded = match percent_encoding::percent_decode_str(request.uri().path()).decode_utf8() {
        Ok(decoded) => decoded,
        Err(_) => return error(exchange, 404, head).await,
    };
    if decoded.ends_with("/index.html") {
        return redirect(exchange, &request, "./".into()).await;
    }
    let mut relative = PathBuf::new();
    for part in decoded.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                relative.pop();
            }
            part => relative.push(part),
        }
    }
    let path = match files.canonicalize(root.join(relative)).await {
        Ok(path) if path.starts_with(root) => path,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            return super::error(exchange, 403, head).await
        }
        _ => return error(exchange, 404, head).await,
    };
    let metadata = files.metadata(path.clone()).await?;
    if metadata.is_dir() {
        if !decoded.ends_with('/') {
            return redirect(
                exchange,
                &request,
                format!("{}/", decoded.rsplit('/').next().unwrap_or("")),
            )
            .await;
        }
        let index = files.canonicalize(path.join("index.html")).await;
        if let Ok(index) = index {
            if !index.starts_with(root) {
                return error(exchange, 404, head).await;
            }
            if files
                .metadata(index.clone())
                .await
                .is_ok_and(|metadata| metadata.is_file())
            {
                return content::serve_using(&index, &request, exchange, files).await;
            }
        }
        let modified = metadata.modified().ok();
        if conditions::unmodified(&request, modified) {
            let mut response = http::Response::builder().status(304);
            if let Some(modified) = modified {
                response = response.header("last-modified", httpdate::fmt_http_date(modified));
            }
            exchange.send_response(response.body(()).unwrap()).await?;
            return exchange.finish().await;
        }
        return listing::serve_using(path, head, exchange, modified, files).await;
    }
    if decoded.ends_with('/') {
        return redirect(
            exchange,
            &request,
            format!(
                "../{}",
                decoded
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
            ),
        )
        .await;
    }
    content::serve_using(&path, &request, exchange, files).await
}
async fn redirect(
    exchange: &mut dyn HttpExchange,
    request: &http::Request<()>,
    mut location: String,
) -> io::Result<()> {
    if let Some(query) = request.uri().query() {
        location.push('?');
        location.push_str(query);
    }
    // Go's FileServer localRedirect writes an empty response with a relative Location.
    exchange
        .send_response(
            http::Response::builder()
                .status(301)
                .header("location", location)
                .body(())
                .map_err(io::Error::other)?,
        )
        .await?;
    exchange.finish().await
}

mod access;
use access::Access;
