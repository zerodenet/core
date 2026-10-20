//! Bounded browser navigation over an already authenticated TLS carrier.
//! This module never dials a target or grants proxy admission.
use bytes::Bytes;
use futures_util::{stream::FuturesUnordered, StreamExt};
use http::{Request, Uri};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, Semaphore},
};

mod paths;

#[derive(Clone, Debug)]
pub struct Plan {
    pub path: String,
    /// Inclusive padding, concurrency, request count, interval, return delay.
    pub ranges: [(u32, u32); 5],
}
fn sample(range: (u32, u32)) -> u32 {
    rand::random_range(range.0..=range.1)
}

/// Owned work on the original carrier, including its process-wide permit.
/// Dropping this future releases both; preparation never performs I/O or spawns.
pub type NavigationJob = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;
pub struct PreparedNavigation {
    delay: Duration,
    job: NavigationJob,
}
impl PreparedNavigation {
    pub fn into_parts(self) -> (Duration, NavigationJob) {
        (self.delay, self.job)
    }
}

/// Validate and acquire the same global bound before transferring carrier
/// ownership to an executor. No admission means no navigation or return delay.
pub fn prepare<S>(stream: S, host: String, plan: Plan) -> Option<PreparedNavigation>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    if plan
        .ranges
        .iter()
        .zip([65536, 64, 256, 60000, 60000])
        .any(|((from, to), limit)| from > to || *to > limit)
        || plan.ranges[1].1.saturating_mul(plan.ranges[2].1) > 4096
        || !plan.path.starts_with('/')
        || plan.path.starts_with("//")
    {
        return None;
    }
    static ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let permit = ADMISSION
        .get_or_init(|| Arc::new(Semaphore::new(64)))
        .clone()
        .try_acquire_owned()
        .ok()?;
    let delay = Duration::from_millis(sample(plan.ranges[4]) as u64);
    let job = Box::pin(async move {
        let _permit = permit;
        match tokio::time::timeout(Duration::from_secs(60), async move {
            let (sender, connection) = h2::client::handshake(stream)
                .await
                .map_err(io::Error::other)?;
            tokio::pin!(connection);
            let navigation = async {
                crawl(sender, host, plan).await?;
                // Preserve the camouflage connection until peer close or its
                // owning deadline, rather than closing when the caller fails.
                std::future::pending::<io::Result<()>>().await
            };
            tokio::select! {
                result = navigation => result,
                result = &mut connection => result.map_err(io::Error::other),
            }
        })
        .await
        {
            Ok(result) => result,
            // The original navigation lifetime cap is an expected retirement.
            Err(_) => Ok(()),
        }
    });
    Some(PreparedNavigation { delay, job })
}

/// The caller's return delay is independent of navigation on the owned carrier.
/// Legacy callers retain their existing executor; scoped callers use prepare.
pub async fn start<S>(stream: S, host: String, plan: Plan)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(prepared) = prepare(stream, host, plan) else {
        return;
    };
    let (delay, job) = prepared.into_parts();
    tokio::spawn(async move {
        if let Err(error) = job.await {
            tracing::debug!(%error, "HTTP navigation ended");
        }
    });
    tokio::time::sleep(delay).await;
}

#[cfg(test)]
#[path = "../tests/http_navigation/mod.rs"]
mod tests;

async fn crawl(sender: h2::client::SendRequest<Bytes>, host: String, plan: Plan) -> io::Result<()> {
    let paths = paths::for_host(&host, &plan.path);
    let first = paths.lock().await.choose();
    let origin = format!("https://{host}");
    get(
        sender.clone(),
        &origin,
        &first,
        None,
        plan.ranges[0],
        &paths,
    )
    .await?;
    // These are child futures of this single owned job, never detached tasks.
    let mut jobs = FuturesUnordered::new();
    for _ in 0..sample(plan.ranges[1]) {
        let (sender, origin, paths, plan, first) = (
            sender.clone(),
            origin.clone(),
            paths.clone(),
            plan.clone(),
            first.clone(),
        );
        jobs.push(async move {
            let mut referer = first;
            for _ in 0..sample(plan.ranges[2]) {
                let path = paths.lock().await.choose();
                get(
                    sender.clone(),
                    &origin,
                    &path,
                    Some(&referer),
                    plan.ranges[0],
                    &paths,
                )
                .await?;
                referer = path;
                tokio::time::sleep(Duration::from_millis(sample(plan.ranges[3]) as u64)).await;
            }
            Ok::<_, io::Error>(())
        });
    }
    while let Some(result) = jobs.next().await {
        result?;
    }
    Ok(())
}

async fn get(
    sender: h2::client::SendRequest<Bytes>,
    origin: &str,
    path: &str,
    referer: Option<&str>,
    padding: (u32, u32),
    paths: &Arc<Mutex<paths::Paths>>,
) -> io::Result<()> {
    static REQUESTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    let _permit = REQUESTS
        .get_or_init(|| Arc::new(Semaphore::new(32)))
        .clone()
        .acquire_owned()
        .await
        .map_err(io::Error::other)?;
    let uri: Uri = format!("{origin}{path}")
        .parse()
        .map_err(io::Error::other)?;
    let mut request = Request::get(uri).body(()).map_err(io::Error::other)?;
    crate::browser::apply_navigation_headers(request.headers_mut());
    request.headers_mut().insert(
        "cookie",
        format!("padding={}", "0".repeat(sample(padding) as usize))
            .parse()
            .map_err(io::Error::other)?,
    );
    if let Some(referer) = referer {
        request.headers_mut().insert(
            "referer",
            format!("{origin}{referer}")
                .parse()
                .map_err(io::Error::other)?,
        );
    }
    let mut sender = sender.ready().await.map_err(io::Error::other)?;
    let (response, _) = sender
        .send_request(request, true)
        .map_err(io::Error::other)?;
    let response = response.await.map_err(io::Error::other)?;
    let mut body = response.into_body();
    let mut data = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(io::Error::other)?;
        if data.len().saturating_add(chunk.len()) > 4 * 1024 * 1024 {
            return Err(io::Error::other("navigation response exceeds byte budget"));
        }
        data.extend_from_slice(&chunk);
        body.flow_control()
            .release_capacity(chunk.len())
            .map_err(io::Error::other)?;
    }
    paths.lock().await.discover(origin, &data);
    Ok(())
}
