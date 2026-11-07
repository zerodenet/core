//! One original HTTP request and response retain their concrete byte owners.
use super::auth::authenticate;
use super::*;
pub(super) async fn serve_connection(
    mut stream: impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    handle: ProxyHandle,
    auth: Option<&HttpServerAuth>,
    limiters: &ratelimit::ApiRateLimiters,
    stop: CancellationToken,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    original: Arc<connections::Original>,
) -> io::Result<()> {
    let work = serve_request(
        &mut stream,
        handle,
        auth,
        limiters,
        stop,
        budget,
        original.clone(),
    );
    let result = crate::ipc::owner::catch_work(work).await;
    let joined = original.finish_command().await;
    crate::ipc::owner::finish_caught(result, [joined])
}
async fn serve_request(
    stream: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    handle: ProxyHandle,
    auth: Option<&HttpServerAuth>,
    limiters: &ratelimit::ApiRateLimiters,
    stop: CancellationToken,
    budget: Arc<zero_runtime::configuration::ControlBudget>,
    original: Arc<connections::Original>,
) -> io::Result<()> {
    let request = Arc::new(tokio::select! {
        _ = stop.cancelled() => return Ok(()),
        result = input::read_request(stream,&budget) => result?,
    });

    let auth_ctx = authenticate(&request, auth, &budget)?;
    if auth_ctx.permissions.is_empty() {
        let response = body::HttpBody::Raw(br#"{"error":"unauthorized"}"#);
        return tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = output::write_response(&mut *stream, "HTTP/1.1 401 Unauthorized\r\n", &response,&budget) => result,
        };
    }

    // Rate limiting.
    let category = rate_limit_category(&request);
    let allowed = match category {
        RateLimitCategory::Query => limiters.query.allow(),
        RateLimitCategory::Command => limiters.command.allow(),
        RateLimitCategory::Sse => limiters.sse_connections.allow(),
    };
    if !allowed {
        let response = body::HttpBody::Raw(br#"{"error":"rate limit exceeded"}"#);
        return tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = output::write_response(&mut *stream, "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\n", &response,&budget) => result,
        };
    }

    let _subscription_charge = if matches!(category, RateLimitCategory::Sse) {
        match budget.reserve(zero_runtime::configuration::ControlCharge::Subscription) {
            Ok(charge) => Some(charge),
            Err(_) => {
                let response =
                    body::HttpBody::Raw(br#"{"error":"subscription capacity exhausted"}"#);
                return tokio::select! { _ = stop.cancelled() => Ok(()), result = output::write_response(&mut *stream, "HTTP/1.1 503 Service Unavailable\r\n", &response,&budget) => result };
            }
        }
    } else {
        None
    };
    let accepted_command = request.method == "POST"
        && request
            .path
            .split('?')
            .next()
            .is_some_and(|path| path == "/api/v1/commands" || path == "/commands");
    let observation = if accepted_command {
        match original.reserve_command(request.clone()) {
            Ok(observation) => Some(observation),
            Err(error) => {
                if error.kind() != io::ErrorKind::WouldBlock {
                    return Err(error);
                }
                let error = zero_api::ApiError::new(
                    zero_api::ApiErrorCode::Conflict,
                    "root control command admission is full",
                );
                let response = body::HttpBody::Error(error);
                return tokio::select! {biased;_=stop.cancelled()=>Ok(()),result=output::write_response(&mut *stream,"HTTP/1.1 409 Conflict\r\n",&response,&budget)=>result};
            }
        }
    } else {
        None
    };
    let route = if let Some(observation) = &observation {
        // This original handler retains the accepted operation through its
        // actual completion/rollback ACK. Stop only suppresses the later write.
        if stop.is_cancelled() {
            return Ok(());
        }
        observation
            .scope(router::route_with_budget(
                &request, &handle, &auth_ctx, &budget,
            ))
            .await
    } else {
        tokio::select! {_=stop.cancelled()=>return Ok(()),result=router::route_with_budget(&request,&handle,&auth_ctx,&budget)=>result}
    };
    match route {
        RouteResult::Reject(error) => Err(error),
        RouteResult::Respond(status, body) => tokio::select! {
            _ = stop.cancelled() => Ok(()),
            result = output::write_response(&mut *stream, &status, &body,&budget) => result,
        },
        RouteResult::Sse {
            subscriber,
            catch_up,
        } => {
            sse::run_sse_stream_with_budget(
                stream,
                subscriber,
                catch_up,
                Duration::from_secs(30),
                stop.cancelled_owned(),
                budget,
                original.connection_budget(),
            )
            .await
        }
    }
}

enum RateLimitCategory {
    Query,
    Command,
    Sse,
}

fn rate_limit_category(request: &HttpRequest) -> RateLimitCategory {
    let method = request.method.as_str();
    let path = &request.path;

    if path.contains("/events/stream") {
        return RateLimitCategory::Sse;
    }
    if method == "POST" || path.contains("/commands") {
        return RateLimitCategory::Command;
    }
    RateLimitCategory::Query
}
