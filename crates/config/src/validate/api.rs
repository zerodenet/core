use std::collections::HashSet;

use crate::{
    ApiConfig, ConfigError, ControlApiConfig, ControlGrpcConfig, EventSinkConfig,
    ExhaustedDeliveryPolicy,
};

use super::validate_tag;

pub(super) fn validate_api(api: &ApiConfig) -> Result<(), ConfigError> {
    if api.dispatcher.replay_batch_size == 0 {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher.replay_batch_size must be greater than zero".to_owned(),
        ));
    }
    if api.dispatcher.max_in_memory_deliveries == 0 && api.outbox_path.is_none() {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher.max_in_memory_deliveries=0 requires api.outbox_path".to_owned(),
        ));
    }
    if api.dispatcher.max_retry_attempts == 0
        || api.dispatcher.retry_initial_delay_ms == 0
        || api.dispatcher.retry_max_delay_ms == 0
        || api.dispatcher.webhook_timeout_ms == 0
    {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher retry and timeout values must be greater than zero".to_owned(),
        ));
    }
    if api.dispatcher.retry_initial_delay_ms > api.dispatcher.retry_max_delay_ms {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher.retry_initial_delay_ms must not exceed retry_max_delay_ms".to_owned(),
        ));
    }
    if api.dispatcher.outbox_min_free_bytes == 0 {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher.outbox_min_free_bytes must be greater than zero".to_owned(),
        ));
    }
    if !(1..=50).contains(&api.dispatcher.outbox_min_free_percent) {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher.outbox_min_free_percent must be between 1 and 50".to_owned(),
        ));
    }
    if api.dispatcher.exhausted_delivery_policy == ExhaustedDeliveryPolicy::DeadLetter
        && api.dead_letter_path.is_none()
    {
        return Err(ConfigError::InvalidApi(
            "api.dispatcher exhausted_delivery_policy `dead_letter` requires api.dead_letter_path"
                .to_owned(),
        ));
    }
    let mut sink_tags = HashSet::new();
    for sink in &api.event_sinks {
        validate_tag("api event sink", sink.tag(), &mut sink_tags)?;
        validate_event_sink_events(sink.tag(), sink.events())?;
        if let Some(source_id) = sink.source_id() {
            validate_optional_non_empty("event sink source_id", source_id)?;
        }

        match sink {
            EventSinkConfig::JsonLines { path, .. } => {
                if path.trim().is_empty() {
                    return Err(ConfigError::InvalidApi(
                        "`jsonl` event sink requires a non-empty `path`".to_owned(),
                    ));
                }
            }
            EventSinkConfig::Webhook {
                url,
                headers,
                allow_insecure,
                ..
            } => {
                validate_webhook_url(url, *allow_insecure)?;
                for (name, value) in headers {
                    validate_optional_non_empty("webhook header name", name)?;
                    validate_optional_non_empty("webhook header value", value)?;
                }
            }
        }
    }

    if let Some(path) = &api.dead_letter_path {
        validate_optional_non_empty("dead_letter_path", path)?;
    }
    if let Some(path) = &api.outbox_path {
        validate_optional_non_empty("outbox_path", path)?;
    }

    validate_control_api(&api.control)
}

fn validate_event_sink_events(tag: &str, events: &[String]) -> Result<(), ConfigError> {
    let mut seen = HashSet::new();
    for event in events {
        if event.trim().is_empty() {
            return Err(ConfigError::InvalidApi(format!(
                "event sink `{tag}` contains an empty event type"
            )));
        }

        if !zero_api::event_type::is_known(event) {
            return Err(ConfigError::InvalidApi(format!(
                "event sink `{tag}` references unknown event type `{event}`"
            )));
        }

        if !seen.insert(event.as_str()) {
            return Err(ConfigError::InvalidApi(format!(
                "event sink `{tag}` contains duplicate event type `{event}`"
            )));
        }
    }
    Ok(())
}

fn validate_webhook_url(url: &str, allow_insecure: bool) -> Result<(), ConfigError> {
    if url.trim().is_empty() {
        return Err(ConfigError::InvalidApi(
            "`webhook` event sink requires a non-empty `url`".to_owned(),
        ));
    }

    if url.starts_with("https://") {
        return Ok(());
    }

    if url.starts_with("http://") {
        if allow_insecure {
            return Ok(());
        }

        return Err(ConfigError::InvalidApi(
            "`http://` webhook urls require `allow_insecure: true`".to_owned(),
        ));
    }

    Err(ConfigError::InvalidApi(
        "`webhook` event sink `url` must start with `https://` or `http://`".to_owned(),
    ))
}

fn validate_control_api(control: &ControlApiConfig) -> Result<(), ConfigError> {
    let has_control_fields = control.listen.is_some()
        || control.api_key.is_some()
        || control.api_key_env.is_some()
        || control.grpc.is_some();
    if !control.enabled {
        if has_control_fields {
            return Err(ConfigError::InvalidApi(
                "`api.control` fields require `enabled: true`".to_owned(),
            ));
        }

        return Ok(());
    }

    if control.listen.is_none() {
        return Err(ConfigError::InvalidApi(
            "`api.control.enabled` requires `listen`".to_owned(),
        ));
    }

    validate_api_key_fields("api control", &control.api_key, &control.api_key_env)?;
    match &control.grpc {
        Some(grpc) => validate_control_grpc(control, grpc),
        None => Ok(()),
    }
}

fn validate_control_grpc(
    control: &ControlApiConfig,
    grpc: &ControlGrpcConfig,
) -> Result<(), ConfigError> {
    let has_mtls = grpc
        .tls
        .as_ref()
        .and_then(|tls| tls.client_ca_cert_path.as_ref())
        .is_some();

    if let Some(tls) = &grpc.tls {
        validate_optional_non_empty("api.control.grpc.tls.cert_path", &tls.cert_path)?;
        validate_optional_non_empty("api.control.grpc.tls.key_path", &tls.key_path)?;
        if let Some(path) = &tls.client_ca_cert_path {
            validate_optional_non_empty("api.control.grpc.tls.client_ca_cert_path", path)?;
        }
        if grpc.allow_insecure_remote {
            return Err(ConfigError::InvalidApi(
                "`api.control.grpc.allow_insecure_remote` must be false when native TLS is configured"
                    .to_owned(),
            ));
        }
    }

    let listen = control
        .listen
        .as_ref()
        .expect("control listen was validated before gRPC policy");
    let listen_ip = listen
        .address
        .parse::<std::net::IpAddr>()
        .map_err(|error| {
            ConfigError::InvalidApi(format!(
                "`api.control.listen.address` must be an IP address: {error}"
            ))
        })?;
    if grpc.tls.is_none() && !listen_ip.is_loopback() && !grpc.allow_insecure_remote {
        return Err(ConfigError::InvalidApi(
            "plaintext gRPC on a non-loopback control listener requires `api.control.grpc.allow_insecure_remote: true` or native TLS"
                .to_owned(),
        ));
    }
    if !grpc.bearer_auth && !listen_ip.is_loopback() && !has_mtls {
        return Err(ConfigError::InvalidApi(
            "remote gRPC without bearer authentication requires mTLS client authentication"
                .to_owned(),
        ));
    }

    Ok(())
}

fn validate_api_key_fields(
    scope: &'static str,
    api_key: &Option<String>,
    api_key_env: &Option<String>,
) -> Result<(), ConfigError> {
    if api_key.is_none() && api_key_env.is_none() {
        return Err(ConfigError::InvalidApi(format!(
            "`{scope}` requires `api_key` or `api_key_env`"
        )));
    }

    if api_key.is_some() && api_key_env.is_some() {
        return Err(ConfigError::InvalidApi(format!(
            "`{scope}` must not set both `api_key` and `api_key_env`"
        )));
    }

    if let Some(value) = api_key {
        validate_optional_non_empty("api_key", value)?;
    }
    if let Some(value) = api_key_env {
        validate_optional_non_empty("api_key_env", value)?;
    }

    Ok(())
}

fn validate_optional_non_empty(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidApi(format!(
            "`{field}` must not be empty"
        )));
    }

    Ok(())
}
