//! Measure actual immutable owned values before clone/from_value allocations.
use super::*;
fn all(value: &Value, strings: bool, content: bool) -> io::Result<usize> {
    match value {
        Value::String(s) => Ok(if strings { s.len() } else { 0 }),
        Value::Array(rows) => {
            let cell = if content {
                content_cell()
            } else {
                std::mem::size_of::<Value>()
            };
            let mut total = estimate::typed_array(rows.len(), cell)?;
            for row in rows {
                total = sum(total, all(row, strings, content)?)?;
            }
            Ok(total)
        }
        Value::Object(rows) => {
            let mut total = if content {
                estimate::typed_array(rows.len(), content_cell() * 2)?
            } else {
                estimate::map(rows.len())?
            };
            for (key, value) in rows {
                if strings {
                    total = sum(total, key.len())?;
                }
                total = sum(total, all(value, strings, content)?)?;
            }
            Ok(total)
        }
        _ => Ok(0),
    }
}
fn content_cell() -> usize {
    // serde_core 1.0.228 Content: its widest JSON arms are Vec/String
    // plus a scalar/discriminant. Sum, rather than union, is a safe envelope
    // on both 32/64-bit targets; no private serde ABI is imported.
    std::mem::size_of::<(Vec<()>, u64, u8)>()
}
pub(super) fn cloned(value: &Value) -> io::Result<usize> {
    all(value, true, false)
}
fn rows(v: &Value, key: &str) -> usize {
    v.get(key).and_then(Value::as_array).map_or(0, Vec::len)
}
pub(super) fn query(value: &Value) -> io::Result<usize> {
    let Some((key, inner)) = value.as_object().and_then(|m| m.iter().next()) else {
        return Ok(0);
    };
    match key.as_str() {
        "traffic_stat" => inner
            .get("scope")
            .map_or(Ok(0), |scope| all(scope, false, true)),
        "traffic_stats" => {
            let mut total = estimate::typed_array(
                rows(inner, "scopes"),
                std::mem::size_of::<zero_api::TrafficScope>(),
            )?;
            if let Some(scopes) = inner.get("scopes").and_then(Value::as_array) {
                for scope in scopes {
                    total = sum(total, all(scope, false, true)?)?;
                }
            }
            Ok(total)
        }
        _ => Ok(0),
    }
}
pub(super) fn command(
    method: &str,
    params: &Value,
    book: &Arc<ControlBudget>,
) -> io::Result<usize> {
    match method {
        "tun.start" => sum(
            estimate::typed_array(rows(params, "include_cidrs"), std::mem::size_of::<String>())?,
            estimate::typed_array(rows(params, "exclude_cidrs"), std::mem::size_of::<String>())?,
        ),
        "stats.reset" => {
            let mut total = estimate::typed_array(
                rows(params, "targets"),
                std::mem::size_of::<zero_api::StatsResetTarget>(),
            )?;
            if let Some(targets) = params.get("targets").and_then(Value::as_array) {
                for target in targets {
                    if let Some(scope) = target.get("scope") {
                        total = sum(total, all(scope, false, true)?)?;
                    }
                }
            }
            Ok(total)
        }
        "config.apply" | "config.apply_runtime" | "config.validate" => match params.get("config") {
            Some(config) => copied_value(config, book),
            None => Ok(0),
        },
        _ => Ok(0),
    }
}
fn copied_value(value: &Value, book: &Arc<ControlBudget>) -> io::Result<usize> {
    let mut total = all(value, false, false)?;
    match value {
        Value::Array(rows) => {
            for row in rows {
                total = sum(total, magic(row, book)?)?;
            }
        }
        _ => total = sum(total, magic(value, book)?)?,
    };
    Ok(total)
}
fn magic(value: &Value, book: &Arc<ControlBudget>) -> io::Result<usize> {
    match value {
        Value::Object(rows) => {
            let mut total = 0;
            if let Some((key, Value::String(raw))) = rows.iter().next() {
                if key == "$serde_json::private::RawValue" {
                    total = sum(
                        sum(raw.len(), estimate::value(raw.as_bytes(), book)?)?,
                        estimate::scratch(raw.as_bytes())?,
                    )?;
                }
            }
            for child in rows.values() {
                total = sum(total, magic(child, book)?)?;
            }
            Ok(total)
        }
        Value::Array(rows) => {
            let mut total = 0;
            for child in rows {
                total = sum(total, magic(child, book)?)?;
            }
            Ok(total)
        }
        _ => Ok(0),
    }
}
