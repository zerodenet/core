use zero_api::ApiError;

/// Map an `ApiError` to the appropriate HTTP status code.
pub fn api_error_status(error: &ApiError) -> &'static str {
    match error.code {
        zero_api::ApiErrorCode::NotFound => "HTTP/1.1 404 Not Found\r\n",
        zero_api::ApiErrorCode::InvalidArgument => "HTTP/1.1 400 Bad Request\r\n",
        zero_api::ApiErrorCode::PermissionDenied
        | zero_api::ApiErrorCode::InsufficientOsPrivilege => "HTTP/1.1 403 Forbidden\r\n",
        zero_api::ApiErrorCode::FeatureDisabled | zero_api::ApiErrorCode::Unsupported => {
            "HTTP/1.1 501 Not Implemented\r\n"
        }
        zero_api::ApiErrorCode::Conflict => "HTTP/1.1 409 Conflict\r\n",
        zero_api::ApiErrorCode::Internal => "HTTP/1.1 500 Internal Server Error\r\n",
    }
}

#[cfg(test)]
mod tests;
