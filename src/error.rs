use color_eyre::eyre::Error;
use common_x::restful::axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub(crate) enum AppError {
    ValidateFailed(String),
    NotFound,
    IsDisabled(String),
    CallPdsFailed(String),
    Unknown(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error, error_message) = match self {
            AppError::ValidateFailed(msg) => (
                StatusCode::BAD_REQUEST,
                "ValidateFailed",
                string_to_static_str(msg),
            ),
            AppError::NotFound => (
                StatusCode::NOT_FOUND,
                "NotFound",
                string_to_static_str("NOT_FOUND".to_owned()),
            ),
            AppError::IsDisabled(msg) => (
                StatusCode::FORBIDDEN,
                "IsDisabled",
                string_to_static_str(msg),
            ),
            AppError::CallPdsFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "CallPdsFailed",
                string_to_static_str(json!({"pds": msg}).to_string()),
            ),
            AppError::Unknown(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Unknown",
                string_to_static_str(msg),
            ),
        };
        let body = Json(json!({
            "code": status.as_u16(),
            "error": error,
            "message": error_message,
        }));
        (status, body).into_response()
    }
}

impl<E> From<E> for AppError
where
    E: Into<Error>,
{
    fn from(err: E) -> Self {
        Self::Unknown(err.into().to_string())
    }
}

fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
