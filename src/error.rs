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
    RpcFailed(String),
    MicroPayIncomplete(String),
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
            AppError::RpcFailed(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "RpcFailed",
                string_to_static_str(json!({"rpc": msg}).to_string()),
            ),
            AppError::MicroPayIncomplete(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "MicroPayIncomplete",
                string_to_static_str(json!({"micro_pay": msg}).to_string()),
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
