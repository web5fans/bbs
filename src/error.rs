use color_eyre::eyre::Error;
use common_x::restful::axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug)]
pub(crate) enum AppError {
    Validate(String),
    NotFound,
    Unknown(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Validate(msg) => (StatusCode::BAD_REQUEST, string_to_static_str(msg)),
            AppError::NotFound => (
                StatusCode::NOT_FOUND,
                string_to_static_str("NOT_FOUND".to_owned()),
            ),
            AppError::Unknown(msg) => {
                (StatusCode::INTERNAL_SERVER_ERROR, string_to_static_str(msg))
            }
        };
        let body = Json(json!({
            "code": status.as_u16(),
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
