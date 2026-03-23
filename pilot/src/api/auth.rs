use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

/// Holds the expected password.
#[derive(Clone)]
pub struct AuthPassword(pub String);

/// Middleware that checks Basic auth on write operations (POST/PUT/DELETE).
/// GET/HEAD requests are always allowed (readonly mode).
pub async fn basic_auth_check(
    request: Request,
    next: Next,
    expected: AuthPassword,
) -> Response {
    let method = request.method().clone();

    // GET and HEAD are always allowed (readonly)
    if method == axum::http::Method::GET || method == axum::http::Method::HEAD {
        return next.run(request).await;
    }

    // Check Authorization header
    let auth_header = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if validate_basic_auth(auth_header, &expected.0) {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(axum::http::header::WWW_AUTHENTICATE, "Basic realm=\"regret\"")],
            axum::Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response()
    }
}

fn validate_basic_auth(header: &str, expected_password: &str) -> bool {
    let encoded = header.strip_prefix("Basic ").unwrap_or("");
    if encoded.is_empty() {
        return false;
    }

    use base64::Engine;
    let decoded = match base64::engine::general_purpose::STANDARD.decode(encoded) {
        Ok(d) => d,
        Err(_) => return false,
    };

    let creds = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Format: "username:password" — we only check the password, username can be anything
    match creds.split_once(':') {
        Some((_, password)) => password == expected_password,
        None => false,
    }
}
