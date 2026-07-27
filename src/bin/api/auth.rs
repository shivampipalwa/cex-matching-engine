use std::time::{SystemTime, UNIX_EPOCH};

use argon2::{
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
    password_hash::{SaltString, rand_core::OsRng},
};
use async_trait::async_trait;
use axum::{
    Json,
    extract::{FromRequestParts, State},
    http::{StatusCode, header::AUTHORIZATION, request::Parts},
    response::{IntoResponse, Response},
};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use matching_engine::types::AccountId;
use serde::{Deserialize, Serialize};

use crate::AppState;

/// Typed auth errors so the HTTP layer can map them to status codes
/// (invalid/expired token -> 401, hasher failure -> 500)
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    // argon2's `password_hash::Error` does NOT impl `std::error::Error`, so we
    // can't `#[from]` it — capture its message instead.
    #[error("password hashing error: {0}")]
    Hashing(String),
}

const TOKEN_TTL_SECS: u64 = 24 * 60 * 60;

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: AccountId,
    pub exp: usize,
}

#[derive(Deserialize)]
pub(crate) struct SignupBody {
    email: String,
    password: String,
}
#[derive(Deserialize)]
pub(crate) struct LoginBody {
    email: String,
    password: String,
}

#[derive(Serialize)]
pub(crate) struct AuthResponse {
    token: String,
}

pub(crate) struct AuthUser(pub AccountId);

#[async_trait]
impl FromRequestParts<AppState> for AuthUser {
    type Rejection = ApiError;
    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        let Some(auth_header) = parts.headers.get(AUTHORIZATION) else {
            return Err(ApiError::Unauthorized);
        };
        let Ok(auth_header) = auth_header.to_str() else {
            return Err(ApiError::Unauthorized);
        };
        let Some(token) = auth_header.strip_prefix("Bearer ") else {
            return Err(ApiError::Unauthorized);
        };
        match verify(token, &state.keys.decoding_key) {
            Ok(claims) => Ok(AuthUser(claims.sub)),
            Err(_) => Err(ApiError::Unauthorized),
        }
    }
}

/// Mint a 24h HS256 token whose subject is the account id.
pub fn sign(account_id: AccountId, key: &EncodingKey) -> Result<String, AuthError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs();
    let claims = Claims {
        sub: account_id,
        exp: (now + TOKEN_TTL_SECS) as usize,
    };
    Ok(encode(&Header::new(Algorithm::HS256), &claims, key)?)
}

/// Verify signature + expiry and return the claims.
/// Validation::new(HS256) checks the `exp` claim by default, so an expired token fails here.
pub fn verify(token: &str, key: &DecodingKey) -> Result<Claims, AuthError> {
    let data = decode::<Claims>(token, key, &Validation::new(Algorithm::HS256))?;
    Ok(data.claims)
}

/// Argon2 hash (default params) as a self-describing PHC string (salt embedded).
pub fn hash_password(pw: &str) -> Result<String, AuthError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(pw.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| AuthError::Hashing(e.to_string()))
}

/// `Ok(true)` correct, `Ok(false)` wrong password (a *result*, not an error),
/// `Err` only if the stored hash is malformed / the hasher fails.
pub fn verify_password(pw: &str, hash: &str) -> Result<bool, AuthError> {
    // Parsing the stored PHC string recovers the SAME salt argon2 used, so
    // re-derivation matches. (Re-hashing with a fresh salt never would.)
    let parsed = PasswordHash::new(hash).map_err(|e| AuthError::Hashing(e.to_string()))?;
    match Argon2::default().verify_password(pw.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(e) => Err(AuthError::Hashing(e.to_string())),
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("unauthorized")]
    Unauthorized,
    #[error("email already registered")]
    EmailTaken,
    #[error("internal error: {0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::Unauthorized => StatusCode::UNAUTHORIZED,
            ApiError::EmailTaken => StatusCode::CONFLICT,
            ApiError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
        .into_response()
    }
}

// The default sqlx/auth failures are "internal". The unique-violation case is
// handled explicitly at the call site *before* this blanket conversion runs.
impl From<AuthError> for ApiError {
    fn from(e: AuthError) -> Self {
        ApiError::Internal(e.to_string())
    }
}
impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Internal(e.to_string())
    }
}

pub(crate) async fn signup(
    State(state): State<AppState>,
    Json(body): Json<SignupBody>,
) -> Result<impl IntoResponse, ApiError> {
    let pw_hash = hash_password(&body.password)?;

    // Don't `?` the insert — a duplicate email surfaces as a DB unique violation
    // that must become 409, not the blanket 500 the `From<sqlx::Error>` gives.
    let id: i64 = match sqlx::query_scalar::<_, i64>(
        "INSERT INTO users (email, password_hash) VALUES ($1, $2) RETURNING id",
    )
    .bind(&body.email)
    .bind(&pw_hash)
    .fetch_one(&state.pg_pool)
    .await
    {
        Ok(id) => id,
        Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
            return Err(ApiError::EmailTaken);
        }
        Err(e) => return Err(e.into()),
    };

    // BIGSERIAL is i64 in Postgres; the engine keys accounts by u64.
    let token = sign(id as u64, &state.keys.encoding_key)?;
    Ok((StatusCode::CREATED, Json(AuthResponse { token })))
}

pub(crate) async fn login(
    State(state): State<AppState>,
    Json(body): Json<LoginBody>,
) -> Result<impl IntoResponse, ApiError> {
    let Some((id, hash)) =
        sqlx::query_as::<_, (i64, String)>("SELECT id, password_hash FROM users WHERE email = $1")
            .bind(&body.email)
            .fetch_optional(&state.pg_pool)
            .await?
    else {
        return Err(ApiError::Unauthorized);
    };
    match verify_password(&body.password, &hash)? {
        true => {
            let token = sign(id as u64, &state.keys.encoding_key)?;
            return Ok((StatusCode::OK, Json(AuthResponse { token })));
        }
        false => return Err(ApiError::Unauthorized),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> (EncodingKey, DecodingKey) {
        let secret = b"test-secret-not-for-production";
        (
            EncodingKey::from_secret(secret),
            DecodingKey::from_secret(secret),
        )
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let (enc, dec) = keys();
        let token = sign(42, &enc).unwrap();
        assert_eq!(verify(&token, &dec).unwrap().sub, 42);
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let (enc, _) = keys();
        let token = sign(42, &enc).unwrap();
        let wrong = DecodingKey::from_secret(b"a-different-secret");
        assert!(verify(&token, &wrong).is_err());
    }

    #[test]
    fn verify_rejects_expired_token() {
        let (enc, dec) = keys();
        // hand-craft an already-expired token (exp = 1s after the epoch).
        let token = encode(
            &Header::new(Algorithm::HS256),
            &Claims { sub: 7, exp: 1 },
            &enc,
        )
        .unwrap();
        assert!(verify(&token, &dec).is_err());
    }

    #[test]
    fn password_verifies_correct_and_rejects_wrong() {
        let hash = hash_password("correct horse").unwrap();
        assert!(verify_password("correct horse", &hash).unwrap());
        assert!(!verify_password("wrong password", &hash).unwrap());
    }

    #[test]
    fn same_password_hashes_differently_salted() {
        // Random per-hash salt => two hashes of the same password differ.
        assert_ne!(hash_password("pw").unwrap(), hash_password("pw").unwrap());
    }
}
