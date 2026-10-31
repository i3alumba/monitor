use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
// Auth's unpaginated user list contains exactly the authenticated user.
#[derive(Deserialize)]
#[serde(untagged)]
enum AuthResponse {
    User(AuthUser),
    Users([AuthUser; 1]),
}
#[derive(Deserialize)]
struct AuthUser {
    id: u64,
    services: Services,
}
#[derive(Deserialize)]
struct Services {
    monitor: bool,
}
use std::sync::Arc;
use tokio::sync::Semaphore;
#[derive(Clone)]
pub struct Auth {
    pub client: reqwest::Client,
    pub url: String,
    pub limit: Arc<Semaphore>,
}
pub fn bearer(headers: &HeaderMap) -> Result<&str, StatusCode> {
    if headers.get_all("authorization").iter().count() != 1 {
        return Err(StatusCode::UNAUTHORIZED);
    }
    headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|v| !v.is_empty() && v.len() <= 4096 && v.bytes().all(|c| c.is_ascii_graphic()))
        .ok_or(StatusCode::UNAUTHORIZED)
}
pub fn grant(bytes: &[u8]) -> Result<(), StatusCode> {
    let response: AuthResponse =
        serde_json::from_slice(bytes).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let value = match response {
        AuthResponse::User(user) => user,
        AuthResponse::Users([user]) => user,
    };
    if value.id == 0 {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if value.services.monitor {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}
impl Auth {
    pub async fn check(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let token = bearer(headers)?;
        let _permit = self
            .limit
            .try_acquire()
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        // Bound the entire operation, including streamed response reads.
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut response = self
                .client
                .get(&self.url)
                .bearer_auth(token)
                .send()
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                return Err(StatusCode::UNAUTHORIZED);
            }
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                return Err(StatusCode::FORBIDDEN);
            }
            if !response.status().is_success() {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            {
                if bytes.len() + chunk.len() > 65536 {
                    return Err(StatusCode::SERVICE_UNAVAILABLE);
                }
                bytes.extend_from_slice(&chunk);
            }
            grant(&bytes)
        })
        .await
        .unwrap_or(Err(StatusCode::SERVICE_UNAVAILABLE))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_grants() {
        for value in [
            serde_json::json!({}),
            serde_json::json!({"id":1,"services":{"monitor":"true"}}),
            serde_json::json!({"id":0,"services":{"monitor":true}}),
        ] {
            assert_eq!(
                grant(value.to_string().as_bytes()),
                Err(StatusCode::SERVICE_UNAVAILABLE)
            );
        }
        assert!(grant(br#"{"id":1,"services":{"monitor":true}}"#).is_ok());
        assert_eq!(
            grant(br#"{"id":1,"services":{"monitor":false}}"#),
            Err(StatusCode::FORBIDDEN)
        );
        assert!(grant(br#"[{"id":7,"username":"admin","services":{"monitor":true}}]"#).is_ok());
        assert_eq!(
            grant(br#"[{"id":7,"services":{"monitor":false}}]"#),
            Err(StatusCode::FORBIDDEN)
        );
        for bytes in [
            br#"[]"#.as_slice(),
            br#"[{"id":1,"services":{"monitor":true}},{"id":2,"services":{"monitor":true}}]"#,
            br#"[{"id":0,"services":{"monitor":true}}]"#,
            br#"[{"id":1.0,"services":{"monitor":true}}]"#,
            br#"[{"id":1,"services":{"monitor":"true"}}]"#,
            br#"[{"id":1,"id":2,"services":{"monitor":true}}]"#,
            br#"[{"id":1,"services":{"monitor":true,"monitor":false}}]"#,
            br#"[{"id":1,"services":{"monitor":true},"services":{"monitor":true}}]"#,
        ] {
            assert_eq!(grant(bytes), Err(StatusCode::SERVICE_UNAVAILABLE));
        }
        assert_eq!(bearer(&HeaderMap::new()), Err(StatusCode::UNAUTHORIZED));
    }
}
