use crate::config::TwitterConfig;
use crate::error::{Result, AngelicAngelError};
use crate::push::PushSubscription;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use reqwest::Client;
use serde_json::json;
use std::time::Duration;

const TWITTER_API_BASE: &str = "https://x.com/i/api/1.1";
const AUTHORIZATION_BEARER: &str = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA";
const DEVICE_ID: &str = "Mac/Firefox";

pub fn validate_credentials(config: &TwitterConfig) -> Result<()> {
    // RFC 6265 cookie-octets exclude separators, whitespace and control bytes.
    let valid = |value: &str| !value.is_empty() && value.len() <= 4096
        && value.bytes().all(|byte| matches!(byte, 0x21 | 0x23..=0x2b | 0x2d..=0x3a | 0x3c..=0x5b | 0x5d..=0x7e));
    if !valid(&config.auth_token) || !valid(&config.ct0) {
        return Err(AngelicAngelError::Config("explicit registration requires valid X cookie values".into()));
    }
    Ok(())
}

#[cfg(test)]
mod credential_tests {
    use super::*;

    #[test]
    fn rejects_cookie_injection_without_network() {
        for value in ["", " ", "token; extra=value", "token\r\nX-Injected: yes", "\"token\"", "日本語"] {
            assert!(validate_credentials(&TwitterConfig { auth_token: value.into(), ct0: "synthetic".into() }).is_err());
            assert!(validate_credentials(&TwitterConfig { auth_token: "synthetic".into(), ct0: value.into() }).is_err());
        }
        validate_credentials(&TwitterConfig { auth_token: "synthetic-token_1".into(), ct0: "synthetic-csrf".into() }).unwrap();
    }
}

pub async fn register(
    twitter_config: &TwitterConfig,
    push_subscription: &PushSubscription,
) -> Result<()> {
    validate_credentials(twitter_config)?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .https_only(true)
        .no_proxy()
        .build()
        .map_err(|_| AngelicAngelError::TwitterApi("HTTP client initialization failed".to_string()))?;
    register_push_subscription(&client, twitter_config, push_subscription).await?;
    Ok(())
}

async fn register_push_subscription(
    client: &Client,
    twitter_config: &TwitterConfig,
    push_subscription: &PushSubscription,
) -> Result<()> {
    let url = format!("{}/notifications/settings/login.json", TWITTER_API_BASE);

    let token = &push_subscription.endpoint;
    let encryption_key1 = URL_SAFE_NO_PAD.encode(&push_subscription.keys.public_key);
    let encryption_key2 = URL_SAFE_NO_PAD.encode(&push_subscription.keys.auth_secret);

    let body = json!({
        "push_device_info": {
            "os_version": DEVICE_ID,
            "udid": DEVICE_ID,
            "env": 3,
            "locale": "en",
            "protocol_version": 1,
            "token": token,
            "encryption_key1": encryption_key1,
            "encryption_key2": encryption_key2
        }
    });

    tracing::debug!("sending push subscription request");

    let response = client
        .post(&url)
        .header("Authorization", AUTHORIZATION_BEARER)
        .header("x-csrf-token", &twitter_config.ct0)
        .header("x-twitter-auth-type", "OAuth2Session")
        .header("x-twitter-active-user", "yes")
        .header("x-twitter-client-language", "en")
        .header("Content-Type", "application/json")
        .header(
            "Cookie",
            format!(
                "auth_token={}; ct0={}",
                twitter_config.auth_token, twitter_config.ct0
            ),
        )
        .json(&body)
        .send()
        .await
        .map_err(|_| AngelicAngelError::TwitterApi("push registration request failed or timed out; remote outcome may be unknown".to_string()))?;

    let status = response.status();
    tracing::debug!(status = %status, "response status");

    if !response.status().is_success() {
        return Err(AngelicAngelError::TwitterApi(format!(
            "push subscription registration failed (HTTP {})",
            status.as_u16()
        )));
    }

    // Status is sufficient; server response bodies can contain private data.
    Ok(())
}
