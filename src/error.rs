use thiserror::Error;

#[derive(Error, Debug)]
pub enum AngelicAngelError {
    #[error("{0}")]
    Outbox(#[from] crate::outbox::OutboxError),

    #[error("{0}")]
    Delivery(#[from] crate::delivery::DeliveryError),

    #[error("background task did not complete")]
    BackgroundTask,
    #[error("config error: {0}")]
    Config(String),

    #[error("AutoPush error: {0}")]
    AutoPush(String),

    #[error("Twitter API error: {0}")]
    TwitterApi(String),

    #[error("WebSocket transport failed")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("decryption error: {0}")]
    Decryption(String),

    #[error("ECE error: {0}")]
    #[allow(dead_code)]
    Ece(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP request failed")]
    Http(#[from] reqwest::Error),

    #[error("JSON data is invalid")]
    Json(#[from] serde_json::Error),

    #[error("TOML configuration is invalid")]
    Toml(#[from] toml::de::Error),

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),
}

pub type Result<T> = std::result::Result<T, AngelicAngelError>;
