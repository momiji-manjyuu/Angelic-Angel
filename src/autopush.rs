use crate::config::{AutoPushSession, WebPushKeys};
use crate::error::{Result, AngelicAngelError};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use socket2::{SockRef, TcpKeepalive};
use std::collections::HashMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, client_async_tls_with_config};
use tracing::{debug, info, warn};
use uuid::Uuid;

const AUTOPUSH_WS_URL: &str = "wss://push.services.mozilla.com/";

/// Twitter/X VAPID public key (applicationServerKey) used by PushManager.subscribe().
const TWITTER_VAPID_PUBLIC_KEY: &[u8] = &[
    4, 94, 104, 18, 141, 49, 13, 74, 96, 202, 82, 131, 78, 91, 29, 242, 150, 102, 197, 0, 53, 149,
    230, 8, 54, 38, 62, 173, 43, 28, 89, 130, 191, 222, 213, 128, 147, 62, 21, 49, 187, 95, 212,
    194, 196, 253, 140, 157, 234, 34, 8, 234, 143, 158, 221, 15, 83, 8, 222, 111, 100, 204, 213,
    48, 75,
];

/// Application-level ping interval in seconds.
/// Firefox uses 30 minutes (services.push.pingInterval = 1800000ms), but we use 5 minutes
/// to stay within typical infrastructure idle-connection timeouts (~20 minutes).
/// Well above autopush-rs's ExcessivePing threshold (45 seconds).
const PING_INTERVAL_SECS: u64 = 5 * 60;

/// Pong wait timeout in seconds. Matches Firefox's requestTimeout (10000ms).
const PONG_TIMEOUT_SECS: u64 = 10;

/// WebSocket close code for server-initiated backoff (Firefox: kBACKOFF_WS_STATUS_CODE = 4774).
const BACKOFF_WS_STATUS_CODE: u16 = 4774;

/// TCP keepalive interval in seconds.
/// Prevents intermediate devices (NAT/LB) from dropping idle TCP connections.
/// Typical NAT idle timeouts are 15-30 minutes; 60s keepalive comfortably avoids them.
const TCP_KEEPALIVE_SECS: u64 = 60;

/// WebSocket Ping frame interval in seconds (RFC 6455 Section 5.5.2).
/// Prevents L7 load balancers/CDNs from considering the WebSocket idle at the frame level.
/// Sent independently of the application-level ping (5 minutes).
const WS_PING_INTERVAL_SECS: u64 = 150;

/// Bound DNS/TCP setup and TLS/WebSocket setup independently.
const CONNECT_TIMEOUT_SECS: u64 = 15;
/// A stalled write must force reconnect rather than hang the collector.
const WRITE_TIMEOUT_SECS: u64 = 10;
/// Bound allocations before protocol parsing and the tighter ciphertext limit.
const MAX_WEBSOCKET_MESSAGE_BYTES: usize = 1024 * 1024;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Result of a new AutoPush registration.
#[derive(Clone)]
pub struct AutoPushRegistration {
    pub uaid: String,
    pub channel_id: String,
    pub endpoint: String,
}

/// Information returned when re-registration is needed (includes freshly generated keys).
#[derive(Clone)]
pub struct ReregistrationInfo {
    pub registration: AutoPushRegistration,
    /// Freshly generated keys (Firefox: ensureCrypto() always creates new keys on re-subscribe).
    pub keys: WebPushKeys,
}

/// A received push notification.
#[derive(Clone)]
pub struct Notification {
    pub channel_id: String,
    pub version: String,
    pub data: Option<String>,
    pub headers: Option<HashMap<String, String>>,
}

pub struct AutoPushClient {
    ws: WsStream,
    #[allow(dead_code)]
    uaid: String,
    #[allow(dead_code)]
    channel_id: String,
}

// --- AutoPush protocol message types ---

#[derive(Serialize, Deserialize)]
#[serde(tag = "messageType")]
enum AutoPushMessage {
    #[serde(rename = "hello")]
    Hello {
        #[serde(skip_serializing_if = "Option::is_none")]
        uaid: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "channelIDs")]
        channel_ids: Option<Vec<String>>,
        use_webpush: bool,
        /// Firefox-compatible: broadcast listeners (always empty `{}`).
        broadcasts: HashMap<String, String>,
    },
    #[serde(rename = "register")]
    Register {
        #[serde(rename = "channelID")]
        channel_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        key: Option<String>,
    },
    #[serde(rename = "unregister")]
    Unregister {
        #[serde(rename = "channelID")]
        channel_id: String,
        code: u16,
    },
    #[serde(rename = "ack")]
    Ack { updates: Vec<AckUpdate> },
}

/// ACK status codes (Firefox PushServiceWebSocket.sys.mjs compatible).
#[derive(Debug, Clone, Copy)]
pub enum AckCode {
    /// Successfully delivered.
    Delivered = 100,
    /// Decryption failed.
    DecryptionError = 101,
    /// Delivery failed for other reasons.
    NotDelivered = 102,
}

#[derive(Serialize, Deserialize)]
struct AckUpdate {
    #[serde(rename = "channelID")]
    channel_id: String,
    version: String,
    /// ACK status code (100=delivered, 101=decryption_error, 102=not_delivered).
    code: u16,
}

#[derive(Deserialize)]
#[serde(tag = "messageType")]
enum AutoPushResponse {
    #[serde(rename = "hello")]
    Hello {
        status: u16,
        uaid: String,
        #[allow(dead_code)]
        #[serde(default)]
        use_webpush: Option<bool>,
        #[allow(dead_code)]
        #[serde(default)]
        broadcasts: Option<HashMap<String, String>>,
    },
    #[serde(rename = "register")]
    Register {
        status: u16,
        #[serde(rename = "pushEndpoint")]
        push_endpoint: String,
        #[serde(rename = "channelID")]
        #[allow(dead_code)]
        channel_id: String,
    },
    #[serde(rename = "notification")]
    Notification {
        #[serde(rename = "channelID")]
        channel_id: String,
        version: String,
        #[serde(default)]
        data: Option<String>,
        #[serde(default)]
        headers: Option<HashMap<String, String>>,
    },
}

/// What was received while waiting for a pong.
enum PongWaitResult {
    PongReceived,
    NotificationReceived(Notification),
    ConnectionClosed,
    Timeout,
}

// Parser and transport diagnostics can echo untrusted server text, URLs, headers,
// or payloads. Never propagate their Display/Debug implementations to callers.
fn websocket_error(_: tokio_tungstenite::tungstenite::Error) -> AngelicAngelError {
    AngelicAngelError::AutoPush("WebSocket transport failed".to_string())
}

async fn send_ws(ws: &mut WsStream, message: Message) -> Result<()> {
    // A timeout can leave a partially written frame. Every caller returns its
    // error and discards the connection rather than reusing that stream.
    tokio::time::timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), ws.send(message))
        .await
        .map_err(|_| AngelicAngelError::AutoPush("WebSocket send timed out".to_string()))?
        .map_err(websocket_error)
}

async fn close_ws(ws: &mut WsStream) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), ws.close(None))
        .await
        .map_err(|_| AngelicAngelError::AutoPush("WebSocket close timed out".to_string()))?
        .map_err(websocket_error)
}

fn parse_response(text: &str) -> Result<AutoPushResponse> {
    serde_json::from_str(text).map_err(|_| {
        AngelicAngelError::AutoPush("invalid AutoPush protocol response".to_string())
    })
}

fn serialize_message(message: &AutoPushMessage) -> Result<String> {
    serde_json::to_string(message).map_err(|_| {
        AngelicAngelError::AutoPush("failed to serialize AutoPush message".to_string())
    })
}

/// Determines if a text message is an application-level pong.
///
/// Firefox treats an empty JSON object `{}` or any JSON without a `messageType` field as a pong.
fn is_pong(text: &str) -> bool {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(text) {
        if let Some(obj) = value.as_object() {
            return obj.is_empty() || !obj.contains_key("messageType");
        }
    }
    false
}

/// Checks if a WebSocket close frame indicates a server backoff request (close code 4774).
fn is_backoff_close(frame: &Option<tokio_tungstenite::tungstenite::protocol::CloseFrame>) -> bool {
    match frame {
        Some(cf) => {
            cf.code
                == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::from(
                    BACKOFF_WS_STATUS_CODE,
                )
        }
        None => false,
    }
}

/// Opens a WebSocket connection with TCP keepalive enabled.
///
/// Sets TCP keepalive on the underlying socket to prevent intermediate network devices
/// (NAT tables, load balancers) from dropping idle connections. Sets a Firefox-like
/// User-Agent header to mimic a browser connection.
async fn connect_ws(url: &str) -> Result<WsStream> {
    let mut request = url
        .into_client_request()
        .map_err(|_| AngelicAngelError::AutoPush("failed to build AutoPush request".to_string()))?;
    request.headers_mut().insert(
        "User-Agent",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:134.0) Gecko/20100101 Firefox/134.0"
            .parse()
            .unwrap(),
    );
    // Origin header for proxy compatibility.
    request
        .headers_mut()
        .insert("Origin", "https://x.com".parse().unwrap());

    let uri = request.uri().clone();
    let host = uri
        .host()
        .ok_or_else(|| AngelicAngelError::AutoPush("URL has no host".to_string()))?;
    let port = uri.port_u16().unwrap_or(443);
    let addr = format!("{}:{}", host, port);

    let tcp_stream = tokio::time::timeout(
        Duration::from_secs(CONNECT_TIMEOUT_SECS),
        TcpStream::connect(&addr),
    )
        .await
        .map_err(|_| AngelicAngelError::AutoPush("AutoPush TCP connection timed out".to_string()))?
        .map_err(|_| AngelicAngelError::AutoPush("AutoPush TCP connection failed".to_string()))?;

    let sock_ref = SockRef::from(&tcp_stream);
    let keepalive = TcpKeepalive::new().with_time(Duration::from_secs(TCP_KEEPALIVE_SECS));
    sock_ref
        .set_tcp_keepalive(&keepalive)
        .map_err(|_| AngelicAngelError::AutoPush("failed to set TCP keepalive".to_string()))?;
    tcp_stream.set_nodelay(true)
        .map_err(|_| AngelicAngelError::AutoPush("failed to set TCP nodelay".to_string()))?;

    debug!(
        "TCP connection established (keepalive={}s, nodelay=true)",
        TCP_KEEPALIVE_SECS
    );

    // TLS + WebSocket handshake (connector: None = rustls via rustls-tls-webpki-roots feature).
    let mut websocket_config = WebSocketConfig::default();
    websocket_config.max_message_size = Some(MAX_WEBSOCKET_MESSAGE_BYTES);
    websocket_config.max_frame_size = Some(MAX_WEBSOCKET_MESSAGE_BYTES);
    let (ws, _response) = tokio::time::timeout(
        Duration::from_secs(CONNECT_TIMEOUT_SECS),
        client_async_tls_with_config(request, tcp_stream, Some(websocket_config), None),
    )
        .await
        .map_err(|_| AngelicAngelError::AutoPush("AutoPush TLS/WebSocket handshake timed out".to_string()))?
        .map_err(websocket_error)?;

    debug!("WebSocket connection established (TLS + keepalive)");
    Ok(ws)
}

/// Performs a fresh registration: hello -> register -> receive endpoint.
pub async fn register_new(_keys: &WebPushKeys) -> Result<AutoPushRegistration> {
    info!("starting new AutoPush registration");

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;

    // 1. Send hello
    let hello_msg = AutoPushMessage::Hello {
        uaid: Some("".to_string()),
        channel_ids: None,
        use_webpush: true,
        broadcasts: HashMap::new(),
    };
    let hello_json = serialize_message(&hello_msg)?;
    send_ws(&mut ws, Message::Text(hello_json.into())).await?;

    debug!("hello sent");

    // 2. Receive hello response (timeout: 10s, matching Firefox requestTimeout)
    let uaid = match tokio::time::timeout(Duration::from_secs(PONG_TIMEOUT_SECS), ws.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            let resp: AutoPushResponse = parse_response(&text)?;
            match resp {
                AutoPushResponse::Hello {
                    status,
                    uaid,
                    ..
                } => {
                    if status != 200 {
                        return Err(AngelicAngelError::AutoPush(format!(
                            "hello failed: status={}",
                            status
                        )));
                    }
                    info!("hello succeeded");
                    uaid
                }
                _ => {
                    return Err(AngelicAngelError::AutoPush(
                        "unexpected response: expected hello".to_string(),
                    ));
                }
            }
        }
        Ok(Some(Ok(_))) => {
            return Err(AngelicAngelError::AutoPush(
                "unexpected WebSocket message type".to_string(),
            ));
        }
        Ok(Some(Err(e))) => return Err(websocket_error(e)),
        Ok(None) => {
            return Err(AngelicAngelError::AutoPush(
                "connection closed by server".to_string(),
            ));
        }
        Err(_) => {
            return Err(AngelicAngelError::AutoPush(
                "hello response timed out (10s)".to_string(),
            ));
        }
    };

    // 3. Send register
    let channel_id = Uuid::new_v4().to_string();
    let register_msg = AutoPushMessage::Register {
        channel_id: channel_id.clone(),
        key: Some(URL_SAFE_NO_PAD.encode(TWITTER_VAPID_PUBLIC_KEY)),
    };
    let register_json = serialize_message(&register_msg)?;
    send_ws(&mut ws, Message::Text(register_json.into())).await?;

    debug!("register sent");

    // 4. Receive register response (timeout: 10s)
    let endpoint =
        match tokio::time::timeout(Duration::from_secs(PONG_TIMEOUT_SECS), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let resp: AutoPushResponse = parse_response(&text)?;
                match resp {
                    AutoPushResponse::Register {
                        status,
                        push_endpoint,
                        ..
                    } => {
                        if status != 200 {
                            return Err(AngelicAngelError::AutoPush(format!(
                                "register failed: status={}",
                                status
                            )));
                        }
                        info!("register succeeded");
                        push_endpoint
                    }
                    _ => {
                        return Err(AngelicAngelError::AutoPush(
                            "unexpected response: expected register".to_string(),
                        ));
                    }
                }
            }
            Ok(Some(Ok(_))) => {
                return Err(AngelicAngelError::AutoPush(
                    "unexpected WebSocket message type".to_string(),
                ));
            }
            Ok(Some(Err(e))) => return Err(websocket_error(e)),
            Ok(None) => {
                return Err(AngelicAngelError::AutoPush(
                    "connection closed by server".to_string(),
                ));
            }
            Err(_) => {
                return Err(AngelicAngelError::AutoPush(
                    "register response timed out (10s)".to_string(),
                ));
            }
        };

    // 5. Disconnect
    close_ws(&mut ws).await?;

    Ok(AutoPushRegistration {
        uaid,
        channel_id,
        endpoint,
    })
}

/// Result of a reconnect attempt.
pub enum ConnectResult {
    /// Connected successfully with the existing session.
    Connected(AutoPushClient),
    /// Legacy API variant. Passive listening never creates a new registration.
    #[allow(dead_code)]
    NeedsReregistration(ReregistrationInfo),
}

/// Reconnects to AutoPush with an existing session.
///
/// If the server assigns a different UAID, fail closed. Creating another push
/// registration and updating X require an explicit operator registration command.
pub async fn connect_and_listen(
    session: &AutoPushSession,
    _keys: &WebPushKeys,
) -> Result<ConnectResult> {
    info!("reconnecting to AutoPush");

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;

    // Send hello (Firefox does not send channelIDs in hello).
    let hello_msg = AutoPushMessage::Hello {
        uaid: Some(session.uaid.clone()),
        channel_ids: None,
        use_webpush: true,
        broadcasts: HashMap::new(),
    };
    let hello_json = serialize_message(&hello_msg)?;
    send_ws(&mut ws, Message::Text(hello_json.into())).await?;

    debug!("hello sent (reconnect)");

    // Receive hello response (timeout: 10s).
    match tokio::time::timeout(Duration::from_secs(PONG_TIMEOUT_SECS), ws.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            let resp: AutoPushResponse = parse_response(&text)?;
            match resp {
                AutoPushResponse::Hello {
                    status,
                    uaid,
                    ..
                } => {
                    if status != 200 {
                        return Err(AngelicAngelError::AutoPush(format!(
                            "hello failed (reconnect): status={}",
                            status
                        )));
                    }
                    if uaid != session.uaid {
                        warn!("UAID invalid; explicit re-registration required");
                        let _ = close_ws(&mut ws).await;
                        return Err(AngelicAngelError::AutoPush(
                            "UAID invalid; explicit re-registration required".to_string(),
                        ));
                    }
                    info!("hello succeeded (reconnect)");
                }
                _ => {
                    return Err(AngelicAngelError::AutoPush(
                        "unexpected response: expected hello (reconnect)".to_string(),
                    ));
                }
            }
        }
        Ok(Some(Ok(_))) => {
            return Err(AngelicAngelError::AutoPush(
                "unexpected WebSocket message type".to_string(),
            ));
        }
        Ok(Some(Err(e))) => return Err(websocket_error(e)),
        Ok(None) => {
            return Err(AngelicAngelError::AutoPush(
                "connection closed by server".to_string(),
            ));
        }
        Err(_) => {
            return Err(AngelicAngelError::AutoPush(
                "hello response timed out (10s)".to_string(),
            ));
        }
    }

    Ok(ConnectResult::Connected(AutoPushClient {
        ws,
        uaid: session.uaid.clone(),
        channel_id: session.channel_id.clone(),
    }))
}

impl AutoPushClient {
    /// Waits for the next notification, automatically handling ping/pong and keepalive.
    ///
    /// Implements a three-tier timer scheme matching Firefox (PushServiceWebSocket.sys.mjs):
    /// - Application ping timer: sends `{}` every 5 minutes to verify server liveness.
    /// - Request timeout: if no pong arrives within 10 seconds of a ping, forces reconnect.
    /// - Backoff: exponential backoff on connection errors (managed by the caller in listener.rs).
    pub async fn next_notification(&mut self) -> Result<Option<Notification>> {
        let mut next_app_ping = Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
        let mut next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);

        loop {
            let now = Instant::now();
            let app_remaining = next_app_ping.saturating_duration_since(now);
            let ws_remaining = next_ws_ping.saturating_duration_since(now);
            let remaining = app_remaining.min(ws_remaining);

            match tokio::time::timeout(remaining, self.ws.next()).await {
                // Message received: reset ping timers (Firefox resets on any message receipt).
                Ok(msg) => {
                    next_app_ping = Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
                    next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);

                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            if is_pong(&text) {
                                debug!("application-level pong received");
                                continue;
                            }

                            let resp: AutoPushResponse = parse_response(&text)?;
                            match resp {
                                AutoPushResponse::Notification {
                                    channel_id,
                                    version,
                                    data,
                                    headers,
                                } => {
                                    // Every notification reaches durable deduplication before
                                    // its caller ACKs, including upstream retransmissions.
                                    info!("notification received");
                                    return Ok(Some(Notification {
                                        channel_id,
                                        version,
                                        data,
                                        headers,
                                    }));
                                }
                                _ => {
                                    debug!("non-notification message received");
                                    continue;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            debug!("WebSocket ping received, sending pong");
                            send_ws(&mut self.ws, Message::Pong(data)).await?;
                            continue;
                        }
                        Some(Ok(Message::Pong(_))) => {
                            debug!("WebSocket pong received");
                            continue;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            if is_backoff_close(&frame) {
                                warn!("server backoff request received (close code 4774)");
                                return Err(AngelicAngelError::AutoPush(
                                    "BACKOFF: server requested backoff".to_string(),
                                ));
                            }
                            info!("WebSocket closed by server");
                            return Ok(None);
                        }
                        Some(Ok(_)) => {
                            debug!("unexpected message type");
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(websocket_error(e));
                        }
                        None => {
                            info!("WebSocket stream ended");
                            return Ok(None);
                        }
                    }
                }
                // Timer expired: determine which ping timer fired.
                Err(_) => {
                    let now = Instant::now();

                    if now >= next_app_ping {
                        debug!(
                            idle_secs = PING_INTERVAL_SECS,
                            "sending application-level ping"
                        );
                        send_ws(&mut self.ws, Message::Text("{}".into())).await?;

                        match self.wait_for_pong_or_notification().await? {
                            PongWaitResult::PongReceived => {
                                debug!("pong received, connection healthy");
                                next_app_ping =
                                    Instant::now() + Duration::from_secs(PING_INTERVAL_SECS);
                                next_ws_ping =
                                    Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);
                            }
                            PongWaitResult::NotificationReceived(notification) => {
                                return Ok(Some(notification));
                            }
                            PongWaitResult::ConnectionClosed => {
                                info!("connection closed while waiting for pong");
                                return Ok(None);
                            }
                            PongWaitResult::Timeout => {
                                warn!(
                                    timeout_secs = PONG_TIMEOUT_SECS,
                                    "pong timed out, reconnection needed"
                                );
                                return Err(AngelicAngelError::AutoPush(
                                    "pong timed out, reconnection needed".to_string(),
                                ));
                            }
                        }
                    } else {
                        // WebSocket Ping frame timer (RFC 6455 Section 5.5.2).
                        debug!(interval_secs = WS_PING_INTERVAL_SECS, "sending WebSocket ping frame");
                        send_ws(&mut self.ws, Message::Ping(vec![].into())).await?;
                        next_ws_ping = Instant::now() + Duration::from_secs(WS_PING_INTERVAL_SECS);
                    }
                }
            }
        }
    }

    /// Waits for a pong or notification with a timeout.
    ///
    /// Implements Firefox's request timeout timer (requestTimeout: 10s).
    /// Returns whichever arrives first: pong, notification, connection close, or timeout.
    async fn wait_for_pong_or_notification(&mut self) -> Result<PongWaitResult> {
        let deadline = Instant::now() + Duration::from_secs(PONG_TIMEOUT_SECS);

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(PongWaitResult::Timeout);
            }

            match tokio::time::timeout(remaining, self.ws.next()).await {
                Ok(Some(Ok(Message::Text(text)))) => {
                    if is_pong(&text) {
                        return Ok(PongWaitResult::PongReceived);
                    }
                    match parse_response(&text) {
                        Ok(AutoPushResponse::Notification {
                            channel_id,
                            version,
                            data,
                            headers,
                        }) => {
                            info!("notification received while waiting for pong");
                            return Ok(PongWaitResult::NotificationReceived(Notification {
                                channel_id,
                                version,
                                data,
                                headers,
                            }));
                        }
                        Ok(_) => {
                            debug!("non-notification message while waiting for pong");
                            continue;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Ok(Some(Ok(Message::Ping(data)))) => {
                    debug!("WebSocket ping received while waiting for pong, sending pong");
                    send_ws(&mut self.ws, Message::Pong(data)).await?;
                    continue;
                }
                Ok(Some(Ok(Message::Close(frame)))) => {
                    if is_backoff_close(&frame) {
                        warn!("server backoff request received while waiting for pong (close code 4774)");
                        return Err(AngelicAngelError::AutoPush(
                            "BACKOFF: server requested backoff".to_string(),
                        ));
                    }
                    return Ok(PongWaitResult::ConnectionClosed);
                }
                Ok(None) => {
                    return Ok(PongWaitResult::ConnectionClosed);
                }
                Ok(Some(Ok(_))) => continue,
                Ok(Some(Err(e))) => return Err(websocket_error(e)),
                Err(_) => return Ok(PongWaitResult::Timeout),
            }
        }
    }

    /// Sends an ACK for a received notification.
    pub async fn ack_notification(
        &mut self,
        channel_id: String,
        version: String,
        code: AckCode,
    ) -> Result<()> {
        let ack_msg = AutoPushMessage::Ack {
            updates: vec![AckUpdate {
                channel_id,
                version,
                code: code as u16,
            }],
        };
        let ack_json = serialize_message(&ack_msg)?;
        send_ws(&mut self.ws, Message::Text(ack_json.into())).await?;
        debug!(code = code as u16, "ACK sent");
        Ok(())
    }

    #[allow(dead_code)]
    pub fn uaid(&self) -> &str {
        &self.uaid
    }

    #[allow(dead_code)]
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }
}

/// Unregisters a channel from AutoPush.
pub async fn unregister(session: &AutoPushSession) -> Result<()> {
    info!("starting AutoPush unregistration");

    let mut ws = connect_ws(AUTOPUSH_WS_URL).await?;

    // 1. Send hello (Firefox does not include channelIDs in hello).
    let hello_msg = AutoPushMessage::Hello {
        uaid: Some(session.uaid.clone()),
        channel_ids: None,
        use_webpush: true,
        broadcasts: HashMap::new(),
    };
    let hello_json = serialize_message(&hello_msg)?;
    send_ws(&mut ws, Message::Text(hello_json.into())).await?;

    debug!("hello sent (unregister)");

    // 2. Receive hello response (timeout: 10s).
    match tokio::time::timeout(Duration::from_secs(PONG_TIMEOUT_SECS), ws.next()).await {
        Ok(Some(Ok(Message::Text(text)))) => {
            let resp: AutoPushResponse = parse_response(&text)?;
            match resp {
                AutoPushResponse::Hello { status, .. } => {
                    if status != 200 {
                        return Err(AngelicAngelError::AutoPush(format!(
                            "hello failed (unregister): status={}",
                            status
                        )));
                    }
                    info!("hello succeeded (unregister)");
                }
                _ => {
                    return Err(AngelicAngelError::AutoPush(
                        "unexpected response: expected hello (unregister)".to_string(),
                    ));
                }
            }
        }
        Ok(Some(Ok(_))) => {
            return Err(AngelicAngelError::AutoPush(
                "unexpected WebSocket message type".to_string(),
            ));
        }
        Ok(Some(Err(e))) => return Err(websocket_error(e)),
        Ok(None) => {
            return Err(AngelicAngelError::AutoPush(
                "connection closed by server".to_string(),
            ));
        }
        Err(_) => {
            return Err(AngelicAngelError::AutoPush(
                "hello response timed out (10s)".to_string(),
            ));
        }
    }

    // 3. Send unregister.
    let unregister_msg = AutoPushMessage::Unregister {
        channel_id: session.channel_id.clone(),
        code: 200,
    };
    let unregister_json = serialize_message(&unregister_msg)?;
    send_ws(&mut ws, Message::Text(unregister_json.into())).await?;

    debug!("unregister sent");

    // 4. Disconnect.
    close_ws(&mut ws).await?;

    info!("AutoPush unregistration complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_parse_errors_do_not_echo_server_text() {
        let error = parse_response(r#"{"messageType":"synthetic-private-server-value"}"#)
            .err().unwrap();
        assert!(!error.to_string().contains("synthetic-private"));
        assert!(!format!("{:?}", error).contains("synthetic-private"));
    }

    #[test]
    fn websocket_errors_do_not_echo_transport_text() {
        let transport = tokio_tungstenite::tungstenite::Error::Io(
            std::io::Error::new(std::io::ErrorKind::Other, "synthetic-private-transport-value"),
        );
        let error = websocket_error(transport);
        assert!(!error.to_string().contains("synthetic-private"));
        assert!(!format!("{:?}", error).contains("synthetic-private"));
    }

    #[test]
    fn duplicate_notifications_are_not_filtered_by_protocol_parsing() {
        let wire = r#"{"messageType":"notification","channelID":"test-channel","version":"test-version","data":"test-payload"}"#;
        for _ in 0..2 {
            assert!(matches!(parse_response(wire), Ok(AutoPushResponse::Notification { .. })));
        }
    }
}
