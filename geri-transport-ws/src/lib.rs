// Testler Claude tarafından yazılmıştır.

use futures_util::{SinkExt, StreamExt};
use geri_adapter_sdk::Transport;
use geri_proto::v4::msg::{Frame, ToHub, ToSource};
use geri_sign::Signer;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpStream, time::Instant};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{self, Message, client::IntoClientRequest, http::HeaderValue},
};

pub use geri_binding_stream::Codec;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

pub const CLOSE_REPLACED: u16 = 4001;

const PING: &str = r#"{"t":"ping"}"#;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("hub url must start with http://, https://, ws:// or wss://")]
    Url,
    #[error("websocket: {0}")]
    Ws(#[from] tungstenite::Error),
    #[error("frame: {0}")]
    Frame(#[from] geri_binding_stream::Error),
    #[error("connection closed")]
    Closed,
    #[error("replaced by a newer connection of the same agent (token shared by two devices?)")]
    Replaced,
    #[error("connect timed out")]
    ConnectTimeout,
    #[error("hub stopped answering")]
    Silent,
    #[error("token or agent id is not a valid header value")]
    Header,
}

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug)]
pub struct WsTransport {
    url: String,
    token: String,
    agent_id: String,
    signer: Option<Arc<Signer>>,
    codec: Codec,
    keepalive: Duration,
    socket: Option<Socket>,
    ping_sent: Option<Instant>,
    next_ping: Instant,
}

impl WsTransport {
    pub fn new(
        base_url: &str,
        token: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Result<Self, Error> {
        let base = base_url.trim_end_matches('/');
        let url = if let Some(rest) = base.strip_prefix("https://") {
            format!("wss://{rest}/v4/ws")
        } else if let Some(rest) = base.strip_prefix("http://") {
            format!("ws://{rest}/v4/ws")
        } else if base.starts_with("wss://") || base.starts_with("ws://") {
            base.into()
        } else {
            return Err(Error::Url);
        };
        Ok(Self {
            url,
            token: token.into(),
            agent_id: agent_id.into(),
            signer: None,
            codec: Codec::Json,
            keepalive: Duration::from_secs(30),
            socket: None,
            ping_sent: None,
            next_ping: Instant::now(),
        })
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_signer(mut self, signer: Arc<Signer>) -> Self {
        self.signer = Some(signer);
        self
    }

    pub fn with_keepalive(mut self, every: Duration) -> Self {
        self.keepalive = every;
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    fn close(&mut self) {
        self.socket = None;
        self.ping_sent = None;
    }

    async fn open(&self) -> Result<Socket, Error> {
        let mut request = self.url.as_str().into_client_request()?;
        let value = |s: &str| HeaderValue::from_str(s).map_err(|_| Error::Header);
        let authorization = match &self.signer {
            Some(signer) => {
                let target = request.uri().path_and_query().map_or("/", |p| p.as_str());
                signer.authorization("GET", target, b"")
            }
            None => {
                let agent = value(&self.agent_id)?;
                request.headers_mut().insert("x-geri-agent", agent);
                format!("Bearer {}", self.token)
            }
        };
        request
            .headers_mut()
            .insert("authorization", value(&authorization)?);
        let (socket, _) =
            tokio::time::timeout(CONNECT_TIMEOUT, tokio_tungstenite::connect_async(request))
                .await
                .map_err(|_| Error::ConnectTimeout)??;
        Ok(socket)
    }
}

impl Transport for WsTransport {
    type Error = Error;

    fn disconnect(&mut self) {
        self.close();
    }

    fn pushes(&self) -> bool {
        true
    }

    async fn connect(&mut self) -> Result<(), Error> {
        self.close();
        let socket = match (self.open().await, &self.signer) {
            (Err(Error::Ws(tungstenite::Error::Http(response))), Some(signer))
                if response.status() == 401 =>
            {
                let counter = response
                    .headers()
                    .get(geri_sign::COUNTER_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .and_then(geri_sign::counter);
                match counter {
                    Some(counter) => {
                        signer.learn(counter);
                        self.open().await?
                    }
                    None => return Err(Error::Ws(tungstenite::Error::Http(response))),
                }
            }
            (result, _) => result?,
        };
        self.socket = Some(socket);
        self.next_ping = Instant::now() + self.keepalive;
        Ok(())
    }

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), Error> {
        let bytes = geri_binding_stream::encode(&frame, self.codec)?;
        let message = match self.codec {
            Codec::Json => Message::Text(String::from_utf8(bytes).unwrap_or_default().into()),
            Codec::Cbor => Message::Binary(bytes.into()),
        };
        let socket = self.socket.as_mut().ok_or(Error::Closed)?;
        if let Err(e) = socket.send(message).await {
            self.close();
            return Err(e.into());
        }
        Ok(())
    }

    async fn recv(&mut self) -> Result<Frame<ToSource>, Error> {
        loop {
            let Some(socket) = self.socket.as_mut() else {
                return std::future::pending().await;
            };
            let incoming = tokio::select! {
                incoming = socket.next() => incoming,
                () = tokio::time::sleep_until(self.next_ping) => {
                    if self.ping_sent.is_some() {
                        self.close();
                        return Err(Error::Silent);
                    }
                    match tokio::time::timeout(self.keepalive, socket.send(Message::Text(PING.into()))).await {
                        Ok(Ok(())) => {},
                        Ok(Err(e)) => { self.close(); return Err(e.into()); },
                        Err(_) => { self.close(); return Err(Error::Silent); },
                    }
                    self.ping_sent = Some(Instant::now());
                    self.next_ping = Instant::now() + self.keepalive;
                    continue;
                }
            };
            self.ping_sent = None;
            self.next_ping = Instant::now() + self.keepalive;
            let frame: Frame<ToSource> = match incoming {
                Some(Ok(Message::Text(text))) => geri_binding_stream::decode(text.as_bytes())?,
                Some(Ok(Message::Binary(bytes))) => geri_binding_stream::decode(&bytes)?,
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Ok(Message::Close(Some(frame))))
                    if u16::from(frame.code) == CLOSE_REPLACED =>
                {
                    self.close();
                    return Err(Error::Replaced);
                }
                Some(Ok(Message::Close(_))) | None => {
                    self.close();
                    return Err(Error::Closed);
                }
                Some(Err(e)) => {
                    self.close();
                    return Err(e.into());
                }
            };
            if frame.id.is_none() && frame.msg == ToSource::Pong {
                continue;
            }
            return Ok(frame);
        }
    }
}

#[cfg(test)]
mod tests;
