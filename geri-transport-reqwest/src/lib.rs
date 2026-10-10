// Testler Claude tarafından yazılmıştır.

use geri_adapter_sdk::Transport;
pub use geri_binding_http::Codec;
use geri_binding_http::{MAX_WAIT_MS, Method, SIGNALS, client};
use geri_proto::v4::msg::{Frame, ToHub, ToSource};
use geri_sign::Signer;
use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const WAIT_GAP: Duration = Duration::from_secs(1);
const WAIT_RETRY: Duration = Duration::from_secs(5);

enum Waited {
    Signals(Vec<ToSource>),
    Unsupported,
    Failed,
}

type Wait = Pin<Box<dyn Future<Output = Waited> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[error("binding: {0}")]
    Binding(#[from] geri_binding_http::Error),
    #[error("blob upload answered {status}: {body}")]
    Blob { status: u16, body: String },
}

#[derive(Clone)]
struct Auth {
    token: String,
    agent_id: String,
    signer: Option<Arc<Signer>>,
}

impl Auth {
    async fn send(
        &self,
        http: &reqwest::Client,
        mut request: reqwest::Request,
    ) -> reqwest::Result<reqwest::Response> {
        let Some(signer) = &self.signer else {
            let headers = request.headers_mut();
            if let Ok(v) = format!("Bearer {}", self.token).parse() {
                headers.insert(reqwest::header::AUTHORIZATION, v);
            }
            if let Ok(v) = self.agent_id.parse() {
                headers.insert("x-geri-agent", v);
            }
            return http.execute(request).await;
        };
        let retry = request.try_clone();
        let response = http.execute(sign(signer, request)).await?;
        let stale = (response.status() == reqwest::StatusCode::UNAUTHORIZED)
            .then(|| response.headers().get(geri_sign::COUNTER_HEADER))
            .flatten()
            .and_then(|v| v.to_str().ok())
            .and_then(geri_sign::counter);
        match (stale, retry) {
            (Some(counter), Some(retry)) => {
                signer.learn(counter);
                http.execute(sign(signer, retry)).await
            }
            _ => Ok(response),
        }
    }
}

fn sign(signer: &Signer, mut request: reqwest::Request) -> reqwest::Request {
    let url = request.url();
    let target = match url.query() {
        Some(q) => format!("{}?{q}", url.path()),
        None => url.path().to_owned(),
    };
    let body = request
        .body()
        .and_then(reqwest::Body::as_bytes)
        .unwrap_or_default();
    let value = signer.authorization(request.method().as_str(), &target, body);
    if let Ok(value) = value.parse() {
        request
            .headers_mut()
            .insert(reqwest::header::AUTHORIZATION, value);
    }
    request
}

pub struct HttpTransport {
    http: reqwest::Client,
    base_url: String,
    auth: Auth,
    codec: Codec,
    answers: VecDeque<Frame<ToSource>>,
    wait: Option<Mutex<Wait>>,
    wait_after: Option<Duration>,
}

impl fmt::Debug for HttpTransport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpTransport")
            .field("base_url", &self.base_url)
            .field("agent_id", &self.auth.agent_id)
            .field("signed", &self.auth.signer.is_some())
            .field("codec", &self.codec)
            .field("long_poll", &self.wait_after.is_some())
            .finish_non_exhaustive()
    }
}

impl HttpTransport {
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        agent_id: impl Into<String>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            http,
            base_url: base_url.into().trim_end_matches('/').into(),
            auth: Auth {
                token: token.into(),
                agent_id: agent_id.into(),
                signer: None,
            },
            codec: Codec::Json,
            answers: VecDeque::new(),
            wait: None,
            wait_after: Some(Duration::ZERO),
        }
    }

    pub fn with_codec(mut self, codec: Codec) -> Self {
        self.codec = codec;
        self
    }

    pub fn with_signer(mut self, signer: Arc<Signer>) -> Self {
        self.auth.signer = Some(signer);
        self
    }

    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = http;
        self
    }

    pub async fn put_blob(&self, sha256: &str, bytes: Vec<u8>) -> Result<(), Error> {
        let request = self
            .request(reqwest::Method::PUT, &format!("/v4/blobs/{sha256}"))
            .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
            .body(bytes)
            .build()?;
        let response = self.auth.send(&self.http, request).await?;
        let status = response.status();
        if status.is_success() {
            Ok(())
        } else {
            Err(Error::Blob {
                status: status.as_u16(),
                body: response.text().await.unwrap_or_default(),
            })
        }
    }

    fn start_wait(&self, after: Duration) -> Wait {
        let request = self
            .request(reqwest::Method::GET, &client::wait(MAX_WAIT_MS).path)
            .build();
        let (http, auth) = (self.http.clone(), self.auth.clone());
        Box::pin(async move {
            tokio::time::sleep(after).await;
            let Ok(request) = request else {
                return Waited::Failed;
            };
            match auth.send(&http, request).await {
                Ok(r) if matches!(r.status().as_u16(), 404 | 405) => Waited::Unsupported,
                Ok(r) if r.status().is_success() => Waited::Signals(client::signals(
                    r.headers().get(SIGNALS).and_then(|v| v.to_str().ok()),
                )),
                Ok(_) | Err(_) => Waited::Failed,
            }
        })
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, format!("{}{path}", self.base_url))
    }
}

impl Transport for HttpTransport {
    type Error = Error;

    fn disconnect(&mut self) {
        self.answers.clear();
        self.wait = None;
        self.wait_after = None;
    }

    fn pushes(&self) -> bool {
        false
    }

    async fn connect(&mut self) -> Result<(), Error> {
        self.answers.clear();
        self.wait = None;
        self.wait_after = Some(Duration::ZERO);
        Ok(())
    }

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), Error> {
        let request = client::request(&frame.msg, self.codec)?;
        let method = match request.method {
            Method::Get => reqwest::Method::GET,
            Method::Put => reqwest::Method::PUT,
            Method::Post => reqwest::Method::POST,
        };
        let mut builder = self
            .request(method, &request.path)
            .header(reqwest::header::ACCEPT, self.codec.content_type());
        if !request.body.is_empty() {
            builder = builder
                .header(reqwest::header::CONTENT_TYPE, request.codec.content_type())
                .body(request.body);
        }
        let response = self.auth.send(&self.http, builder.build()?).await?;
        let status = response.status().as_u16();
        let header = |name| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        let content_type = header(reqwest::header::CONTENT_TYPE.as_str());
        let signals = client::signals(header(SIGNALS).as_deref());
        let body = response.bytes().await?;
        let msg = client::response(&frame.msg, status, content_type.as_deref(), &body)?;
        self.answers.push_back(Frame { id: frame.id, msg });
        self.answers
            .extend(signals.into_iter().map(|msg| Frame { id: None, msg }));
        Ok(())
    }

    async fn recv(&mut self) -> Result<Frame<ToSource>, Error> {
        loop {
            if let Some(frame) = self.answers.pop_front() {
                return Ok(frame);
            }
            let Some(after) = self.wait_after else {
                return std::future::pending().await;
            };
            if self.wait.is_none() {
                self.wait = Some(Mutex::new(self.start_wait(after)));
            }
            let waited = match self.wait.as_mut() {
                Some(wait) => {
                    let wait = wait.get_mut().unwrap_or_else(|e| e.into_inner());
                    wait.as_mut().await
                }
                None => continue,
            };
            self.wait = None;
            self.wait_after = match waited {
                Waited::Signals(signals) => {
                    self.answers
                        .extend(signals.into_iter().map(|msg| Frame { id: None, msg }));
                    Some(WAIT_GAP)
                }
                Waited::Unsupported => None,
                Waited::Failed => Some(WAIT_RETRY),
            };
        }
    }
}

#[cfg(test)]
mod tests;
