//! A connectrpc [`ClientTransport`] over WebTransport.

use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use connectrpc::client::{ClientBody, ClientTransport};
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use http::header::HOST;
use http::{HeaderValue, Request, Response};
use http_body_util::BodyExt;
use hyper::client::conn::http1;
use wtransport::error::ConnectionError;
use wtransport::{ClientConfig, Connection, Endpoint, VarInt};

use crate::{Body, Error, stream_io};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkPhase {
    Connecting,
    Connected,
    /// A session was established and then lost.
    Reconnecting,
}

#[derive(Clone, Copy, Debug)]
pub struct LinkState {
    pub phase: LinkPhase,
    /// Consecutive failed opens; 0 while connected.
    pub failures: u32,
    pub next_attempt_at: Option<Instant>,
}

pub struct ReconnectOptions {
    /// Doubles per consecutive failure up to `max_backoff`.
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// Called on every session transition.
    pub on_state: Option<Box<dyn Fn(LinkState) + Send + Sync>>,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(5),
            on_state: None,
        }
    }
}

type Priority = Arc<dyn Fn(&http::Uri) -> i32 + Send + Sync>;

/// Opens one bidi stream per RPC; cheap to clone.
#[derive(Clone)]
pub struct Transport {
    inner: Arc<Inner>,
    priority: Option<Priority>,
}

enum Inner {
    Fixed(Connection),
    Reconnecting(Arc<Reconnecting>),
}

impl Transport {
    /// Use an existing session as-is.
    pub fn new(connection: Connection) -> Self {
        Self::from(Inner::Fixed(connection))
    }

    /// QUIC send priority for each call's stream, by request URI; higher is sent first.
    pub fn with_priority(
        mut self,
        priority: impl Fn(&http::Uri) -> i32 + Send + Sync + 'static,
    ) -> Self {
        self.priority = Some(Arc::new(priority));
        self
    }

    /// Open on the first RPC and re-open with backoff whenever the session closes.
    /// Concurrent callers share one attempt.
    pub fn reconnecting<F, Fut>(open: F, options: ReconnectOptions) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Connection, Error>> + Send + 'static,
    {
        Self::from(Inner::Reconnecting(Arc::new(Reconnecting {
            open: Box::new(move || open().boxed()),
            options,
            link: Mutex::default(),
        })))
    }

    /// Close the session; a reconnecting transport also stops redialling.
    /// Later calls fail with [`ConnectionError::LocallyClosed`].
    pub fn close(&self) {
        match &*self.inner {
            Inner::Fixed(connection) => connection.close(VarInt::from_u32(0), b""),
            Inner::Reconnecting(link) => link.close(),
        }
    }

    async fn session(&self) -> Result<Connection, Error> {
        match &*self.inner {
            Inner::Fixed(connection) => Ok(connection.clone()),
            Inner::Reconnecting(link) => match link.session() {
                Some(dial) => dial.await.map_err(Error::Session),
                None => Err(ConnectionError::LocallyClosed.into()),
            },
        }
    }
}

/// Open a session to `url` (`https://host:port/path`) with `config`.
pub async fn connect(url: &str, config: ClientConfig) -> Result<Transport, Error> {
    Ok(Transport::new(
        Endpoint::client(config)?.connect(url).await?,
    ))
}

/// [`Transport::reconnecting`] over `url` with `config`.
pub fn reconnect(
    url: impl Into<String>,
    config: ClientConfig,
    options: ReconnectOptions,
) -> Result<Transport, Error> {
    let endpoint = Arc::new(Endpoint::client(config)?);
    let url = url.into();
    Ok(Transport::reconnecting(
        move || {
            let endpoint = endpoint.clone();
            let url = url.clone();
            async move { Ok(endpoint.connect(url).await?) }
        },
        options,
    ))
}

impl From<Inner> for Transport {
    fn from(inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
            priority: None,
        }
    }
}

impl ClientTransport for Transport {
    type ResponseBody = Body;
    type Error = Error;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> Pin<Box<dyn Future<Output = Result<Response<Body>, Error>> + Send + 'static>> {
        let transport = self.clone();
        Box::pin(async move {
            let priority = transport.priority.as_ref().map(|p| p(request.uri()));
            call(transport.session().await?, request, priority).await
        })
    }
}

async fn call(
    connection: Connection,
    mut request: Request<ClientBody>,
    priority: Option<i32>,
) -> Result<Response<Body>, Error> {
    // connectrpc builds absolute URIs; HTTP/1.1 wants origin-form plus Host.
    if let Some(authority) = request.uri().authority().cloned() {
        if let Ok(host) = HeaderValue::from_str(authority.as_str()) {
            request.headers_mut().entry(HOST).or_insert(host);
        }
        let mut parts = request.uri().clone().into_parts();
        parts.scheme = None;
        parts.authority = None;
        if let Ok(uri) = http::Uri::from_parts(parts) {
            *request.uri_mut() = uri;
        }
    }

    let (send, recv) = connection.open_bi().await?.await?;
    if let Some(priority) = priority {
        send.set_priority(priority);
    }
    let (mut sender, conn) = http1::handshake(stream_io(send, recv)).await?;
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(error = %e, "call connection ended");
        }
    });
    let response = sender.send_request(request).await?;
    Ok(response.map(|b| b.map_err(io::Error::other).boxed()))
}

type Dial = Shared<BoxFuture<'static, Result<Connection, Arc<Error>>>>;

struct Reconnecting {
    open: Box<dyn Fn() -> BoxFuture<'static, Result<Connection, Error>> + Send + Sync>,
    options: ReconnectOptions,
    link: Mutex<Link>,
}

#[derive(Default)]
struct Link {
    current: Option<(u64, Dial)>,
    generation: u64,
    failures: u32,
    next_attempt_at: Option<Instant>,
    ever_connected: bool,
    closed: bool,
}

impl Link {
    fn state(&self, phase: LinkPhase) -> LinkState {
        LinkState {
            phase,
            failures: self.failures,
            next_attempt_at: self.next_attempt_at,
        }
    }

    fn down(&self) -> LinkPhase {
        if self.ever_connected {
            LinkPhase::Reconnecting
        } else {
            LinkPhase::Connecting
        }
    }
}

impl Drop for Reconnecting {
    fn drop(&mut self) {
        close_current(self.link.get_mut().unwrap());
    }
}

fn close_current(link: &mut Link) {
    link.closed = true;
    if let Some(Some(Ok(connection))) = link.current.take().map(|(_, dial)| dial.peek().cloned()) {
        connection.close(VarInt::from_u32(0), b"");
    }
}

impl Reconnecting {
    fn close(&self) {
        close_current(&mut self.link.lock().unwrap());
    }

    fn session(self: &Arc<Self>) -> Option<Dial> {
        let mut link = self.link.lock().unwrap();
        if link.closed {
            return None;
        }
        if let Some((_, dial)) = &link.current {
            return Some(dial.clone());
        }
        link.generation += 1;
        let generation = link.generation;
        let wait = link.next_attempt_at.map_or(Duration::ZERO, |at| {
            at.saturating_duration_since(Instant::now())
        });
        let weak = Arc::downgrade(self);
        let dial: Dial = async move {
            tokio::time::sleep(wait).await;
            let Some(this) = weak.upgrade() else {
                return Err(Arc::new(ConnectionError::LocallyClosed.into()));
            };
            match (this.open)().await {
                Ok(connection) => {
                    this.connected(generation, &connection);
                    Ok(connection)
                }
                Err(e) => {
                    this.failed(generation);
                    Err(Arc::new(e))
                }
            }
        }
        .boxed()
        .shared();
        link.current = Some((generation, dial.clone()));
        Some(dial)
    }

    fn connected(self: &Arc<Self>, generation: u64, connection: &Connection) {
        let state = {
            let mut link = self.link.lock().unwrap();
            if link.closed {
                connection.close(VarInt::from_u32(0), b"");
                return;
            }
            link.failures = 0;
            link.next_attempt_at = None;
            link.ever_connected = true;
            link.state(LinkPhase::Connected)
        };
        self.emit(state);

        let weak = Arc::downgrade(self);
        let connection = connection.clone();
        tokio::spawn(async move {
            connection.closed().await;
            let Some(this) = weak.upgrade() else {
                return;
            };
            let state = {
                let mut link = this.link.lock().unwrap();
                if !link.current.as_ref().is_some_and(|(g, _)| *g == generation) {
                    return;
                }
                link.current = None;
                link.state(link.down())
            };
            this.emit(state);
        });
    }

    fn failed(&self, generation: u64) {
        let state = {
            let mut link = self.link.lock().unwrap();
            if link.closed {
                return;
            }
            link.failures += 1;
            let backoff = self
                .options
                .initial_backoff
                .saturating_mul(1u32 << (link.failures - 1).min(31))
                .min(self.options.max_backoff)
                .mul_f64(0.5 + 0.5 * jitter());
            link.next_attempt_at = Some(Instant::now() + backoff);
            if link.current.as_ref().is_some_and(|(g, _)| *g == generation) {
                link.current = None;
            }
            link.state(link.down())
        };
        self.emit(state);
    }

    // Never called with the lock held: `on_state` may use the transport.
    fn emit(&self, state: LinkState) {
        if let Some(on_state) = &self.options.on_state {
            on_state(state);
        }
    }
}

fn jitter() -> f64 {
    (RandomState::new().hash_one(0) >> 11) as f64 / (1u64 << 53) as f64
}
