use wtransport::error::{ConnectingError, ConnectionError, StreamOpeningError};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("endpoint: {0}")]
    Endpoint(#[from] std::io::Error),
    #[error("connecting: {0}")]
    Connecting(#[from] ConnectingError),
    #[error("connection: {0}")]
    Connection(#[from] ConnectionError),
    #[error("open stream: {0}")]
    OpenStream(#[from] StreamOpeningError),
    #[error("http: {0}")]
    Http(#[from] hyper::Error),
    /// A reconnecting transport could not open its session.
    #[error("session: {0}")]
    Session(#[source] std::sync::Arc<Error>),
}
