//! Resumable HTTP reader with transparent retry on failure.
//!
//! When a streaming response body aborts mid-transfer, this [`AsyncRead`] implementation
//! issues an HTTP `Range` request to resume from the last successfully delivered byte.
//! The downstream consumer (e.g. a gzip / zip / tar extractor) sees a seamless byte
//! stream and is unaware of the reconnect.
//!
//! This addresses production failures observed when downloading large wheels (hundreds of
//! megabytes) through HTTP/2 intermediaries that terminate streams mid-response under slow
//! client conditions — for example, Istio/Envoy's `stream_idle_timeout` firing and sending
//! an `RST_STREAM` frame.
//!
//! See `references astral-sh/uv#8692` and Skydio's `nexus-uv-download-failure-investigation.md`.

use std::error::Error as StdError;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::TryStreamExt;
use reqwest::Response;
use tokio::io::{AsyncRead, ReadBuf};
use tracing::{debug, trace, warn};
use url::Url;

use uv_redacted::DisplaySafeUrl;

use crate::BaseClient;
use crate::retry::{RetryState, reqwest_error_to_io_error};

/// Extension trait to convert a [`reqwest::Response`] into a resumable async reader.
pub trait ResponseExt {
    /// Wrap this response in a [`ResumableReader`] that transparently reconnects on transient
    /// network failures using HTTP `Range` requests.
    ///
    /// Returns [`ResumableError::RangeNotSupported`] if the server did not advertise
    /// `Accept-Ranges: bytes` on the initial response.
    fn resumable_stream(
        self,
        client: BaseClient,
        retry_state: Arc<Mutex<RetryState>>,
    ) -> Result<ResumableReader, ResumableError>;

    /// Return `true` if the server advertised `Accept-Ranges: bytes`.
    fn supports_range_requests(&self) -> bool;
}

impl ResponseExt for Response {
    fn resumable_stream(
        self,
        client: BaseClient,
        retry_state: Arc<Mutex<RetryState>>,
    ) -> Result<ResumableReader, ResumableError> {
        let url = self.url().clone();
        ResumableReader::new(client, url, self, retry_state)
    }

    fn supports_range_requests(&self) -> bool {
        self.headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|value| value.to_str().ok())
            == Some("bytes")
    }
}

/// Error returned when the resumable download cannot proceed.
#[derive(Debug, thiserror::Error)]
pub enum ResumableError {
    #[error("Server does not support Range requests (no `Accept-Ranges: bytes` header)")]
    RangeNotSupported,
    #[error(transparent)]
    Request(#[from] reqwest_middleware::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Internal reader state machine.
enum ReaderState {
    /// Actively reading bytes from the current response body.
    Reading {
        stream: Pin<Box<dyn AsyncRead + Send>>,
    },
    /// Waiting out a backoff interval before issuing the next Range request.
    Backoff { sleep: Pin<Box<tokio::time::Sleep>> },
    /// A Range request is in flight.
    Reconnecting {
        future: Pin<Box<dyn Future<Output = Result<Response, reqwest_middleware::Error>> + Send>>,
    },
    /// Terminal failure — the message is surfaced on the next `poll_read`.
    Failed(String),
    /// All expected bytes have been delivered.
    Done,
}

/// An [`AsyncRead`] that resumes from HTTP `Range` requests on transient failures.
///
/// Error classification relies on [`retryable_on_request_failure`] via [`RetryState`]; once
/// the shared retry budget is exhausted, the reader returns the last network error verbatim
/// so the caller can report it.
///
/// [`retryable_on_request_failure`]: crate::retryable_on_request_failure
pub struct ResumableReader {
    /// HTTP client used for re-issuing GETs with a `Range` header.
    client: BaseClient,
    /// URL being downloaded.
    url: Url,
    /// Expected total bytes from the initial response's `Content-Length`, if known.
    content_length: Option<u64>,
    /// Byte offset already delivered to the downstream consumer.
    bytes_delivered: u64,
    /// Middleware retries accumulated across reconnection requests, pending consumption by
    /// the next [`RetryState::should_retry`] call.
    pending_middleware_retries: u32,
    /// Number of reconnection attempts issued (for logging only).
    reconnect_attempts: u32,
    /// Shared retry budget across the outer cached-client loop and this reader.
    retry_state: Arc<Mutex<RetryState>>,
    /// Current state.
    state: ReaderState,
}

impl ResumableReader {
    /// Create a new resumable reader from an initial successful HTTP response.
    pub fn new(
        client: BaseClient,
        url: Url,
        initial_response: Response,
        retry_state: Arc<Mutex<RetryState>>,
    ) -> Result<Self, ResumableError> {
        if !initial_response.supports_range_requests() {
            return Err(ResumableError::RangeNotSupported);
        }
        let content_length = initial_response.content_length();
        let pending_middleware_retries = initial_response
            .extensions()
            .get::<reqwest_retry::RetryCount>()
            .map(|count| count.value())
            .unwrap_or(0);

        debug!(
            "Opening ResumableReader for {} (content_length: {:?})",
            DisplaySafeUrl::from(url.clone()),
            content_length,
        );
        let stream = response_to_async_read(initial_response);

        Ok(Self {
            client,
            url,
            content_length,
            bytes_delivered: 0,
            pending_middleware_retries,
            reconnect_attempts: 0,
            retry_state,
            state: ReaderState::Reading { stream },
        })
    }

    /// Return `true` if the given I/O error came from a stream-level network failure that a
    /// Range-based reconnect could recover from.
    ///
    /// PR 1 ([`reqwest_error_to_io_error`]) ensures that streaming-body failures surface with a
    /// meaningful [`io::ErrorKind`], so this check is a plain kind match — no string inspection
    /// of the error's Display output is needed.
    fn is_transient_error(err: &io::Error) -> bool {
        matches!(
            err.kind(),
            io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::UnexpectedEof
                | io::ErrorKind::TimedOut
                | io::ErrorKind::InvalidData
        )
    }

    /// Consult the shared retry budget for a backoff duration, accounting for any middleware
    /// retries already incurred by the in-flight request.
    fn next_backoff(&mut self) -> Option<Duration> {
        let mut state = self
            .retry_state
            .lock()
            .expect("RetryState mutex poisoned by a panicking task");
        let retries = std::mem::take(&mut self.pending_middleware_retries);
        // We treat the stream error as transient ourselves (the caller already filtered via
        // `is_transient_error`), so ask the policy for a backoff directly by passing a
        // synthetic io::Error of a transient kind.
        let synthetic = io::Error::from(io::ErrorKind::ConnectionReset);
        state.should_retry(&synthetic, retries)
    }

    /// Enqueue a Range request to resume from `bytes_delivered`.
    fn start_reconnect(&mut self) {
        self.reconnect_attempts += 1;
        let position = self.bytes_delivered;
        let range_header = format!("bytes={position}-");
        let url = self.url.clone();
        let client = self.client.clone();

        debug!(
            "Resuming download of {} from byte {} (attempt {})",
            DisplaySafeUrl::from(url.clone()),
            position,
            self.reconnect_attempts,
        );

        let future = Box::pin(async move {
            client
                .for_host(&DisplaySafeUrl::from(url.clone()))
                .get(url)
                .header(reqwest::header::RANGE, range_header)
                .send()
                .await
        });
        self.state = ReaderState::Reconnecting { future };
    }

    /// Validate a reconnect response and transition back to `Reading`.
    fn handle_reconnect_response(&mut self, response: Response) -> Result<(), String> {
        trace!(
            "Reconnect response status={}, content-range={:?}",
            response.status(),
            response.headers().get(reqwest::header::CONTENT_RANGE)
        );

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(format!(
                "expected 206 Partial Content for resumed download, got {}",
                response.status()
            ));
        }

        // Confirm the server's reported total size matches the original Content-Length.
        if let (Some(expected), Some(actual_range)) = (
            self.content_length,
            response.headers().get(reqwest::header::CONTENT_RANGE),
        ) && let Some(total) = actual_range
            .to_str()
            .ok()
            .and_then(|header| header.split('/').next_back())
            .and_then(|size| size.parse::<u64>().ok())
            && total != expected
        {
            return Err(format!(
                "server reports inconsistent total size: expected {expected}, got {total}"
            ));
        }

        // Accumulate middleware retries from the successful reconnection.
        let middleware_retries = response
            .extensions()
            .get::<reqwest_retry::RetryCount>()
            .map(|count| count.value())
            .unwrap_or(0);
        self.pending_middleware_retries += middleware_retries;

        self.state = ReaderState::Reading {
            stream: response_to_async_read(response),
        };
        Ok(())
    }
}

impl AsyncRead for ResumableReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match &mut self.state {
                ReaderState::Reading { stream } => {
                    let before = buf.filled().len();
                    return match stream.as_mut().poll_read(cx, buf) {
                        Poll::Ready(Ok(())) => {
                            let bytes_read = (buf.filled().len() - before) as u64;
                            if bytes_read > 0 {
                                self.bytes_delivered += bytes_read;
                                Poll::Ready(Ok(()))
                            } else if let Some(expected) = self.content_length
                                && self.bytes_delivered < expected
                            {
                                // Premature EOF — treat as a transient failure.
                                trace!(
                                    "Premature EOF at byte {} of {}; will attempt resume",
                                    self.bytes_delivered, expected,
                                );
                                if let Some(backoff) = self.next_backoff() {
                                    self.state = ReaderState::Backoff {
                                        sleep: Box::pin(tokio::time::sleep(backoff)),
                                    };
                                    continue;
                                }
                                self.state = ReaderState::Failed(
                                    "retry budget exhausted while recovering from premature EOF"
                                        .to_string(),
                                );
                                continue;
                            } else {
                                self.state = ReaderState::Done;
                                Poll::Ready(Ok(()))
                            }
                        }
                        Poll::Ready(Err(err)) if Self::is_transient_error(&err) => {
                            trace!(
                                "Transient stream error at byte {}: {} (kind: {:?})",
                                self.bytes_delivered,
                                err,
                                err.kind(),
                            );
                            if let Some(backoff) = self.next_backoff() {
                                self.state = ReaderState::Backoff {
                                    sleep: Box::pin(tokio::time::sleep(backoff)),
                                };
                                continue;
                            }
                            // Budget exhausted — propagate the original error.
                            self.state = ReaderState::Failed(format!(
                                "retry budget exhausted after transient stream error: {err}"
                            ));
                            Poll::Ready(Err(err))
                        }
                        Poll::Ready(Err(err)) => {
                            // Non-transient — propagate immediately.
                            self.state = ReaderState::Failed(format!(
                                "non-transient stream error: {err}"
                            ));
                            Poll::Ready(Err(err))
                        }
                        Poll::Pending => Poll::Pending,
                    };
                }

                ReaderState::Backoff { sleep } => match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => self.start_reconnect(),
                    Poll::Pending => return Poll::Pending,
                },

                ReaderState::Reconnecting { future } => match future.as_mut().poll(cx) {
                    Poll::Ready(Ok(response)) => match self.handle_reconnect_response(response) {
                        Ok(()) => debug!("Resumed download successfully"),
                        Err(message) => {
                            warn!("Reconnection rejected: {message}");
                            self.state = ReaderState::Failed(message.clone());
                            return Poll::Ready(Err(io::Error::other(message)));
                        }
                    },
                    Poll::Ready(Err(err)) => {
                        trace!(
                            "Reconnection attempt {} failed: {err}",
                            self.reconnect_attempts
                        );
                        if let Some(backoff) = self.next_backoff() {
                            self.state = ReaderState::Backoff {
                                sleep: Box::pin(tokio::time::sleep(backoff)),
                            };
                            continue;
                        }
                        let message = format!(
                            "retry budget exhausted after {} reconnection attempt(s): {err}",
                            self.reconnect_attempts,
                        );
                        warn!("{message}");
                        self.state = ReaderState::Failed(message.clone());
                        return Poll::Ready(Err(io::Error::other(message)));
                    }
                    Poll::Pending => return Poll::Pending,
                },

                ReaderState::Failed(message) => {
                    let message = std::mem::take(message);
                    return Poll::Ready(Err(io::Error::other(message)));
                }

                ReaderState::Done => return Poll::Ready(Ok(())),
            }
        }
    }
}

/// Convert a [`reqwest::Response`] body into an [`AsyncRead`] whose errors preserve
/// [`io::ErrorKind`] via [`reqwest_error_to_io_error`].
fn response_to_async_read(response: Response) -> Pin<Box<dyn AsyncRead + Send>> {
    let stream = response.bytes_stream().map_err(reqwest_error_to_io_error);
    Box::pin(tokio_util::io::StreamReader::new(stream))
}

// `StdError` is used by doc-link lints; keep the import.
#[allow(dead_code)]
fn _doc_link_retainer(_: &dyn StdError) {}
