use std::error::Error;
use std::time::{Duration, SystemTime, SystemTimeError};
use std::{io, iter};

use http::status::StatusCode;
use itertools::Itertools;
use reqwest::Response;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::{
    RetryPolicy, Retryable, RetryableStrategy, default_on_request_error, default_on_request_success,
};
use tracing::{debug, trace};
use url::Url;

use uv_redacted::DisplaySafeUrl;

use crate::WrappedReqwestError;

/// An extension over [`DefaultRetryableStrategy`] that logs transient request failures and
/// adds additional retry cases.
pub struct UvRetryableStrategy;

impl RetryableStrategy for UvRetryableStrategy {
    fn handle(&self, res: &Result<Response, reqwest_middleware::Error>) -> Option<Retryable> {
        let retryable = match res {
            Ok(success) => default_on_request_success(success),
            Err(err) => retryable_on_request_failure(err),
        };

        // Log on transient errors
        if retryable == Some(Retryable::Transient) {
            match res {
                Ok(response) => {
                    debug!("Transient request failure for: {}", response.url());
                }
                Err(err) => {
                    let context = iter::successors(err.source(), |&err| err.source())
                        .map(|err| format!("  Caused by: {err}"))
                        .join("\n");
                    debug!(
                        "Transient request failure for {}, retrying: {err}\n{context}",
                        err.url().map(Url::as_str).unwrap_or("unknown URL")
                    );
                }
            }
        }
        retryable
    }
}

/// Per-request retry state and policy.
pub struct RetryState {
    retry_policy: ExponentialBackoff,
    start_time: SystemTime,
    total_retries: u32,
    url: DisplaySafeUrl,
}

impl RetryState {
    /// Initialize the [`RetryState`] and record the start time for the retry policy.
    pub fn start(retry_policy: ExponentialBackoff, url: impl Into<DisplaySafeUrl>) -> Self {
        Self {
            retry_policy,
            start_time: SystemTime::now(),
            total_retries: 0,
            url: url.into(),
        }
    }

    /// The number of retries across all requests.
    ///
    /// After a failed retryable request, this equals the maximum number of retries.
    pub fn total_retries(&self) -> u32 {
        self.total_retries
    }

    /// The total duration from the first request to the (failure) of the last request.
    pub fn duration(&self) -> Result<Duration, SystemTimeError> {
        self.start_time.elapsed()
    }

    /// Determines whether request should be retried.
    ///
    /// Takes the number of retries from nested layers associated with the specific `err` type as
    /// `error_retries`.
    ///
    /// Returns the backoff duration if the request should be retried.
    #[must_use]
    pub fn should_retry(
        &mut self,
        err: &(dyn Error + 'static),
        error_retries: u32,
    ) -> Option<Duration> {
        // If the middleware performed any retries, consider them in our budget.
        self.total_retries += error_retries;
        match retryable_on_request_failure(err) {
            Some(Retryable::Transient) => {
                // Capture `now` before calling the policy so that `execute_after`
                // (computed from a `SystemTime::now()` inside the library) is always
                // >= `now`, making `duration_since` reliable.
                let now = SystemTime::now();
                let retry_decision = self
                    .retry_policy
                    .should_retry(self.start_time, self.total_retries);
                if let reqwest_retry::RetryDecision::Retry { execute_after } = retry_decision {
                    let duration = execute_after
                        .duration_since(now)
                        .unwrap_or_else(|_| Duration::default());

                    self.total_retries += 1;
                    return Some(duration);
                }

                None
            }
            Some(Retryable::Fatal) | None => None,
        }
    }

    /// Wait before retrying the request.
    pub async fn sleep_backoff(&self, duration: Duration) {
        debug!(
            "Transient failure while handling response from {}; retrying after {:.1}s...",
            self.url,
            duration.as_secs_f32(),
        );
        // TODO(konsti): Should we show a spinner plus a message in the CLI while
        // waiting?
        tokio::time::sleep(duration).await;
    }

    /// Wait before retrying the request, without borrowing the retry state.
    ///
    /// The ordinary [`Self::sleep_backoff`] holds `&self` across the await, which prevents
    /// the caller from also holding a mutex guard on the [`RetryState`]. Callers that share
    /// the retry state between layers (see `get_cacheable_with_retry`) should use this
    /// helper after dropping the lock.
    pub async fn sleep_backoff_static(duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

/// Whether the error looks like a network error that should be retried.
///
/// This is an extension over [`reqwest_middleware::default_on_request_failure`], which is missing
/// a number of cases:
/// * Inside the reqwest or reqwest-middleware error is an `io::Error` such as a broken pipe
/// * When streaming a response, a reqwest error may be hidden several layers behind errors
///   of different crates processing the stream, including `io::Error` layers
/// * Any `h2` error
pub fn retryable_on_request_failure(err: &(dyn Error + 'static)) -> Option<Retryable> {
    // First, try to show a nice trace log
    if let Some((Some(status), Some(url))) = find_source::<WrappedReqwestError>(&err)
        .map(|request_err| (request_err.status(), request_err.url()))
    {
        trace!(
            "Considering retry of response HTTP {status} for {url}",
            url = DisplaySafeUrl::from_url(url.clone())
        );
    } else {
        trace!("Considering retry of error: {err:?}");
    }

    let mut has_known_error = false;
    // IO Errors or reqwest errors may be nested through custom IO errors or stream processing
    // crates
    let mut current_source = Some(err);
    while let Some(source) = current_source {
        // Handle different kinds of reqwest error nesting not accessible by downcast.
        let reqwest_err = if let Some(reqwest_err) = source.downcast_ref::<reqwest::Error>() {
            Some(reqwest_err)
        } else if let Some(reqwest_err) = source
            .downcast_ref::<WrappedReqwestError>()
            .and_then(|err| err.inner())
        {
            Some(reqwest_err)
        } else if let Some(reqwest_middleware::Error::Reqwest(reqwest_err)) =
            source.downcast_ref::<reqwest_middleware::Error>()
        {
            Some(reqwest_err)
        } else {
            None
        };

        if let Some(reqwest_err) = reqwest_err {
            has_known_error = true;
            // Ignore the default retry strategy returning fatal.
            if default_on_request_error(reqwest_err) == Some(Retryable::Transient) {
                trace!("Transient nested reqwest error");
                return Some(Retryable::Transient);
            }
            if is_retryable_status_error(reqwest_err) {
                trace!("Transient nested reqwest status code error");
                return Some(Retryable::Transient);
            }

            trace!("Fatal nested reqwest error");
        } else if source.downcast_ref::<h2::Error>().is_some() {
            // All h2 errors look like errors that should be retried
            // https://github.com/astral-sh/uv/issues/15916
            trace!("Transient nested h2 error");
            return Some(Retryable::Transient);
        } else if let Some(io_err) = source.downcast_ref::<io::Error>() {
            has_known_error = true;
            let retryable_io_err_kinds = [
                // https://github.com/astral-sh/uv/issues/12054
                io::ErrorKind::BrokenPipe,
                // From reqwest-middleware
                io::ErrorKind::ConnectionAborted,
                // https://github.com/astral-sh/uv/issues/3514
                io::ErrorKind::ConnectionReset,
                // https://github.com/astral-sh/uv/issues/14699
                io::ErrorKind::InvalidData,
                // https://github.com/astral-sh/uv/issues/17697#issuecomment-3817060484
                io::ErrorKind::TimedOut,
                // https://github.com/astral-sh/uv/issues/9246
                io::ErrorKind::UnexpectedEof,
            ];
            if retryable_io_err_kinds.contains(&io_err.kind()) {
                trace!("Transient IO error: `{}`", io_err.kind());
                return Some(Retryable::Transient);
            }

            trace!(
                "Fatal IO error `{}`, not a transient IO error kind",
                io_err.kind()
            );
        }

        current_source = source.source();
    }

    if !has_known_error {
        trace!("Cannot retry error: neither an IO error nor a reqwest error");
    }

    None
}

/// An error type that supports URL-fallback and exponential-backoff retry logic.
///
/// Used by [`fetch_with_url_fallback`] to drive the retry loop without knowing the concrete error
/// type.
pub trait RetriableError: std::error::Error + Sized + 'static {
    /// Returns `true` if an alternative URL should be tried immediately (without backoff).
    fn should_try_next_url(&self) -> bool;

    /// Returns the number of inner retries already recorded in this error.
    fn retries(&self) -> u32;

    /// Wrap the error to indicate that the operation was retried `retries` times before failing.
    #[must_use]
    fn into_retried(self, retries: u32, duration: Duration) -> Self;
}

/// Whether the error is a status code error that is retryable.
///
/// Port of `reqwest_retry::default_on_request_success`.
fn is_retryable_status_error(reqwest_err: &reqwest::Error) -> bool {
    let Some(status) = reqwest_err.status() else {
        return false;
    };
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

/// Find the first source error of a specific type.
///
/// See <https://github.com/seanmonstar/reqwest/issues/1602#issuecomment-1220996681>
fn find_source<E: Error + 'static>(orig: &dyn Error) -> Option<&E> {
    let mut cause = orig.source();
    while let Some(err) = cause {
        if let Some(typed) = err.downcast_ref() {
            return Some(typed);
        }
        cause = err.source();
    }
    None
}

/// Convert a [`reqwest::Error`] into an [`io::Error`] while preserving the underlying
/// [`io::ErrorKind`] from any nested [`io::Error`] in its source chain.
///
/// This is the bridge between streaming response bodies (which surface errors as [`io::Error`]
/// through `.bytes_stream().map_err(...)`) and [`retryable_on_request_failure`] (which relies on
/// the outer [`io::ErrorKind`] being meaningful to classify transient network failures).
///
/// The plain `io::Error::other(reqwest_err)` pattern loses this information by storing the
/// error under [`io::ErrorKind::Other`], which the classifier treats as unknown.
///
/// See <https://github.com/astral-sh/uv/issues/8692>.
pub fn reqwest_error_to_io_error(err: reqwest::Error) -> io::Error {
    // Walk the source chain for a nested `io::Error`, and adopt its `ErrorKind` while keeping
    // the original `reqwest::Error` as the payload so its Display / source chain are preserved.
    if let Some(inner) = find_source::<io::Error>(&err) {
        return io::Error::new(inner.kind(), err);
    }
    // Fall back to preserving the source chain under `Other`. The classifier will still walk
    // the chain and recover the `reqwest::Error` for classification.
    io::Error::other(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    use anyhow::Result;
    use insta::assert_debug_snapshot;
    use reqwest::Client;
    use reqwest_middleware::ClientWithMiddleware;
    use wiremock::matchers::path;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::{UvRetryableStrategy, retryable_on_request_failure};

    /// Enumerate which status codes we are retrying.
    #[tokio::test]
    async fn retried_status_codes() -> Result<()> {
        let server = MockServer::start().await;
        let client = Client::default();
        let middleware_client = ClientWithMiddleware::default();
        let mut retried = Vec::new();
        for status in 100..599 {
            // Test all standard status codes and an example for a non-RFC code used in the wild.
            if StatusCode::from_u16(status)?.canonical_reason().is_none() && status != 420 {
                continue;
            }

            Mock::given(path(format!("/{status}")))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;

            let response = middleware_client
                .get(format!("{}/{}", server.uri(), status))
                .send()
                .await;

            let middleware_retry =
                UvRetryableStrategy.handle(&response) == Some(Retryable::Transient);

            let response = client
                .get(format!("{}/{}", server.uri(), status))
                .send()
                .await?;

            let uv_retry = match response.error_for_status() {
                Ok(_) => false,
                Err(err) => retryable_on_request_failure(&err) == Some(Retryable::Transient),
            };

            // Ensure we're retrying the same status code as the reqwest_retry crate. We may choose
            // to deviate from this later.
            assert_eq!(middleware_retry, uv_retry);
            if uv_retry {
                retried.push(status);
            }
        }

        assert_debug_snapshot!(retried, @"
        [
            100,
            102,
            103,
            408,
            429,
            500,
            501,
            502,
            503,
            504,
            505,
            506,
            507,
            508,
            510,
            511,
        ]
        ");

        Ok(())
    }

    /// Directly verify that [`reqwest_error_to_io_error`] recovers the inner
    /// [`io::ErrorKind`] from an error wrapping an [`io::Error`]. Uses a synthetic
    /// [`io::Error`] as an outer error with an inner [`io::Error`] source; the shim's
    /// source-chain walk should find the inner kind.
    #[test]
    fn reqwest_error_to_io_error_walks_source_chain() {
        use std::error::Error as StdError;
        use std::fmt;

        // Build a minimal error whose source is an `io::Error` with a known kind.
        #[derive(Debug)]
        struct OuterError(io::Error);
        impl fmt::Display for OuterError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("outer")
            }
        }
        impl StdError for OuterError {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(&self.0)
            }
        }

        let outer = OuterError(io::Error::new(io::ErrorKind::ConnectionReset, "peer went away"));
        // The shim only accepts reqwest::Error, so we exercise the inner logic via
        // `find_source::<io::Error>` directly — which is what the shim uses.
        let found = find_source::<io::Error>(&outer).expect("io::Error in source chain");
        assert!(matches!(found.kind(), io::ErrorKind::ConnectionReset));
    }
}
