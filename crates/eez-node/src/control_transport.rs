//! Control-feed transport (P3) — who opens the TCP socket.
//!
//! Normally the prover DIALS the composer's control endpoint (the composer
//! listens). When the composer host must stay outbound-only
//! (`EEZ_CONTROL_DIAL_ADDR`), the TRANSPORT inverts: the composer dials the
//! prover machine (`eez-proverd --control-listen-addr`) and serves the same
//! three gRPC services over the outbound connection. The wire protocol is
//! unchanged — the composer remains the gRPC server either way; only the
//! direction of the TCP connect differs. Modes are mutually exclusive per
//! node. See docs/reverse-control-transport-design.md.

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::{Level, event};

/// Dial connect timeout (§7.4): bounds the wait behind a black-holing
/// firewall so the backoff cadence is predictable rather than pinned to the
/// OS TCP timeout.
const DIAL_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Minimum time a served connection must stay up before we treat the dial as
/// "good" and reset backoff. A connect-then-instant-close (e.g. the prover
/// accepts then drops on a failed source-IP allowlist check) stays below
/// this, so backoff keeps growing instead of hot-looping (§4.1 fast-reject).
const DIAL_MIN_DWELL: Duration = Duration::from_secs(5);
const DIAL_BACKOFF_START: Duration = Duration::from_secs(1);
const DIAL_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// A served control connection — a `TcpStream` that was either accepted
/// (listen mode) or established outbound (dial mode) — plus a drop guard:
/// tonic drops the IO when the HTTP/2 connection ends, which drops `_closed`
/// and wakes the dial loop to redial.
pub(crate) struct ControlIo {
    inner: TcpStream,
    _closed: Option<oneshot::Sender<()>>,
}

impl ControlIo {
    fn accepted(inner: TcpStream) -> Self {
        Self {
            inner,
            _closed: None,
        }
    }

    fn dialed(inner: TcpStream) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                inner,
                _closed: Some(tx),
            },
            rx,
        )
    }
}

impl AsyncRead for ControlIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for ControlIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

impl Connected for ControlIo {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.inner.connect_info()
    }
}

/// The two ways the control server gets its connections. `Listen` carries the
/// std listener bound (fatally) in the launch path; `Dial` carries the
/// receiving end of [`dial_loop`]'s connections.
pub(crate) enum ControlTransport {
    Listen(std::net::TcpListener),
    Dial(mpsc::Receiver<ControlIo>),
}

/// Turn the transport into the tonic `serve_with_incoming` stream. Called
/// inside the server task (the listen arm registers the std listener with
/// the runtime's reactor, which needs the async context).
pub(crate) fn incoming(
    transport: ControlTransport,
) -> io::Result<Pin<Box<dyn Stream<Item = io::Result<ControlIo>> + Send>>> {
    match transport {
        ControlTransport::Listen(listener) => {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            Ok(Box::pin(
                wrappers::TcpListenerStream::new(listener).map(|r| r.map(ControlIo::accepted)),
            ))
        }
        ControlTransport::Dial(rx) => Ok(Box::pin(
            wrappers::ReceiverStream::new(rx).map(Ok::<ControlIo, io::Error>),
        )),
    }
}

/// Reverse-transport dial loop: connect out to the prover machine, hand the
/// connection to the tonic server through `tx`, and redial whenever it drops
/// or fails. Never gives up on the peer — the composer keeps sequencing while
/// the prover machine is away and settlement resumes on reconnect (the prover
/// replays from the ring / its checkpoint).
///
/// PANICS (fatal, by design) if `tx` is closed — that means the control-feed
/// server task is gone, so there is nothing to serve into and a silently
/// sequencing-but-never-settling composer is worse than a restart. Spawned as
/// a critical task, so the panic tears the node down and the container restart
/// policy retries.
pub(crate) async fn dial_loop(addr: SocketAddr, tx: mpsc::Sender<ControlIo>) {
    let mut backoff = DIAL_BACKOFF_START;
    loop {
        match tokio::time::timeout(DIAL_CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(sock)) => {
                let _ = sock.set_nodelay(true);
                let peer = sock.peer_addr().ok();
                let (io, closed) = ControlIo::dialed(sock);
                assert!(
                    tx.send(io).await.is_ok(),
                    "control-feed server task gone; cannot serve the reverse control connection \
                     — failing the node so the restart policy retries"
                );
                let connected_at = Instant::now();
                event!(
                    name: "eez.control_feed.reverse_connected",
                    Level::INFO,
                    ?peer,
                    "reverse control transport connected (composer dialed out)"
                );
                // Resolves (with an error — the sender is dropped, never sent
                // on) when tonic tears the connection down.
                let _ = closed.await;
                let dwell = connected_at.elapsed();
                event!(
                    name: "eez.control_feed.reverse_closed",
                    Level::WARN,
                    %addr,
                    dwell_secs = dwell.as_secs(),
                    "reverse control connection closed; redialing"
                );
                if dwell >= DIAL_MIN_DWELL {
                    // A genuinely-served connection: redial promptly.
                    backoff = DIAL_BACKOFF_START;
                } else {
                    // connect-then-instant-close (fast-reject hazard): back off
                    // so a misconfigured allowlist / wrong address does not spin.
                    sleep_and_grow(&mut backoff).await;
                }
            }
            Ok(Err(e)) => {
                event!(
                    name: "eez.control_feed.reverse_dial_failed",
                    Level::WARN,
                    %addr,
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "reverse control dial failed; retrying"
                );
                sleep_and_grow(&mut backoff).await;
            }
            Err(_timeout) => {
                event!(
                    name: "eez.control_feed.reverse_dial_timeout",
                    Level::WARN,
                    %addr,
                    timeout_secs = DIAL_CONNECT_TIMEOUT.as_secs(),
                    backoff_secs = backoff.as_secs(),
                    "reverse control dial timed out; retrying"
                );
                sleep_and_grow(&mut backoff).await;
            }
        }
    }
}

async fn sleep_and_grow(backoff: &mut Duration) {
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(DIAL_BACKOFF_MAX);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `ControlIo` transparently delegates reads/writes to the inner socket —
    /// the wrapper must not alter the byte stream tonic serves over.
    #[tokio::test]
    async fn control_io_delegates_read_write() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut io = ControlIo::accepted(sock);
            let mut buf = [0u8; 5];
            io.read_exact(&mut buf).await.unwrap();
            // Echo back uppercased so the test asserts both directions.
            io.write_all(&buf.to_ascii_uppercase()).await.unwrap();
            io.flush().await.unwrap();
        });

        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut got = [0u8; 5];
        client.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"HELLO");
        server.await.unwrap();
    }

    /// The dialed drop-guard fires when the `ControlIo` is dropped (tonic
    /// dropping the connection IO), which is the dial loop's redial signal.
    #[tokio::test]
    async fn dialed_drop_guard_fires_on_drop() {
        let (io_a, _sock_b) = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let client = TcpStream::connect(addr).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            (server, client)
        };
        let (io, mut closed) = ControlIo::dialed(io_a);
        // Guard is live while the io is held.
        assert!(closed.try_recv().is_err());
        drop(io);
        // Dropping the io drops the sender ⇒ the receiver resolves (with a
        // closed error — nothing is ever sent on it).
        assert!(closed.await.is_err());
    }

    /// The `Dial` transport wraps each received socket into `incoming` items;
    /// the stream ends when the dial loop's sender is dropped.
    #[tokio::test]
    async fn dial_transport_incoming_yields_then_ends() {
        let (tx, rx) = mpsc::channel(1);
        let mut incoming = incoming(ControlTransport::Dial(rx)).unwrap();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let sock = TcpStream::connect(addr).await.unwrap();
        let (io, _guard) = ControlIo::dialed(sock);
        tx.send(io).await.unwrap();

        assert!(incoming.next().await.is_some(), "first item delivered");
        drop(tx);
        assert!(incoming.next().await.is_none(), "stream ends when tx drops");
    }
}
