use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::transport::server::{Connected, TcpConnectInfo};
use tracing::{Level, event};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MIN_GOOD_DWELL: Duration = Duration::from_secs(5);
const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

pub(crate) struct ControlIo {
    inner: TcpStream,
    _closed: Option<oneshot::Sender<()>>,
}

impl ControlIo {
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
}

impl Connected for ControlIo {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> TcpConnectInfo {
        self.inner.connect_info()
    }
}

pub(crate) fn incoming(
    rx: mpsc::Receiver<ControlIo>,
) -> Pin<Box<dyn Stream<Item = io::Result<ControlIo>> + Send>> {
    Box::pin(ReceiverStream::new(rx).map(Ok::<ControlIo, io::Error>))
}

pub(crate) async fn dial_loop(addr: SocketAddr, tx: mpsc::Sender<ControlIo>) {
    let mut backoff = BACKOFF_START;
    loop {
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(sock)) => {
                let _ = sock.set_nodelay(true);
                let peer = sock.peer_addr().ok();
                let (io, closed) = ControlIo::dialed(sock);
                assert!(
                    tx.send(io).await.is_ok(),
                    "control-feed server task exited; cannot serve reverse control connection"
                );
                let started = Instant::now();
                event!(
                    name: "eez.control_feed.reverse_connected",
                    Level::INFO,
                    ?peer,
                    "reverse control connected",
                );
                let _ = closed.await;
                let dwell = started.elapsed();
                event!(
                    name: "eez.control_feed.reverse_closed",
                    Level::WARN,
                    %addr,
                    dwell_secs = dwell.as_secs(),
                    "reverse control closed; redialing",
                );
                if dwell >= MIN_GOOD_DWELL {
                    backoff = BACKOFF_START;
                } else {
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
                    "reverse control dial failed",
                );
                sleep_and_grow(&mut backoff).await;
            }
            Err(_) => {
                event!(
                    name: "eez.control_feed.reverse_dial_timeout",
                    Level::WARN,
                    %addr,
                    timeout_secs = CONNECT_TIMEOUT.as_secs(),
                    backoff_secs = backoff.as_secs(),
                    "reverse control dial timed out",
                );
                sleep_and_grow(&mut backoff).await;
            }
        }
    }
}

async fn sleep_and_grow(backoff: &mut Duration) {
    tokio::time::sleep(*backoff).await;
    *backoff = (*backoff * 2).min(BACKOFF_MAX);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dialed_drop_guard_fires_on_drop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (io, closed) = ControlIo::dialed(server);
        drop(client);
        drop(io);
        assert!(closed.await.is_err());
    }

    #[tokio::test]
    async fn incoming_yields_dialed_connection() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (io, closed) = ControlIo::dialed(server);
        let (tx, rx) = mpsc::channel(1);
        tx.send(io).await.unwrap();
        drop(tx);

        let mut incoming = incoming(rx);
        let io = incoming.next().await.unwrap().unwrap();
        drop(client);
        drop(io);
        assert!(closed.await.is_err());
    }
}
