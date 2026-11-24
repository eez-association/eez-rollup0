use std::{
    collections::HashMap,
    fs::{File, OpenOptions, TryLockError},
    net::{TcpListener, UdpSocket},
    sync::{LazyLock, Mutex},
};

/// Ports this process has claimed, each with its lock file held open.
///
/// A claim locks `<tmp>/eez-testkit-ports/<port>` until this process exits.
/// nextest runs each test in its own process, so test processes running side
/// by side never hand out the same port, also while a child that will bind it
/// is starting or restarting.
static CLAIMED: LazyLock<Mutex<HashMap<u16, File>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Claims `port` for this process; false when this or another process holds it.
fn claim(claimed: &mut HashMap<u16, File>, port: u16) -> bool {
    if claimed.contains_key(&port) {
        return false;
    }
    let dir = std::env::temp_dir().join("eez-testkit-ports");
    std::fs::create_dir_all(&dir).expect("create port lock directory");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(port.to_string()))
        .expect("open port lock file");
    match file.try_lock() {
        Ok(()) => {
            claimed.insert(port, file);
            true
        }
        Err(TryLockError::WouldBlock) => false,
        Err(TryLockError::Error(e)) => panic!("lock port {port}: {e}"),
    }
}

/// Keeps selected sockets bound while a child command is being assembled.
///
/// The lease must be released immediately before spawning the child. This
/// cannot make a subprocess bind atomic, but it reduces the race from the
/// entire fixture-setup interval to the `drop`/`spawn` boundary. The port's
/// claim outlives the lease (see [`CLAIMED`]).
pub(crate) struct PortLease {
    port: u16,
    _tcp: Option<TcpListener>,
    _tcp_secondary: Option<TcpListener>,
    _udp: Option<UdpSocket>,
}

impl PortLease {
    pub(crate) fn tcp() -> Self {
        let mut claimed = CLAIMED.lock().expect("claimed ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP reservation");
            let port = listener.local_addr().expect("TCP local_addr").port();
            if claim(&mut claimed, port) {
                return Self {
                    port,
                    _tcp: Some(listener),
                    _tcp_secondary: None,
                    _udp: None,
                };
            }
        }
    }

    pub(crate) fn udp() -> Self {
        let mut claimed = CLAIMED.lock().expect("claimed ports lock");
        loop {
            let socket = UdpSocket::bind("127.0.0.1:0").expect("bind UDP reservation");
            let port = socket.local_addr().expect("UDP local_addr").port();
            if claim(&mut claimed, port) {
                return Self {
                    port,
                    _tcp: None,
                    _tcp_secondary: None,
                    _udp: Some(socket),
                };
            }
        }
    }

    pub(crate) fn tcp_udp() -> Self {
        let mut claimed = CLAIMED.lock().expect("claimed ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP reservation");
            let port = listener.local_addr().expect("TCP local_addr").port();
            if claimed.contains_key(&port) {
                continue;
            }
            let Ok(socket) = UdpSocket::bind(("127.0.0.1", port)) else {
                continue;
            };
            if !claim(&mut claimed, port) {
                continue;
            }
            return Self {
                port,
                _tcp: Some(listener),
                _tcp_secondary: None,
                _udp: Some(socket),
            };
        }
    }

    /// Reserve an HTTP port and its implicit `port + 1` WebSocket port.
    pub(crate) fn http_pair() -> Self {
        let mut claimed = CLAIMED.lock().expect("claimed ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP reservation");
            let port = listener.local_addr().expect("HTTP local_addr").port();
            let Some(ws_port) = port.checked_add(1) else {
                continue;
            };
            if claimed.contains_key(&port) || claimed.contains_key(&ws_port) {
                continue;
            }
            let Ok(ws_listener) = TcpListener::bind(("127.0.0.1", ws_port)) else {
                continue;
            };
            if !claim(&mut claimed, port) {
                continue;
            }
            if !claim(&mut claimed, ws_port) {
                // Drops the file, which releases `port` again.
                claimed.remove(&port);
                continue;
            }
            return Self {
                port,
                _tcp: Some(listener),
                _tcp_secondary: Some(ws_listener),
                _udp: None,
            };
        }
    }

    pub(crate) const fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A port another process holds is not handed out until it lets go. Two
    /// claim tables stand for two processes: each claim opens its own file,
    /// and the locks of two open files conflict even within one process.
    #[test]
    fn a_port_claimed_by_another_process_is_not_handed_out() {
        let mut first = HashMap::new();
        let mut second = HashMap::new();
        let port = loop {
            let port = TcpListener::bind("127.0.0.1:0")
                .and_then(|listener| listener.local_addr())
                .expect("free port")
                .port();
            if claim(&mut first, port) {
                break port;
            }
        };
        assert!(!claim(&mut second, port));
        assert!(!claim(&mut first, port));
        drop(first);
        assert!(claim(&mut second, port));
    }
}
