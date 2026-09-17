use std::{
    collections::HashSet,
    net::{TcpListener, UdpSocket},
    sync::{LazyLock, Mutex},
};

static ASSIGNED: LazyLock<Mutex<HashSet<u16>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

/// Keeps selected sockets bound while a child command is being assembled.
///
/// The lease must be released immediately before spawning the child. This
/// cannot make a subprocess bind atomic, but it reduces the race from the
/// entire fixture-setup interval to the `drop`/`spawn` boundary.
pub(crate) struct PortLease {
    port: u16,
    _tcp: Option<TcpListener>,
    _tcp_secondary: Option<TcpListener>,
    _udp: Option<UdpSocket>,
}

impl PortLease {
    pub(crate) fn tcp() -> Self {
        let mut used = ASSIGNED.lock().expect("assigned ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP reservation");
            let port = listener.local_addr().expect("TCP local_addr").port();
            if used.insert(port) {
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
        let mut used = ASSIGNED.lock().expect("assigned ports lock");
        loop {
            let socket = UdpSocket::bind("127.0.0.1:0").expect("bind UDP reservation");
            let port = socket.local_addr().expect("UDP local_addr").port();
            if used.insert(port) {
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
        let mut used = ASSIGNED.lock().expect("assigned ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind TCP reservation");
            let port = listener.local_addr().expect("TCP local_addr").port();
            if used.contains(&port) {
                continue;
            }
            let Ok(socket) = UdpSocket::bind(("127.0.0.1", port)) else {
                continue;
            };
            used.insert(port);
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
        let mut used = ASSIGNED.lock().expect("assigned ports lock");
        loop {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP reservation");
            let port = listener.local_addr().expect("HTTP local_addr").port();
            let Some(ws_port) = port.checked_add(1) else {
                continue;
            };
            if used.contains(&port) || used.contains(&ws_port) {
                continue;
            }
            let Ok(ws_listener) = TcpListener::bind(("127.0.0.1", ws_port)) else {
                continue;
            };
            used.insert(port);
            used.insert(ws_port);
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
