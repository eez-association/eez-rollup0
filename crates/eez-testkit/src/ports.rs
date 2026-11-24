use std::{
    collections::HashMap,
    fs::{File, OpenOptions, TryLockError},
    hash::{BuildHasher, Hasher, RandomState},
    net::{TcpListener, UdpSocket},
    ops::Range,
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicU32, Ordering},
    },
};

/// The lowest port a lease hands out.
const FIRST_PORT: u16 = 10_000;
/// The start of the kernel's ephemeral range when it cannot be read (Linux's default).
const DEFAULT_EPHEMERAL_START: u16 = 32_768;

/// The ports leases come from: below the kernel's ephemeral range, which it
/// gives to outgoing connections and port-0 binds. A port from that range could
/// be taken by any process between the lease's release and the child's bind.
static LEASE_RANGE: LazyLock<Range<u16>> = LazyLock::new(|| {
    let ephemeral = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok();
    lease_range(ephemeral.as_deref())
});

/// [`FIRST_PORT`] up to the start of `ephemeral`, the kernel's
/// `ip_local_port_range`.
fn lease_range(ephemeral: Option<&str>) -> Range<u16> {
    let start = ephemeral
        .and_then(|range| range.split_whitespace().next()?.parse().ok())
        .unwrap_or(DEFAULT_EPHEMERAL_START);
    assert!(
        start > FIRST_PORT,
        "the kernel's ephemeral ports start at {start}, leaving no test ports from {FIRST_PORT} below them"
    );
    FIRST_PORT..start
}

/// The next port this process probes. Starts at a random point of
/// [`LEASE_RANGE`], so test processes running side by side probe different ports.
static NEXT_PORT: LazyLock<AtomicU32> = LazyLock::new(|| {
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u32(std::process::id());
    AtomicU32::new(hasher.finish() as u32)
});

/// A port from [`LEASE_RANGE`] to probe; successive calls walk the range.
fn next_port() -> u16 {
    let range = &*LEASE_RANGE;
    let len = u32::from(range.end - range.start);
    range.start + (NEXT_PORT.fetch_add(1, Ordering::Relaxed) % len) as u16
}

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
/// The lease must be released immediately before spawning the child. Between
/// the release and the child's bind the port stays free: the kernel gives
/// ports of [`LEASE_RANGE`] to no one on its own, and the port's claim, which
/// outlives the lease (see [`CLAIMED`]), keeps other test processes off it.
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
            let port = next_port();
            let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) else {
                continue;
            };
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
            let port = next_port();
            let Ok(socket) = UdpSocket::bind(("127.0.0.1", port)) else {
                continue;
            };
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
            let port = next_port();
            if claimed.contains_key(&port) {
                continue;
            }
            let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) else {
                continue;
            };
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
            let port = next_port();
            let ws_port = port + 1;
            if !LEASE_RANGE.contains(&ws_port)
                || claimed.contains_key(&port)
                || claimed.contains_key(&ws_port)
            {
                continue;
            }
            let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) else {
                continue;
            };
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

    #[test]
    fn the_lease_range_ends_below_the_kernels_ephemeral_range() {
        assert_eq!(lease_range(Some("32768\t60999\n")), 10_000..32_768);
        assert_eq!(lease_range(Some("40000 65000")), 10_000..40_000);
        assert_eq!(lease_range(None), 10_000..32_768);
        assert_eq!(lease_range(Some("garbage")), 10_000..32_768);
    }

    #[test]
    fn leased_ports_are_distinct_and_outside_the_ephemeral_range() {
        let ephemeral = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok();
        let range = lease_range(ephemeral.as_deref());
        let leases = [
            PortLease::tcp(),
            PortLease::tcp(),
            PortLease::udp(),
            PortLease::tcp_udp(),
            PortLease::http_pair(),
        ];
        let mut ports: Vec<u16> = leases.iter().map(PortLease::port).collect();
        // The pair's WebSocket port is leased too.
        ports.push(leases[4].port() + 1);
        for port in &ports {
            assert!(range.contains(port), "port {port} is outside {range:?}");
        }
        let mut distinct = ports.clone();
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), ports.len(), "{ports:?}");
    }

    /// A port another process holds is not handed out until it lets go. Two
    /// claim tables stand for two processes: each claim opens its own file,
    /// and the locks of two open files conflict even within one process.
    #[test]
    fn a_port_claimed_by_another_process_is_not_handed_out() {
        let mut first = HashMap::new();
        let mut second = HashMap::new();
        let port = loop {
            let port = next_port();
            if TcpListener::bind(("127.0.0.1", port)).is_ok() && claim(&mut first, port) {
                break port;
            }
        };
        assert!(!claim(&mut second, port));
        assert!(!claim(&mut first, port));
        drop(first);
        assert!(claim(&mut second, port));
    }
}
