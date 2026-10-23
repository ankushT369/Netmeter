use std::{
    net::SocketAddr,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
    time::Instant,
};

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ConnState {
    Established = 0,
    Connecting = 1,
    Closed = 2,
}

// ConnectionMetrics contains client connections informations.
pub struct ConnectionMetrics {
    pub addr: SocketAddr,
    state: AtomicU8,
    // bytes sent by client to server
    pub bytes_received: AtomicUsize,
    // bytes sent by server to client
    pub bytes_sent: AtomicUsize,
    pub conn_time: Instant,
}

impl ConnectionMetrics {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: AtomicU8::new(ConnState::Connecting as u8),
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            conn_time: Instant::now(),
        }
    }

    pub fn set_conn_state(&self, state: ConnState) {
        self.state.store(state as u8, Ordering::Relaxed);
    }

    pub fn get_conn_state(&self) -> ConnState {
        match self.state.load(Ordering::Relaxed) {
            0 => ConnState::Established,
            1 => ConnState::Connecting,
            2 => ConnState::Closed,
            _ => unreachable!("Invalid connection state"),
        }
    }

    pub fn get_bytes_received(&self) -> usize {
        self.bytes_received.load(Ordering::Relaxed)
    }

    pub fn get_bytes_sent(&self) -> usize {
        self.bytes_sent.load(Ordering::Relaxed)
    }

    pub fn get_conn_duration(&self) -> std::time::Duration {
        self.conn_time.elapsed()
    }
}
