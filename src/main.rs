use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
enum ConnState {
    Established,
    Connecting,
    Closed,
}

// ConnectionMetrics contains client connections informations.
struct ConnectionMetrics {
    addr: SocketAddr,
    state: ConnState,
    // state: std::sync::Mutex<ConnState>,
    // bytes sent by client to server
    bytes_received: AtomicUsize,
    // bytes sent by server to client
    bytes_sent: AtomicUsize,
    conn_time: std::time::Instant,
}

impl ConnectionMetrics {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            // state: std::sync::Mutex::new(ConnState::Connecting),
            state: ConnState::Connecting,
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            conn_time: std::time::Instant::now(),
        }
    }

    fn set_conn_state(&mut self, state: ConnState) {
        self.state = state;
    }

    // fn set_conn_state(&self, state: ConnState) {
    //     *self.state.lock().unwrap() = state;
    // }

    // fn up_bytes() {
    //
    // }
    //
    // fn down_bytes() {
    //
    // }

    // fn get_conn_state(&self) -> &ConnState {
    //     &self.state
    // }
}

async fn process(mut client: TcpStream, addr: &'static str, conn: mut ConnectionMetrics/*, conn: Arc<ConnectionMetrics>*/) {
    // Disable Nagle's algorithm
    client.set_nodelay(true).unwrap();

    // Connect to the server, stream is for forwarding the data
    // from server to client.
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Disable Nagle's algorithm
    stream.set_nodelay(true).unwrap();

    conn.set_conn_state(ConnState::Established);

    // up: bytes sent by client to server
    // down: bytes sent by server to client
    let mut up = [0u8; 8192];
    let mut down = [0u8; 8192];

    loop {
        tokio::select! {
            // upstream read from client write to server
            n = client.read(&mut up) => {
                let n = n.unwrap();
                if n == 0 {
                    conn.set_conn_state(ConnState::Closed);
                    break;
                }
                conn.bytes_received.fetch_add(n, Ordering::Relaxed);
                stream.write_all(&up[..n]).await.unwrap();
            }

            // downstream read from server write to client
            n = stream.read(&mut down) => {
                let n = n.unwrap();
                if n == 0 {
                    break;
                }
                conn.bytes_sent.fetch_add(n, Ordering::Relaxed);
                client.write_all(&down[..n]).await.unwrap();
            }

        }
    }

    conn.set_conn_state(ConnState::Closed);
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let s_addr: &'static str = "127.0.0.1:8000";
    let c_addr: &'static str = "tcpbin.net:52227";
    // let c_addr: &'static str = "tcpbin.com:4242";

    // Listen as a server
    let listener = TcpListener::bind(s_addr).await?;
    println!("netmeter listening on {}", s_addr);

    loop {
        let (socket, addr) = listener.accept().await?;

        // let conn = Arc::new(ConnectionMetrics::new(addr));
        let mut conn = ConnectionMetrics::new(addr);
        // conn_list.lock().await.push(Arc::clone(&conn));

        // A new task is spawned for each inbound socket.
        // The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, c_addr, mut conn/*, Arc::clone(&conn)*/).await;
        });
    }
}
