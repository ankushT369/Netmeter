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

struct ConnectionMetrics {
    addr: SocketAddr,
    state: std::sync::Mutex<ConnState>,
    bytes_received: AtomicUsize,
    bytes_sent: AtomicUsize,
    conn_time: std::time::Instant,
}

impl ConnectionMetrics {
    fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            state: std::sync::Mutex::new(ConnState::Connecting),
            bytes_received: AtomicUsize::new(0),
            bytes_sent: AtomicUsize::new(0),
            conn_time: std::time::Instant::now(),
        }
    }

    fn set_conn_state(&self, state: ConnState) {
        *self.state.lock().unwrap() = state;
    }

    // fn get_conn_state(&self) -> &ConnState {
    //     &self.state
    // }
}

async fn process(mut socket: TcpStream, addr: &'static str, conn: Arc<ConnectionMetrics>) {
    socket.set_nodelay(true).unwrap();

    // Connect to the server, stream is for forwarding the data
    // from clients to server.
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.set_nodelay(true).unwrap();

    conn.set_conn_state(ConnState::Established);

    // up: bytes sent by client to server
    // down: bytes sent by server to client
    let mut up = [0u8; 8192];
    let mut down = [0u8; 8192];

    loop {
        tokio::select! {
            n = socket.read(&mut up) => {
                let n = n.unwrap();
                if n == 0 {
                    conn.set_conn_state(ConnState::Closed);
                    break;
                }
                conn.bytes_received.fetch_add(n, Ordering::Relaxed);
                stream.write_all(&up[..n]).await.unwrap();
            }

            n = stream.read(&mut down) => {
                let n = n.unwrap();
                if n == 0 {
                    break;
                }
                conn.bytes_sent.fetch_add(n, Ordering::Relaxed);
                socket.write_all(&down[..n]).await.unwrap();
            }

        }
    }

    conn.set_conn_state(ConnState::Closed);
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let s_addr: &'static str = "127.0.0.1:8000";
    let c_addr: &'static str = "tcpbin.net:52227";

    let conn_list: Arc<tokio::sync::Mutex<Vec<Arc<ConnectionMetrics>>>> =
        Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let monitor_list = Arc::clone(&conn_list);

    // Listen as a server
    let listener = TcpListener::bind(s_addr).await?;
    println!("nethawk listening on {}", s_addr);

    // tokio::spawn(async move {
    //     loop {
    //         clearscreen::clear().expect("failed to clear screen");
    //         let list = monitor_list.lock().await;
    //
    //         for conn in list.iter() {
    //             let received = conn.bytes_received.load(Ordering::Relaxed);
    //             let sent = conn.bytes_sent.load(Ordering::Relaxed);
    //             let state = conn.state.lock().unwrap();
    //
    //             println!(
    //                 "Client: {} | State: {:?} | Received: {} bytes | Sent: {} bytes",
    //                 conn.addr, state, received, sent
    //             );
    //         }
    //         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    //     }
    // });

    loop {
        let (socket, addr) = listener.accept().await?;

        let conn = Arc::new(ConnectionMetrics::new(addr));
        conn_list.lock().await.push(Arc::clone(&conn));

        let conn_clone = Arc::clone(&conn);

        // A new task is spawned for each inbound socket.
        // The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, c_addr, conn_clone).await;
        });
    }
}
