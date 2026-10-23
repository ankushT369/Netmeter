
mod conn_met;

use conn_met::{ConnState, ConnectionMetrics};

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

async fn process(mut client: TcpStream, addr: &'static str, mut conn: ConnectionMetrics) {
    // Disable Nagle's algorithm
    if let Err(e) = client.set_nodelay(true) {
        eprintln!("Failed to set client TCP_NODELAY: {e}");
        return;
    }

    // Connect to the server, stream is for forwarding the data
    // from server to client.
    let mut stream = match TcpStream::connect(addr).await {
        Ok(stream) => stream,
        Err(e) => {
            eprintln!("Failed to connect to upstream server: {e}");
            conn.set_conn_state(ConnState::Closed);
            return;
        }
    };

    // Disable Nagle's algorithm
    if let Err(e) = stream.set_nodelay(true) {
        eprintln!("Failed to set upstream TCP_NODELAY: {e}");
        conn.set_conn_state(ConnState::Closed);
        return;
    }

    conn.set_conn_state(ConnState::Established);

    // up: bytes sent by client to server
    // down: bytes sent by server to client
    let mut up = [0u8; 8192];
    let mut down = [0u8; 8192];

    loop {
        tokio::select! {
            // upstream read from client write to server
            result = client.read(&mut up) => {
                let n = match result {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Client read error: {e}");
                        break;
                    }
                };

                if n == 0 {
                    conn.set_conn_state(ConnState::Closed);
                    break;
                }

                conn.bytes_received.fetch_add(n, Ordering::Relaxed);

                if let Err(e) = stream.write_all(&up[..n]).await {
                    eprintln!("Upstream write error: {e}");
                    break;
                }
            }

            // downstream read from server write to client
            result = stream.read(&mut down) => {
                let n = match result {
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Upstream read error: {e}");
                        break;
                    }
                };

                if n == 0 {
                    break;
                }

                conn.bytes_sent.fetch_add(n, Ordering::Relaxed);

                if let Err(e) = client.write_all(&down[..n]).await {
                    eprintln!("Client write error: {e}");
                    break;
                }
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
        let conn = ConnectionMetrics::new(addr);
        // conn_list.lock().await.push(Arc::clone(&conn));

        // A new task is spawned for each inbound socket.
        // The socket is moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket, c_addr, conn).await;
        });
    }
}

