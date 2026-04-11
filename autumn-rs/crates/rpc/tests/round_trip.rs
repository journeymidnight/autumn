//! Integration tests for autumn-rpc client-server round-trip.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use autumn_rpc::client::RpcClient;
use autumn_rpc::error::StatusCode;
use autumn_rpc::pool::{ConnPool, MSG_TYPE_PING};
use autumn_rpc::server::RpcServer;
use bytes::Bytes;

fn rt() -> compio::runtime::Runtime {
    compio::runtime::Runtime::new().unwrap()
}

/// Start a server on an ephemeral port and return the bound address.
fn start_echo_server() -> SocketAddr {
    // Bind to find a free port.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    std::thread::spawn(move || {
        rt().block_on(async move {
            let server = RpcServer::new(|msg_type, payload| async move {
                match msg_type {
                    // Echo: return payload as-is
                    1 => Ok(payload),
                    // Error: return an error
                    2 => Err((StatusCode::NotFound, "not found".to_string())),
                    // Ping handler
                    MSG_TYPE_PING => Ok(Bytes::new()),
                    _ => Err((StatusCode::InvalidArgument, "unknown msg_type".to_string())),
                }
            });
            server.serve(addr).await.unwrap();
        });
    });

    // Wait for the server to start listening.
    for _ in 0..50 {
        if std::net::TcpStream::connect(addr).is_ok() {
            return addr;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("server did not start in time");
}

#[test]
fn unary_echo() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();
        let resp = client.call(1, Bytes::from_static(b"hello")).await.unwrap();
        assert_eq!(resp, Bytes::from_static(b"hello"));
    });
}

#[test]
fn unary_error() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();
        let err = client.call(2, Bytes::new()).await.unwrap_err();
        match err {
            autumn_rpc::RpcError::Status { code, message } => {
                assert_eq!(code, StatusCode::NotFound);
                assert_eq!(message, "not found");
            }
            other => panic!("expected Status error, got: {other:?}"),
        }
    });
}

#[test]
fn concurrent_requests() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();

        let mut handles = Vec::new();
        for i in 0u32..100 {
            let client = client.clone();
            handles.push(compio::runtime::spawn(async move {
                let payload = Bytes::from(i.to_le_bytes().to_vec());
                let resp = client.call(1, payload.clone()).await.unwrap();
                assert_eq!(resp, payload);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    });
}

#[test]
fn large_payload() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();
        let payload = Bytes::from(vec![0xAB; 1024 * 1024]); // 1 MB
        let resp = client.call(1, payload.clone()).await.unwrap();
        assert_eq!(resp.len(), 1024 * 1024);
        assert_eq!(resp, payload);
    });
}

#[test]
fn connection_pool_basic() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let mut pool = ConnPool::new();
        let client = pool.connect(addr).await.unwrap();
        let resp = client.call(1, Bytes::from_static(b"pool")).await.unwrap();
        assert_eq!(resp, Bytes::from_static(b"pool"));

        // Second connect should reuse the same client.
        let client2 = pool.connect(addr).await.unwrap();
        assert_eq!(client.peer_addr(), client2.peer_addr());
    });
}

#[test]
fn multiple_clients_same_server() {
    let addr = start_echo_server();
    rt().block_on(async move {
        let c1 = RpcClient::connect(addr).await.unwrap();
        let c2 = RpcClient::connect(addr).await.unwrap();

        let r1 = c1.call(1, Bytes::from_static(b"c1")).await.unwrap();
        let r2 = c2.call(1, Bytes::from_static(b"c2")).await.unwrap();

        assert_eq!(r1, Bytes::from_static(b"c1"));
        assert_eq!(r2, Bytes::from_static(b"c2"));
    });
}

/// Test that a server with a handler counting requests works correctly.
#[test]
fn stateful_handler() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let counter = Arc::new(AtomicU32::new(0));
    let counter_for_server = counter.clone();

    std::thread::spawn(move || {
        rt().block_on(async move {
            let counter = counter_for_server;
            let server = RpcServer::new(move |_msg_type, _payload| {
                let counter = counter.clone();
                async move {
                    let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
                    Ok(Bytes::from(n.to_le_bytes().to_vec()))
                }
            });
            server.serve(addr).await.unwrap();
        });
    });

    for _ in 0..50 {
        if std::net::TcpStream::connect(addr).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();
        for expected in 1u32..=10 {
            let resp = client.call(1, Bytes::new()).await.unwrap();
            let n = u32::from_le_bytes(resp[..4].try_into().unwrap());
            assert_eq!(n, expected);
        }
    });
}
