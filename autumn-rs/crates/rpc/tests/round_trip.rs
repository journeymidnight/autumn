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

/// F099-I-fix stress test: 2048 concurrent `call_vectored` futures sharing a
/// single `RpcClient` must ALL complete successfully. This mirrors the
/// F099-I concern scenario where 256 ps-conn tasks × 8 inflight per-frame
/// futures = 2048 concurrent `tx.send()` calls funneled into the PS→extent-
/// node RpcConn writer_task. The original F099-I note speculated that the
/// kernel might reject the resulting SendMsg op with EINVAL (e.g.
/// UIO_MAXIOV overflow on the per-msg iovec list, mpsc reservation
/// exhaustion, or io_uring SQ overflow).
///
/// This test nails down that the writer_task's `write_vectored_all` path
/// does NOT intrinsically fail under 2048 concurrent 2-iov submissions on
/// loopback TCP.  Each `call_vectored(msg_type, vec![part])` produces a
/// `SubmitMsg::Vectored { bufs: [hdr, part] }` with exactly 2 iovecs —
/// identical to the shape `StreamClient::launch_append` generates for
/// each replica.  The writer_task serialises them (single writer), so
/// iovlen per syscall stays at 2.
///
/// If the test passes, it confirms:
///   * 2-iov SendMsg is never rejected by the kernel regardless of
///     concurrent pressure on the submit channel.
///   * `futures::channel::mpsc::bounded(1024)` handles 2048 simultaneous
///     senders (each gets its guaranteed slot, so capacity is 1024 +
///     num_senders = 3072 — ample for the load).
///   * The compio io_uring SQ accommodates the sustained write rate
///     without returning EAGAIN/EINVAL under this pattern.
#[test]
fn writer_task_handles_2048_concurrent_vectored() {
    use futures::future::join_all;

    let addr = start_echo_server();
    rt().block_on(async move {
        let client = RpcClient::connect(addr).await.unwrap();
        const N: usize = 2048;

        let mut futs = Vec::with_capacity(N);
        for i in 0u32..N as u32 {
            let c = client.clone();
            futs.push(compio::runtime::spawn(async move {
                // 2-iov vectored payload (header + 1 part) — matches the
                // StreamClient launch_append shape.
                let part = Bytes::from(i.to_le_bytes().to_vec());
                let resp = c.call_vectored(1, vec![part.clone()]).await;
                match resp {
                    Ok(bytes) => {
                        assert_eq!(bytes, part, "req {i} got wrong echo payload");
                        Ok(i)
                    }
                    Err(e) => Err(format!("req {i} failed: {e}")),
                }
            }));
        }

        let results: Vec<Result<u32, String>> = join_all(futs)
            .await
            .into_iter()
            .map(|h| h.unwrap())
            .collect();

        let errors: Vec<_> = results.iter().filter_map(|r| r.as_ref().err()).collect();
        assert!(
            errors.is_empty(),
            "{} of {} requests failed; first error: {:?}",
            errors.len(),
            N,
            errors.first()
        );
        assert_eq!(results.len(), N);
    });
}
