//! RFC 026 through the public Node API, real sidecar, and real tailnet.
mod common;

use base64::Engine;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use truffle_core::network::{NetworkError, NodeIdentity};
use truffle_core::transport::{RawIncoming, RawListener};
use truffle_core::{EgressRoute, LoopbackConfig, NodeError};

async fn socks_connect(config: &LoopbackConfig, target: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(&config.address).await.unwrap();
    stream.write_all(&[5, 1, 2]).await.unwrap();
    let mut reply = [0; 2];
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [5, 2], "SOCKS requires password authentication");
    let mut auth = vec![1, 5];
    auth.extend_from_slice(b"tsnet");
    auth.push(config.proxy_credential.len().try_into().unwrap());
    auth.extend_from_slice(config.proxy_credential.as_bytes());
    stream.write_all(&auth).await.unwrap();
    stream.read_exact(&mut reply).await.unwrap();
    assert_eq!(reply, [1, 0], "SOCKS credential accepted");
    let mut connect = vec![5, 1, 0];
    match target.ip() {
        IpAddr::V4(ip) => {
            connect.push(1);
            connect.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            connect.push(4);
            connect.extend_from_slice(&ip.octets());
        }
    }
    connect.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&connect).await.unwrap();
    let mut header = [0; 4];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[..3], &[5, 0, 0], "SOCKS CONNECT succeeded");
    let address_length = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream.read_u8().await.unwrap() as usize,
        other => panic!("unknown SOCKS address kind {other}"),
    };
    stream
        .read_exact(&mut vec![0; address_length + 2])
        .await
        .unwrap();
    stream
}

/// Verify payload integrity and the far end's WhoIs identity through each rung.
async fn round_trip(
    client: &mut TcpStream,
    listener: &mut RawListener,
    caller: &NodeIdentity,
) -> RawIncoming {
    const PAYLOAD: &[u8] = b"RFC026 through the calling node";
    let send = async {
        client.write_all(PAYLOAD).await.unwrap();
        let mut echo = vec![0; PAYLOAD.len()];
        client.read_exact(&mut echo).await.unwrap();
        assert_eq!(echo, PAYLOAD);
    };
    let receive = async {
        let mut incoming = listener.accept().await.expect("remote accepted egress");
        let remote: SocketAddr = incoming.remote_addr.parse().unwrap();
        assert_eq!(Some(remote.ip()), caller.ip);
        let identity = incoming.remote_identity.as_ref().expect("WhoIs identity");
        assert_eq!(
            identity.node_id.as_deref(),
            Some(caller.tailscale_id.as_str())
        );
        let mut bytes = vec![0; PAYLOAD.len()];
        incoming.stream.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, PAYLOAD);
        incoming.stream.write_all(&bytes).await.unwrap();
        incoming
    };
    let (_, incoming) = tokio::join!(send, receive);
    incoming
}

#[tokio::test]
async fn test_node_egress_uses_its_own_tailnet_identity() {
    let Some(authkey) = common::require_authkey("test_node_egress_uses_its_own_tailnet_identity")
    else {
        return;
    };
    common::init_test_tracing();
    let pair = common::make_truffle_pair(&authkey).await;
    timeout(Duration::from_secs(60), async {
        let mut listener = pair.beta.listen_tcp(0).await.unwrap();
        let target = SocketAddr::new(pair.beta.local_info().ip.unwrap(), listener.port);
        let caller = pair.alpha.local_info();
        assert!(!caller.tailscale_id.is_empty());

        let loopback = pair.alpha.loopback().await.unwrap();
        assert!(loopback.address.parse::<SocketAddr>().unwrap().ip().is_loopback());
        assert!(loopback.local_api_credential.is_none());
        let privileged = pair.alpha.loopback_with_local_api().await.unwrap();
        assert_eq!(privileged.address, loopback.address);
        assert_eq!(privileged.proxy_credential, loopback.proxy_credential);
        assert!(privileged.local_api_credential.is_some());
        let mut socks = socks_connect(&loopback, target).await;
        drop(round_trip(&mut socks, &mut listener, &caller).await);
        drop(socks);

        let bound = pair.alpha.egress_add(EgressRoute {
            id: "live-route".into(), target: target.to_string(), login: None,
        }).await.unwrap();
        assert!(bound.listen.parse::<SocketAddr>().unwrap().ip().is_loopback());
        let routes = pair.alpha.egress_list().await.unwrap();
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].id, bound.id);
        assert_eq!(routes[0].target, target.to_string());
        let mut direct = TcpStream::connect(&bound.listen).await.unwrap();
        let remote = round_trip(&mut direct, &mut listener, &caller).await;
        pair.alpha.egress_remove(&bound.id).await.unwrap();
        assert_eq!(direct.read(&mut [0; 1]).await.unwrap(), 0, "removal closes live streams");
        drop(remote);
        assert!(pair.alpha.egress_list().await.unwrap().is_empty());
        let refused = pair.alpha.egress_add(EgressRoute {
            id: "host-fallback".into(), target: "127.0.0.1:80".into(), login: None,
        }).await.unwrap_err();
        assert!(matches!(refused, NodeError::Network(NetworkError::EgressError { code, .. }) if code == "TARGET_NOT_PEER"));

        let proxy = pair.alpha.egress_http_proxy().await.unwrap();
        assert_eq!(pair.alpha.egress_http_proxy().await.unwrap(), proxy);
        assert!(proxy.address.parse::<SocketAddr>().unwrap().ip().is_loopback());
        let auth = base64::engine::general_purpose::STANDARD.encode(format!("truffle:{}", proxy.credential));
        let mut tunnel = TcpStream::connect(&proxy.address).await.unwrap();
        tunnel.write_all(format!("CONNECT {target} HTTP/1.1\r\nHost: {target}\r\nProxy-Authorization: Basic {auth}\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        while !response.ends_with(b"\r\n\r\n") {
            assert!(response.len() < 4096, "bounded CONNECT response headers");
            response.push(tunnel.read_u8().await.unwrap());
        }
        assert!(response.starts_with(b"HTTP/1.1 200 "));
        drop(round_trip(&mut tunnel, &mut listener, &caller).await);
    }).await.expect("egress operations complete within their deadline");
    pair.stop().await;
}
