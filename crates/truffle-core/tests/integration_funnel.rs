//! Opt-in public-internet test. The test registers an ephemeral node and serves
//! only generated test data. See docs/TESTING.md for the grant and invocation.
mod common;

use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use truffle_core::proxy::{ProxyConfig, ProxyRoute, ProxyUpdate};
use truffle_core::{FunnelState, NodeBuilder};

async fn public_ip(name: &str) -> Option<Ipv4Addr> {
    let output = Command::new("dig")
        .args(["+short", "+time=3", "+tries=1", "@1.1.1.1", name, "A"])
        .output()
        .await
        .expect("dig is required for public DNS");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|s| s.parse::<Ipv4Addr>().ok())
        .find(|ip| {
            !ip.is_private()
                && !ip.is_loopback()
                && (ip.octets()[0] != 100 || !(64..128).contains(&ip.octets()[1]))
        })
}

fn public_request(name: &str, ip: Ipv4Addr, path: &str) -> Command {
    let mut cmd = Command::new("curl");
    cmd.kill_on_drop(true).args([
        "--silent",
        "--show-error",
        "--noproxy",
        "*",
        "--max-time",
        "20",
        "--http2",
        "--resolve",
        &format!("{name}:443:{ip}"),
        &format!("https://{name}{path}"),
    ]);
    cmd
}

async fn fetch(name: &str, ip: Ipv4Addr, path: &str) -> (String, String) {
    let out = public_request(name, ip, path)
        .args(["--write-out", "\n%{http_code}:%{http_version}"])
        .output()
        .await
        .expect("curl is required");
    let text = String::from_utf8_lossy(&out.stdout);
    let (body, status) = text.rsplit_once('\n').unwrap_or(("", ""));
    (body.to_owned(), status.to_owned())
}

#[tokio::test]
#[ignore = "public DNS and a Funnel-enabled real tailnet; run manually or nightly"]
async fn public_funnel_serves_updates_and_revokes() {
    let Some(key) = common::require_authkey("public_funnel_serves_updates_and_revokes") else {
        return;
    };
    let state = tempfile::tempdir().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    std::fs::write(dir.path().join("check.txt"), &nonce).unwrap();
    let node = NodeBuilder::default()
        .app_id("funnel-test")
        .unwrap()
        .hostname(format!("truffle-pb-{}", &nonce[..8]))
        .unwrap()
        .sidecar_path(common::default_sidecar_path())
        .state_dir(state.path().to_str().unwrap())
        .auth_key(&key)
        .ephemeral(true)
        .build()
        .await
        .unwrap();
    let status = node.funnel().status().await.unwrap();
    assert_eq!(
        status.state,
        FunnelState::Ready,
        "test node needs Funnel permission"
    );
    assert!(status.ports.contains(&443));

    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live:ProxyRoute=serde_json::from_value(serde_json::json!({"prefix":"/live","targetUrl":format!("http://{}",backend.local_addr().unwrap())})).unwrap();
    let (advance, when_updated) = tokio::sync::oneshot::channel();
    let stream_backend = tokio::spawn(async move {
        let (conn, _) = backend.accept().await.unwrap();
        let mut reader = BufReader::new(conn);
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await.unwrap() == 0 || line == "\r\n" {
                break;
            }
        }
        let mut conn = reader.into_inner();
        conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata: first\n\n").await.unwrap();
        when_updated.await.unwrap();
        conn.write_all(b"data: second\n\n").await.unwrap()
    });
    let config = |id: &str, prefix: &str, funnel: bool| -> ProxyConfig {
        serde_json::from_value(
            serde_json::json!({"id":id,"name":id,"listen_port":443,"funnel":funnel,
            "routes":[{"prefix":prefix,"dir":dir.path()}]}),
        )
        .unwrap()
    };
    node.proxy()
        .add(config("private", "/private", false))
        .await
        .unwrap();
    let mut public = config("public", "/shared", true);
    public.routes.push(live.clone());
    let info = node.proxy().add(public).await.unwrap();
    let name = info.url.strip_prefix("https://").unwrap();
    let until = Instant::now() + Duration::from_secs(600);
    let ip = loop {
        if let Some(ip) = public_ip(name).await {
            let (body, status) = fetch(name, ip, "/shared/check.txt").await;
            if status == "200:2" && body == nonce {
                break ip;
            }
        }
        assert!(
            Instant::now() < until,
            "public HTTPS/2 did not become ready"
        );
        tokio::time::sleep(Duration::from_secs(3)).await;
    };
    assert_eq!(fetch(name, ip, "/private/check.txt").await.1, "404:2");
    let mut stream = public_request(name, ip, "/live")
        .arg("--no-buffer")
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(stream.stdout.take().unwrap()).lines();
    assert_eq!(
        lines.next_line().await.unwrap().as_deref(),
        Some("data: first")
    );
    node.proxy()
        .update(
            "public",
            ProxyUpdate {
                routes: vec![live],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    advance.send(()).unwrap();
    assert_eq!(lines.next_line().await.unwrap().as_deref(), Some(""));
    assert_eq!(
        lines.next_line().await.unwrap().as_deref(),
        Some("data: second")
    );
    assert!(stream.wait().await.unwrap().success());
    stream_backend.await.unwrap();
    assert_eq!(fetch(name, ip, "/shared/check.txt").await.1, "404:2");
    node.proxy()
        .update("public", ProxyUpdate::default())
        .await
        .unwrap();
    assert_eq!(fetch(name, ip, "/live").await.1, "404:2");
    node.proxy().remove("public").await.unwrap();
    let (_, after) = fetch(name, ip, "/shared/check.txt").await;
    assert!(
        after.starts_with("000") || after.starts_with('5') || after.is_empty(),
        "public listener still answered: {after}"
    );
    node.proxy().remove("private").await.unwrap();
    node.stop().await;
}
