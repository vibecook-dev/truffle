//! Real tsnet QUIC regression: ephemeral server, concurrent/repeated clients,
//! verified payloads, and release of the relay port when endpoints close.
mod common;
use std::time::Duration;

#[tokio::test]
async fn concurrent_quic_clients_use_distinct_ephemeral_relays() {
    let Some(key) =
        common::require_authkey("concurrent_quic_clients_use_distinct_ephemeral_relays")
    else {
        return;
    };
    let pair = common::make_truffle_pair(&key).await;
    let listener = pair.beta.listen_quic(0).await.unwrap();
    let port = listener.port();
    assert_ne!(port, 0);
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let conn = listener.accept().await.unwrap();
            tasks.spawn(async move {
                let mut stream = conn.accept_stream().await.unwrap().unwrap();
                let payload = stream.read(128).await.unwrap().unwrap();
                stream.write(&payload).await.unwrap();
                stream.finish();
                while conn.accept_stream().await.unwrap().is_some() {}
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
        listener.close();
    });
    let outcome = tokio::time::timeout(Duration::from_secs(45), async {
        for wave in 0..2 {
            let mut clients = tokio::task::JoinSet::new();
            for client in 0..2 {
                let node = pair.alpha.clone();
                let peer = pair.beta_device_id.clone();
                clients.spawn(async move {
                    let conn = node.connect_quic(&peer, port).await.unwrap();
                    let mut stream = conn.open_stream().await.unwrap();
                    let payload = format!("wave-{wave}-client-{client}");
                    stream.write(payload.as_bytes()).await.unwrap();
                    stream.finish();
                    assert_eq!(stream.read(128).await.unwrap().unwrap(), payload.as_bytes());
                    conn.close();
                });
            }
            while let Some(result) = clients.join_next().await {
                result.unwrap();
            }
        }
        server.await.unwrap();
        loop {
            match pair.beta.listen_quic(port).await {
                Ok(listener) => {
                    listener.close();
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await;
    pair.stop().await;
    outcome.expect("QUIC transfer or relay release timed out");
}
