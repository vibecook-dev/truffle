//! `truffle status` -- show node status and connectivity.

use crate::config::TruffleConfig;
use crate::daemon::client::DaemonClient;
use crate::daemon::protocol::method;
use crate::exit_codes;
use crate::json_output;
use crate::output;

pub async fn run(config: &TruffleConfig, json: bool, _watch: bool) -> Result<(), (i32, String)> {
    let client = DaemonClient::new();
    client
        .ensure_running(config)
        .await
        .map_err(|e| (exit_codes::NOT_ONLINE, e.to_string()))?;

    let result = client
        .request(method::STATUS, serde_json::json!({}))
        .await
        .map_err(|e| (exit_codes::ERROR, e.to_string()))?;

    if json {
        let node_name = result["device_name"]
            .as_str()
            .unwrap_or(&config.node.device_name);
        let mut map = json_output::envelope(node_name);

        // Merge daemon response fields into the envelope
        if let Some(obj) = result.as_object() {
            for (k, v) in obj {
                if k != "device_name" {
                    map.insert(k.clone(), v.clone());
                }
            }
        }

        json_output::print_json(&serde_json::Value::Object(map));
        return Ok(());
    }

    let name = result["device_name"].as_str().unwrap_or("-");
    let status = result["status"].as_str().unwrap_or("offline");
    let ip = result["ip"].as_str().unwrap_or("");
    let dns = result["dns_name"].as_str().unwrap_or("");
    let login = result["login_name"].as_str().unwrap_or("");
    let uptime = result["uptime_secs"].as_u64().unwrap_or(0);
    let peer_count = result["peer_count"].as_u64().unwrap_or(0);

    output::print_title("truffle", name);
    println!();
    println!(
        "  {:<12}{} {}",
        "Status",
        output::status_indicator(status),
        output::status_label(status),
    );

    if !ip.is_empty() {
        println!("  {:<12}{}", "IP", ip);
    }
    if !dns.is_empty() {
        println!("  {:<12}{}", "DNS", output::dim(dns));
    }
    // RFC 025 §3.6: printed only when Layer 3 knows it.
    if !login.is_empty() {
        println!("  {:<12}{}", "Login", login);
    }

    println!("  {:<12}{}", "Uptime", output::format_uptime(uptime));
    println!("  {:<12}{}", "Peers", peer_count);

    // Health info
    if let Some(health) = result.get("health") {
        if let Some(expiry) = health["key_expiry"].as_str() {
            if !expiry.is_empty() {
                println!("  {:<12}{}", "Key expiry", output::dim(expiry));
            }
        }
        if let Some(warnings) = health["warnings"].as_array() {
            for w in warnings {
                if let Some(s) = w.as_str() {
                    output::print_warning(s);
                }
            }
        }
    }

    println!();

    Ok(())
}
