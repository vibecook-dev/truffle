//! Name resolution for truffle node addressing.
//!
//! Resolves user-provided names to peer device IDs using the Node's peer list.
//! Priority order:
//! 1. Config aliases
//! 2. Peer device names (exact, case-insensitive)
//! 3. Peer device IDs (exact)
//! 4. IP addresses (match against peer IPs)
//! 5. Fuzzy suggestions if no match

use std::collections::HashMap;

use truffle_core::Peer;

// ==========================================================================
// Types
// ==========================================================================

/// A successfully resolved target.
#[derive(Debug, Clone)]
pub struct ResolvedTarget {
    /// Networking route selector for `Node::send` / dial (RFC 022).
    /// Prefer the Tailscale routing key so it matches message attribution
    /// (`msg.from`); still accepts ULID queries at resolve time.
    pub peer_id: String,
    /// The display name for the node (the peer's human-readable
    /// `device_name`, NOT the Tailscale hostname slug).
    pub display_name: String,
    /// The peer's IP address.
    pub ip: String,
    /// How the name was resolved.
    pub resolved_via: ResolvedVia,
}

/// How a name was resolved.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedVia {
    Alias,
    PeerName,
    PeerId,
    IpAddress,
}

/// Errors that can occur during name resolution.
#[derive(Debug, Clone)]
pub enum ResolveError {
    NotFound {
        input: String,
        suggestion: Option<String>,
    },
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResolveError::NotFound { input, suggestion } => {
                write!(f, "Can't find a node named \"{input}\".")?;
                if let Some(suggest) = suggestion {
                    write!(f, " Did you mean \"{suggest}\"?")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ResolveError {}

// ==========================================================================
// Name resolver
// ==========================================================================

/// Resolves user-provided node names to device IDs using the v2 Peer list.
pub struct NameResolver {
    aliases: HashMap<String, String>,
    peers: Vec<Peer>,
}

impl NameResolver {
    /// Create a new resolver with aliases and the current peer list.
    pub fn new(aliases: HashMap<String, String>, peers: Vec<Peer>) -> Self {
        Self { aliases, peers }
    }

    /// Resolve a user-provided name to a target.
    pub fn resolve(&self, name: &str) -> Result<ResolvedTarget, ResolveError> {
        let name_lower = name.to_lowercase();

        // 1. Check config aliases
        if let Some(target) = self.aliases.get(name) {
            // Alias may resolve to a peer name -- try to find the peer
            for peer in &self.peers {
                if peer_label(peer).eq_ignore_ascii_case(target) {
                    return Ok(ResolvedTarget {
                        peer_id: peer_route_id(peer),
                        display_name: peer_label(peer),
                        ip: peer.ip.to_string(),
                        resolved_via: ResolvedVia::Alias,
                    });
                }
            }
            // If alias doesn't match a peer, return not found
            return Err(ResolveError::NotFound {
                input: name.to_string(),
                suggestion: None,
            });
        }

        // 2. Check peer display / device names (case-insensitive)
        for peer in &self.peers {
            if peer_label(peer).eq_ignore_ascii_case(&name_lower)
                || peer
                    .device_name
                    .as_ref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(&name_lower))
            {
                return Ok(ResolvedTarget {
                    peer_id: peer_route_id(peer),
                    display_name: peer_label(peer),
                    ip: peer.ip.to_string(),
                    resolved_via: ResolvedVia::PeerName,
                });
            }
        }

        // 3. Check peer device IDs (exact match) and tailscale ids
        for peer in &self.peers {
            if peer.device_id.as_deref() == Some(name) || peer.tailscale_id == name {
                return Ok(ResolvedTarget {
                    peer_id: peer_route_id(peer),
                    display_name: peer_label(peer),
                    ip: peer.ip.to_string(),
                    resolved_via: ResolvedVia::PeerId,
                });
            }
        }

        // 4. Check IP addresses
        if let Ok(addr) = name.parse::<std::net::IpAddr>() {
            for peer in &self.peers {
                if peer.ip == addr {
                    return Ok(ResolvedTarget {
                        peer_id: peer_route_id(peer),
                        display_name: peer_label(peer),
                        ip: peer.ip.to_string(),
                        resolved_via: ResolvedVia::IpAddress,
                    });
                }
            }
        }

        // No match found -- try fuzzy suggestions
        let suggestion = self.suggest(name);
        Err(ResolveError::NotFound {
            input: name.to_string(),
            suggestion,
        })
    }

    fn suggest(&self, name: &str) -> Option<String> {
        let name_lower = name.to_lowercase();
        let mut best: Option<(String, usize)> = None;

        let mut candidates: Vec<String> = Vec::new();
        for alias in self.aliases.keys() {
            candidates.push(alias.clone());
        }
        for peer in &self.peers {
            candidates.push(peer_label(peer));
        }

        for candidate in &candidates {
            let dist = edit_distance(&name_lower, &candidate.to_lowercase());
            let threshold = (name.len() / 3).max(2);
            if dist <= threshold {
                if let Some((_, best_dist)) = &best {
                    if dist < *best_dist {
                        best = Some((candidate.clone(), dist));
                    }
                } else {
                    best = Some((candidate.clone(), dist));
                }
            }
        }

        if best.is_none() {
            for candidate in &candidates {
                let cand_lower = candidate.to_lowercase();
                if cand_lower.contains(&name_lower) || name_lower.contains(&cand_lower) {
                    return Some(candidate.clone());
                }
            }
        }

        best.map(|(name, _)| name)
    }
}

fn edit_distance(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let n = a_chars.len();
    let m = b_chars.len();

    if n == 0 {
        return m;
    }
    if m == 0 {
        return n;
    }

    let mut prev: Vec<usize> = (0..=m).collect();
    let mut curr = vec![0; m + 1];

    for i in 1..=n {
        curr[0] = i;
        for j in 1..=m {
            let cost = if a_chars[i - 1] == b_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }

    prev[m]
}

/// UI / alias match label (RFC 022).
fn peer_label(peer: &Peer) -> String {
    peer.device_name
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| peer.display_name.clone())
}

/// String to feed into `Node::send` / dial — Tailscale routing key (RFC 022).
/// Matches `msg.from` / `offer.from_peer` attribution.
fn peer_route_id(peer: &Peer) -> String {
    peer.tailscale_id.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    fn make_peers() -> Vec<Peer> {
        vec![
            Peer {
                device_id: Some("01J4K9M2Z8AB3RNYQPW6H5TC0X".to_string()),
                device_name: Some("Alice's MacBook".to_string()),
                display_name: "Alice's MacBook".to_string(),
                hostname: "truffle-cli-alice-s-macbook".to_string(),
                tailscale_id: "laptop-ts-001".to_string(),
                peer_ref: "laptop-ts-001:1".to_string(),
                generation: 1,
                ip: "100.64.0.3".parse::<IpAddr>().unwrap(),
                online: true,
                ws_connected: false,
                connection_type: "direct".to_string(),
                os: Some("macos".to_string()),
                last_seen: None,
                login_name: Some("alice@example.com".to_string()),
            },
            Peer {
                device_id: Some("01J4K9M2Z8AB3RNYQPW6H5TC0Y".to_string()),
                device_name: Some("Prod Server".to_string()),
                display_name: "Prod Server".to_string(),
                hostname: "truffle-cli-prod-server".to_string(),
                tailscale_id: "server-ts-002".to_string(),
                peer_ref: "server-ts-002:1".to_string(),
                generation: 1,
                ip: "100.64.0.1".parse::<IpAddr>().unwrap(),
                online: true,
                ws_connected: true,
                connection_type: "direct".to_string(),
                os: Some("linux".to_string()),
                last_seen: None,
                login_name: Some("ops@example.com".to_string()),
            },
        ]
    }

    #[test]
    fn test_resolve_peer_name() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());
        let result = resolver.resolve("Alice's MacBook").unwrap();
        // peer_id is the Tailscale routing key used for networking.
        assert_eq!(result.peer_id, "laptop-ts-001");
        assert_eq!(result.display_name, "Alice's MacBook");
        assert_eq!(result.resolved_via, ResolvedVia::PeerName);
    }

    #[test]
    fn test_resolve_peer_name_case_insensitive() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());
        let result = resolver.resolve("alice's macbook").unwrap();
        assert_eq!(result.peer_id, "laptop-ts-001");
    }

    #[test]
    fn test_resolve_peer_id() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());
        // ULID query still resolves; returned route id is Tailscale.
        let result = resolver.resolve("01J4K9M2Z8AB3RNYQPW6H5TC0Y").unwrap();
        assert_eq!(result.display_name, "Prod Server");
        assert_eq!(result.peer_id, "server-ts-002");
        assert_eq!(result.resolved_via, ResolvedVia::PeerId);
    }

    /// Hostname slug alone is not a primary label; tailscale id still
    /// resolves as an advanced routing key (RFC 022).
    #[test]
    fn test_resolver_hostname_slug_not_primary() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());

        // Bare hostname slug (without full truffle- prefix) should NOT resolve.
        let err = resolver.resolve("cli-alice-s-macbook").unwrap_err();
        assert!(matches!(err, ResolveError::NotFound { .. }));

        // Tailscale id resolves via the peer-id path.
        let result = resolver.resolve("laptop-ts-001").unwrap();
        assert_eq!(result.peer_id, "laptop-ts-001");
    }

    #[test]
    fn test_resolve_not_found() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());
        let err = resolver.resolve("nonexistent").unwrap_err();
        match err {
            ResolveError::NotFound { input, .. } => {
                assert_eq!(input, "nonexistent");
            }
        }
    }

    #[test]
    fn test_resolve_fuzzy_suggestion() {
        let resolver = NameResolver::new(HashMap::new(), make_peers());
        let err = resolver.resolve("Prod Servr").unwrap_err();
        match err {
            ResolveError::NotFound { suggestion, .. } => {
                assert_eq!(suggestion, Some("Prod Server".to_string()));
            }
        }
    }

    #[test]
    fn test_resolve_picks_first_match_when_device_names_collide() {
        // RFC §13.3: device_name is NOT required to be unique within an app.
        // Two laptops literally named "Alice's MacBook" can both run the
        // same app and both appear in the peer list. The resolver picks the
        // first match by linear scan order, which matches the order peers
        // are listed by the daemon. Callers that need disambiguation should
        // use device_id directly.
        let mut peers = make_peers();
        // Inject a second peer with the same device_name as the first.
        peers.push(Peer {
            device_id: Some("01J4K9M2Z8AB3RNYQPW6H5TC0Z".to_string()),
            device_name: Some("Alice's MacBook".to_string()), // intentional collision
            display_name: "Alice's MacBook".to_string(),
            hostname: "truffle-cli-alice-s-macbook-2".to_string(),
            tailscale_id: "duplicate-ts-003".to_string(),
            peer_ref: "duplicate-ts-003:1".to_string(),
            generation: 1,
            ip: "100.64.0.5".parse::<IpAddr>().unwrap(),
            online: true,
            ws_connected: false,
            connection_type: "direct".to_string(),
            os: Some("macos".to_string()),
            last_seen: None,
            login_name: Some("alice@example.com".to_string()),
        });
        let resolver = NameResolver::new(HashMap::new(), peers);
        let result = resolver.resolve("Alice's MacBook").unwrap();
        // First match by scan order — routing key of the first peer.
        assert_eq!(result.peer_id, "laptop-ts-001");
    }

    #[test]
    fn test_resolve_disambiguates_collided_names_by_device_id() {
        // When device_names collide, the user can disambiguate by passing
        // the device_id directly.
        let mut peers = make_peers();
        peers.push(Peer {
            device_id: Some("01J4K9M2Z8AB3RNYQPW6H5TC0Z".to_string()),
            device_name: Some("Alice's MacBook".to_string()),
            display_name: "Alice's MacBook".to_string(),
            hostname: "truffle-cli-alice-s-macbook-2".to_string(),
            tailscale_id: "duplicate-ts-003".to_string(),
            peer_ref: "duplicate-ts-003:1".to_string(),
            generation: 1,
            ip: "100.64.0.5".parse::<IpAddr>().unwrap(),
            online: true,
            ws_connected: false,
            connection_type: "direct".to_string(),
            os: Some("macos".to_string()),
            last_seen: None,
            login_name: Some("alice@example.com".to_string()),
        });
        let resolver = NameResolver::new(HashMap::new(), peers);
        let result = resolver.resolve("01J4K9M2Z8AB3RNYQPW6H5TC0Z").unwrap();
        assert_eq!(result.display_name, "Alice's MacBook");
        // Second peer's Tailscale routing key.
        assert_eq!(result.peer_id, "duplicate-ts-003");
        assert_eq!(result.resolved_via, ResolvedVia::PeerId);
    }
}
