# Tailscale API Surface Analysis for Truffle

**Date:** 2026-03-18
**Tailscale SDK version:** `tailscale.com v1.94.1`
**Truffle sidecar:** `sidecar-slim` (Go shim with stdin/stdout JSON IPC)

---

## Part 1: Full Tailscale/tsnet API Surface

### 1.1 tsnet.Server — Struct Fields

| Field | Type | Description |
|-------|------|-------------|
| `Dir` | `string` | State directory (default: `os.UserConfigDir`) |
| `Store` | `ipn.StateStore` | Custom state store (default: `FileStore` at `Dir/tailscaled.state`) |
| `Hostname` | `string` | Hostname presented to control server (default: binary name) |
| `UserLogf` | `logger.Logf` | Logger for user-facing messages (default: `log.Printf`) |
| `Logf` | `logger.Logf` | Logger for backend debug logs (default: discarded) |
| `Ephemeral` | `bool` | Register as ephemeral node (auto-removed on disconnect) |
| `AuthKey` | `string` | Auth key for node creation (preferred over `TS_AUTHKEY` env) |
| `ClientSecret` | `string` | OAuth client secret for workload identity federation |
| `ClientID` | `string` | Client ID for workload identity federation |
| `IDToken` | `string` | ID token for workload identity |
| `Audience` | `string` | Audience for ID token |
| `ControlURL` | `string` | Coordination server URL (default: Tailscale's servers) |
| `RunWebClient` | `bool` | Run embedded web client on port 5252 |
| `Port` | `uint16` | UDP port for WireGuard (0 = auto-select) |
| `AdvertiseTags` | `[]string` | Tags to advertise for ACL enforcement |
| `Tun` | `tun.Device` | Custom TUN device (set before Start) |

### 1.2 tsnet.Server — Methods

#### Lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `Start` | `() error` | Connect to tailnet. Called implicitly by listener/dial methods if not called. |
| `Up` | `(ctx context.Context) (*ipnstate.Status, error)` | Start and block until running. Returns status with IPs. More convenient than `Start()` + poll. |
| `Close` | `() error` | Shut down the server. Must not be called concurrent with `Start`. |

#### Listening

| Method | Signature | Description |
|--------|-----------|-------------|
| `Listen` | `(network, addr string) (net.Listener, error)` | TCP listener on tailnet only. Unencrypted within WireGuard tunnel. |
| `ListenTLS` | `(network, addr string) (net.Listener, error)` | TLS listener with auto-provisioned Let's Encrypt certs via Tailscale. |
| `ListenPacket` | `(network, addr string) (net.PacketConn, error)` | UDP listener on tailnet. Network must be "udp"/"udp4"/"udp6". |
| `ListenFunnel` | `(network, addr string, opts ...FunnelOption) (net.Listener, error)` | Public internet listener via Tailscale Funnel. Ports: 443, 8443, 10000. Also listens on local tailnet by default. |
| `ListenService` | `(name string, mode ServiceMode) (*ServiceListener, error)` | Tailscale Service listener. Requires tagged node. Returns `ServiceListener` with FQDN. |

#### Dialing & Clients

| Method | Signature | Description |
|--------|-----------|-------------|
| `Dial` | `(ctx context.Context, network, address string) (net.Conn, error)` | Connect to address on tailnet. |
| `HTTPClient` | `() *http.Client` | HTTP client configured for Tailscale routing. |
| `LocalClient` | `() (*local.Client, error)` | Get LocalClient for local API access (WhoIs, Status, etc.). |
| `Loopback` | `() (addr, proxyCred, localAPICred string, err error)` | Start SOCKS5 proxy + LocalAPI on loopback. Returns credentials. |

#### Information

| Method | Signature | Description |
|--------|-----------|-------------|
| `TailscaleIPs` | `() (ip4, ip6 netip.Addr)` | Get IPv4 and IPv6 addresses directly. |
| `CertDomains` | `() []string` | DNS names/domains this server can get TLS certs for. |
| `GetRootPath` | `() string` | Root path for state and data. |
| `Sys` | `() *tsd.System` | Handle to Tailscale subsystems (unstable). |

#### Logging & Debug

| Method | Signature | Description |
|--------|-----------|-------------|
| `LogtailWriter` | `() io.Writer` | Writer for logs visible only to Tailscale support. |
| `CapturePcap` | `(ctx context.Context, pcapFile string) error` | Save packet capture in pcap format. |

#### Advanced

| Method | Signature | Description |
|--------|-----------|-------------|
| `RegisterFallbackTCPHandler` | `(cb FallbackTCPHandler) func()` | Last-resort handler for TCP flows with no listener. Returns deregister function. |

#### Supporting Types

- **`FunnelOption`** — `FunnelOnly()` (internet-only), `FunnelTLSConfig(*tls.Config)` (custom TLS)
- **`ServiceMode`** — `ServiceModeHTTP` (HTTP/HTTPS), `ServiceModeTCP` (raw TCP with optional TLS termination)
- **`ServiceListener`** — embeds `net.Listener`, adds `FQDN string` field
- **`FallbackTCPHandler`** — `func(src, dst netip.AddrPort) (handler func(net.Conn), intercept bool)`

### 1.3 LocalClient (tailscale.com/client/local) — Full Method Inventory

> **Note:** `tailscale.com/client/tailscale.LocalClient` is now a type alias for `tailscale.com/client/local.Client`. The `client/tailscale` package is deprecated.

#### Status & Identity (Stable APIs)

| Method | Signature | Description |
|--------|-----------|-------------|
| `Status` | `(ctx) (*ipnstate.Status, error)` | Full daemon status including peers. |
| `StatusWithoutPeers` | `(ctx) (*ipnstate.Status, error)` | Lightweight status (no peer enumeration). |
| `WhoIs` | `(ctx, remoteAddr string) (*apitype.WhoIsResponse, error)` | Identify connecting peer by IP or IP:port. Returns user profile + node info. |
| `WhoIsNodeKey` | `(ctx, key.NodePublic) (*apitype.WhoIsResponse, error)` | Identify peer by WireGuard public key. |
| `WhoIsProto` | `(ctx, proto, remoteAddr string) (*apitype.WhoIsResponse, error)` | Protocol-specific peer identification (tcp/udp). |

#### Authentication & Login

| Method | Signature | Description |
|--------|-----------|-------------|
| `StartLoginInteractive` | `(ctx) error` | Start interactive login flow. |
| `Logout` | `(ctx) error` | Log out the current node from tailnet. |
| `IDToken` | `(ctx, audience string) (*tailcfg.TokenResponse, error)` | Get OIDC ID token for audience. |

#### Preferences & Configuration

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetPrefs` | `(ctx) (*ipn.Prefs, error)` | Get current preferences. |
| `EditPrefs` | `(ctx, *ipn.MaskedPrefs) (*ipn.Prefs, error)` | Update specific preference fields. |
| `CheckPrefs` | `(ctx, *ipn.Prefs) error` | Validate preferences without applying. |
| `Start` | `(ctx, ipn.Options) error` | Apply config and start state machine. |
| `ReloadConfig` | `(ctx) (bool, error)` | Reload config file if possible. |

#### Profile Management (Multi-Account)

| Method | Signature | Description |
|--------|-----------|-------------|
| `ProfileStatus` | `(ctx) (current, all, error)` | Get current profile and list of all profiles. |
| `SwitchProfile` | `(ctx, ipn.ProfileID) error` | Switch to different profile/account. |
| `SwitchToEmptyProfile` | `(ctx) error` | Create and switch to new empty profile. |
| `DeleteProfile` | `(ctx, ipn.ProfileID) error` | Remove a profile. |

#### File Transfer (Taildrop)

| Method | Signature | Description |
|--------|-----------|-------------|
| `PushFile` | `(ctx, targetNodeID, size, name, io.Reader) error` | Send file to another node. Size -1 for unknown. |
| `WaitingFiles` | `(ctx) ([]apitype.WaitingFile, error)` | List received files pending acceptance. |
| `AwaitWaitingFiles` | `(ctx, duration) ([]apitype.WaitingFile, error)` | Wait for incoming files. |
| `GetWaitingFile` | `(ctx, baseName) (io.ReadCloser, int64, error)` | Read a specific waiting file. |
| `DeleteWaitingFile` | `(ctx, baseName) error` | Delete a waiting file. |
| `FileTargets` | `(ctx) ([]apitype.FileTarget, error)` | List valid file transfer targets. |

#### Networking & Connectivity

| Method | Signature | Description |
|--------|-----------|-------------|
| `DialTCP` | `(ctx, host string, port uint16) (net.Conn, error)` | Connect via Tailscale (accepts DNS name, FQDN, or IP). |
| `UserDial` | `(ctx, network, host, port) (net.Conn, error)` | Connect via Tailscale for given network. |
| `Ping` | `(ctx, ip, pingType) (*ipnstate.PingResult, error)` | Ping a node. Types: TSMP, ICMP, disco. |
| `PingWithOpts` | `(ctx, ip, pingType, opts) (*ipnstate.PingResult, error)` | Ping with size option. |
| `CheckIPForwarding` | `(ctx) error` | Check subnet router/exit node config. |
| `DisconnectControl` | `(ctx) error` | Disconnect from control plane. |
| `SetUseExitNode` | `(ctx, bool) error` | Toggle exit node usage. |
| `SuggestExitNode` | `(ctx) (suggestion, error)` | Get exit node suggestion. |

#### DNS & Certificates

| Method | Signature | Description |
|--------|-----------|-------------|
| `SetDNS` | `(ctx, name, value string) error` | Set DNS TXT record (for ACME challenges). |
| `QueryDNS` | `(ctx, name, queryType) ([]byte, resolvers, error)` | Execute DNS query. |
| `GetDNSOSConfig` | `(ctx) (*apitype.DNSOSConfig, error)` | Get system DNS configuration. |
| `CertPair` | `(ctx, domain) (certPEM, keyPEM, error)` | Get TLS cert+key for domain (stable). |
| `CertPairWithValidity` | `(ctx, domain, minValidity) (certPEM, keyPEM, error)` | Get cert with minimum validity (stable). |
| `GetCertificate` | `(*tls.ClientHelloInfo) (*tls.Certificate, error)` | For `tls.Config.GetCertificate` (stable). |
| `ExpandSNIName` | `(ctx, name) (fqdn, bool)` | Expand bare name to full TLS cert name. |

#### Serve & Funnel Configuration

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetServeConfig` | `(ctx) (*ipn.ServeConfig, error)` | Get current serve/funnel config. |
| `SetServeConfig` | `(ctx, *ipn.ServeConfig) error` | Set serve/funnel configuration. nil disables. |

#### Taildrive (File Sharing)

| Method | Signature | Description |
|--------|-----------|-------------|
| `DriveSetServerAddr` | `(ctx, addr) error` | Set Taildrive file server address. |
| `DriveShareSet` | `(ctx, *drive.Share) error` | Add/update a Taildrive share. |
| `DriveShareRemove` | `(ctx, name) error` | Remove a Taildrive share. |
| `DriveShareRename` | `(ctx, old, new) error` | Rename a share. |
| `DriveShareList` | `(ctx) ([]*drive.Share, error)` | List shares. |

#### Metrics & Health

| Method | Signature | Description |
|--------|-----------|-------------|
| `DaemonMetrics` | `(ctx) ([]byte, error)` | Prometheus-format daemon metrics. |
| `UserMetrics` | `(ctx) ([]byte, error)` | Prometheus-format user metrics. |
| `IncrementCounter` | `(ctx, name, delta) error` | Increment a counter metric. |
| `IncrementGauge` | `(ctx, name, delta) error` | Increment a gauge metric. |
| `SetGauge` | `(ctx, name, value) error` | Set gauge to absolute value. |

#### Event Bus & Monitoring

| Method | Signature | Description |
|--------|-----------|-------------|
| `WatchIPNBus` | `(ctx, mask) (*IPNBusWatcher, error)` | Subscribe to IPN state changes. Returns watcher with `Next()` method. |
| `StreamBusEvents` | `(ctx) iter.Seq2[DebugEvent, error]` | Iterator over bus events. |

#### Debug & Diagnostics

| Method | Signature | Description |
|--------|-----------|-------------|
| `BugReport` | `(ctx, note) (string, error)` | Generate bug report marker. |
| `TailDaemonLogs` | `(ctx) (io.Reader, error)` | Stream daemon logs. |
| `Goroutines` | `(ctx) ([]byte, error)` | Dump goroutines. |
| `Pprof` | `(ctx, type, seconds) ([]byte, error)` | Profile data. |
| `DebugAction` | `(ctx, action) error` | Invoke debug action ("rebind", "restun", etc.). |
| `DebugDERPRegion` | `(ctx, regionID) (*DebugDERPRegionReport, error)` | DERP region diagnostics. |
| `DebugSetExpireIn` | `(ctx, duration) error` | Force key expiry (debug/testing). |
| `StreamDebugCapture` | `(ctx) (io.ReadCloser, error)` | Stream pcap-format packet capture. |
| `DebugPacketFilterRules` | `(ctx) ([]FilterRule, error)` | Get current packet filter rules (ACLs). |

#### Shutdown

| Method | Signature | Description |
|--------|-----------|-------------|
| `ShutdownTailscaled` | `(ctx) error` | Graceful daemon shutdown. |

#### Network Lock (Tailnet Key Authority)

Full suite of methods for managing tailnet key authority: `NetworkLockStatus`, `NetworkLockInit`, `NetworkLockModify`, `NetworkLockSign`, `NetworkLockLog`, `NetworkLockDisable`, `NetworkLockForceLocalDisable`, etc.

#### DERP Map

| Method | Signature | Description |
|--------|-----------|-------------|
| `CurrentDERPMap` | `(ctx) (*tailcfg.DERPMap, error)` | Get current DERP relay server map. |

### 1.4 ipnstate.Status — Key Fields

The `Status` returned by `lc.Status()` / `lc.StatusWithoutPeers()` contains:

| Field | Type | Description |
|-------|------|-------------|
| `BackendState` | `string` | One of: `"NoState"`, `"NeedsLogin"`, `"NeedsMachineAuth"`, `"Stopped"`, `"Starting"`, `"Running"` |
| `AuthURL` | `string` | URL for interactive login (when `NeedsLogin`) |
| `TailscaleIPs` | `[]netip.Addr` | This node's Tailscale IPs (v4 + v6) |
| `Self` | `*PeerStatus` | This node's own peer status |
| `Peer` | `map[key.NodePublic]*PeerStatus` | All peers on the tailnet |
| `Health` | `[]string` | Health warnings/issues |
| `MagicDNSSuffix` | `string` | MagicDNS suffix (e.g., `tailnet-name.ts.net`) |
| `CurrentTailnet` | `*TailnetStatus` | Tailnet metadata (name, MagicDNSSuffix, MagicDNSEnabled) |
| `CertDomains` | `[]string` | Domains this node can get certs for |
| `ClientVersion` | `*tailcfg.ClientVersion` | Update availability info |

#### PeerStatus — Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `ID` | `tailcfg.StableNodeID` | Stable node ID |
| `PublicKey` | `key.NodePublic` | WireGuard public key |
| `HostName` | `string` | Peer hostname |
| `DNSName` | `string` | Full MagicDNS name (with trailing dot) |
| `OS` | `string` | Operating system |
| `Online` | `bool` | Whether peer is online |
| `TailscaleIPs` | `[]netip.Addr` | Peer's Tailscale IPs |
| `Tags` | `[]string` | ACL tags assigned to this peer |
| `KeyExpiry` | `time.Time` | When this node's key expires |
| `Expired` | `bool` | Whether key has expired |
| `ExitNode` | `bool` | Whether this is our current exit node |
| `ExitNodeOption` | `bool` | Whether this peer offers exit node |
| `ShareeNode` | `bool` | Whether this is a shared-in node |
| `Active` | `bool` | Whether peer has recent traffic |
| `CurAddr` | `string` | Current direct address (empty if relayed) |
| `Relay` | `string` | DERP region being used for relay |
| `RxBytes` | `int64` | Bytes received from peer |
| `TxBytes` | `int64` | Bytes transmitted to peer |
| `LastSeen` | `*time.Time` | When peer was last seen |
| `LastWrite` | `*time.Time` | Last traffic to peer |
| `LastHandshake` | `time.Time` | Last WireGuard handshake |
| `InNetworkMap` | `bool` | Whether peer is in our network map |
| `InMagicSock` | `bool` | Whether peer has a magicsock endpoint |
| `InEngine` | `bool` | Whether peer is in WireGuard engine |
| `Capabilities` | `[]tailcfg.NodeCapability` | Peer capabilities |

### 1.5 Tailscale Control Plane API (HTTPS)

Base URL: `https://api.tailscale.com/api/v2`

#### Device Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/devices` | List all devices |
| GET | `/device/{deviceID}` | Get device details |
| DELETE | `/device/{deviceID}` | Delete/remove a device |
| POST | `/device/{deviceID}/authorized` | Authorize a device |
| POST | `/device/{deviceID}/tags` | Set device tags |
| POST | `/device/{deviceID}/key` | Set key expiry disabled/enabled |
| GET | `/device/{deviceID}/routes` | Get device routes |
| POST | `/device/{deviceID}/routes` | Set device routes |
| POST | `/device/{deviceID}/expire-key` | Expire a device's key immediately |

#### Auth Key Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/keys` | List auth keys |
| POST | `/tailnet/{tailnet}/keys` | Create auth key |
| GET | `/tailnet/{tailnet}/keys/{keyID}` | Get auth key details |
| DELETE | `/tailnet/{tailnet}/keys/{keyID}` | Delete auth key |

Auth key options: `reusable`, `ephemeral`, `preauthorized`, `tags`, `expirySeconds`, `description`.

#### DNS Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/dns/nameservers` | Get DNS nameservers |
| POST | `/tailnet/{tailnet}/dns/nameservers` | Set DNS nameservers |
| GET | `/tailnet/{tailnet}/dns/preferences` | Get DNS preferences |
| POST | `/tailnet/{tailnet}/dns/preferences` | Set DNS preferences (MagicDNS on/off) |
| GET | `/tailnet/{tailnet}/dns/searchpaths` | Get DNS search paths |
| POST | `/tailnet/{tailnet}/dns/searchpaths` | Set DNS search paths |

#### ACL Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/acl` | Get ACL policy |
| POST | `/tailnet/{tailnet}/acl` | Set ACL policy |
| POST | `/tailnet/{tailnet}/acl/validate` | Validate ACL policy without applying |
| GET | `/tailnet/{tailnet}/acl/preview` | Preview ACL changes |

#### Tailnet Settings

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/settings` | Get tailnet settings |
| PATCH | `/tailnet/{tailnet}/settings` | Update tailnet settings |

#### Users

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/users` | List users |
| GET | `/user/{userID}` | Get user details |

#### Webhooks

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tailnet/{tailnet}/webhooks` | List webhooks |
| POST | `/tailnet/{tailnet}/webhooks` | Create webhook |
| DELETE | `/tailnet/{tailnet}/webhooks/{webhookID}` | Delete webhook |

### 1.6 Tailscale Networking Concepts

#### DERP (Designated Encrypted Relay for Packets)
- Relay servers for when direct WireGuard connections cannot be established (NAT traversal fails)
- Traffic is end-to-end encrypted even through DERP relays (WireGuard encryption)
- Tailscale operates ~20+ DERP regions globally
- Clients automatically select the lowest-latency DERP region
- DERP is used as a fallback; direct connections (via NAT hole-punching) are preferred
- `LocalClient.CurrentDERPMap()` returns the map of available DERP servers
- `PeerStatus.Relay` indicates which DERP region is used for a peer connection
- `PeerStatus.CurAddr` being empty means traffic is relayed through DERP

#### MagicDNS
- Automatic DNS resolution within the tailnet
- Each node gets `hostname.tailnet-name.ts.net` format
- Supports HTTPS certificates via Let's Encrypt (provisioned automatically by `ListenTLS`)
- `Status.MagicDNSSuffix` provides the tailnet's DNS suffix
- DNS name always has trailing dot in raw API responses (`Self.DNSName`)

#### WireGuard Encryption
- All traffic is always encrypted with WireGuard
- Each node has a unique WireGuard key pair
- Keys are rotated periodically
- Even DERP-relayed traffic is WireGuard-encrypted
- `ListenTLS` adds an additional TLS layer on top (for HTTPS/browser compatibility)

#### ACLs (Access Control Lists)
- Defined in Tailscale admin console or via API
- Control which nodes can communicate with which
- Based on users, groups, tags, and auto-groups
- Default: allow all within tailnet (can be restricted)
- Tags override user-based ACLs for the device

#### Tags
- Applied to devices for ACL-based access control
- Tagged devices act with the capabilities of the tag, not the user
- `AdvertiseTags` on `tsnet.Server` requests tags at node creation
- Tags are verified/approved by ACL policy
- Useful for service-to-service communication patterns

#### Ephemeral Nodes
- Set `tsnet.Server.Ephemeral = true`
- Auto-removed from tailnet when disconnected
- No persistent state on Tailscale's control plane
- Ideal for short-lived processes/containers

#### Auth Keys
- Pre-authorization tokens for headless/automated node setup
- Types: one-time vs reusable, ephemeral, preauthorized
- Bypass interactive login flow
- Have configurable expiry (default: 90 days)
- Can be tagged (device inherits tags)

### 1.7 Node Key Lifecycle

#### Key Expiry
- By default, node keys expire after 180 days
- Expiry can be disabled per-device via admin console or API
- `PeerStatus.KeyExpiry` — when the key expires
- `PeerStatus.Expired` — whether the key has already expired
- When a key expires, the node loses connectivity to the tailnet
- Re-authentication is required (interactive login or new auth key)
- `DebugSetExpireIn` — force expiry for testing

#### BackendState Values (from `ipnstate.Status.BackendState`)

| State | Meaning |
|-------|---------|
| `"NoState"` | Initial state, never started |
| `"NeedsLogin"` | Node needs authentication (key expired, first run, or logged out) |
| `"NeedsMachineAuth"` | Node authenticated but waiting for admin approval |
| `"Stopped"` | Explicitly stopped |
| `"Starting"` | Starting up, connecting to control |
| `"Running"` | Fully connected and operational |

---

## Part 2: What Truffle Currently Uses

> **Updated 2026-03-19** after RFC 008 implementation (Phase 2: Sidecar Improvements).

### 2.1 Go Sidecar (sidecar-slim/main.go)

#### tsnet.Server Fields Used

| Field | Where | Value |
|-------|-------|-------|
| `Hostname` | `handleStart()` | From `startData.Hostname` |
| `Dir` | `handleStart()` | From `startData.StateDir` |
| `Logf` | `handleStart()` | `log.Printf` |
| `AuthKey` | `handleStart()` | From `startData.AuthKey` (optional) |
| `Ephemeral` | `handleStart()` | From `startData.Ephemeral` (optional) -- **NEW in RFC 008** |
| `AdvertiseTags` | `handleStart()` | From `startData.Tags` (optional) -- **NEW in RFC 008** |

#### tsnet.Server Methods Used

| Method | Where | Purpose |
|--------|-------|---------|
| `Start()` | `handleStart()` | Initialize tsnet node |
| `LocalClient()` | `waitForRunning()`, `listenTLS()`, `listenTCP()`, `handleGetPeers()`, `monitorState()`, incoming connection handlers | Get local API client |
| `ListenTLS("tcp", ":443")` | `listenTLS()` | TLS listener for secure mesh + HTTP connections |
| `Listen("tcp", ":9417")` | `listenTCP()` | Plain TCP listener for unencrypted mesh connections |
| `Dial(ctx, "tcp", addr)` | `handleDial()` | Connect to remote peer |
| `Close()` | `handleStop()` | Shutdown (with explicit listener cleanup) -- **improved in RFC 008** |

#### LocalClient Methods Used

| Method | Where | Purpose |
|--------|-------|---------|
| `StatusWithoutPeers(ctx)` | `waitForRunning()`, `monitorState()` | Poll for BackendState + AuthURL; background state monitoring -- **expanded in RFC 008** |
| `Status(ctx)` | `handleGetPeers()` | Get full peer list with rich info |
| `WhoIs(ctx, remoteAddr)` | `resolvePeerIdentity()` | Identify incoming connections by IP -- **NEW in RFC 008**, replaces O(n) `Status()` calls |

#### Status Fields Consumed

| Field | Where |
|-------|-------|
| `status.BackendState` | `waitForRunning()`, `monitorState()` — check for `"Running"`, `"NeedsLogin"`, `"NeedsMachineAuth"` |
| `status.AuthURL` | `waitForRunning()`, `monitorState()` — send `tsnet:authRequired` event |
| `status.TailscaleIPs[0]` | `waitForRunning()` — report our IP |
| `status.Self.DNSName` | `waitForRunning()` — report our MagicDNS name |
| `status.Self.KeyExpiry` | `monitorState()` — detect key expiry -- **NEW in RFC 008** |
| `status.Health` | `monitorState()` — health warnings -- **NEW in RFC 008** |
| `peer.ID` | `handleGetPeers()` — peer identity |
| `peer.HostName` | `handleGetPeers()` — peer hostname |
| `peer.DNSName` | `handleGetPeers()` — peer MagicDNS name |
| `peer.TailscaleIPs` | `handleGetPeers()` — peer IPs |
| `peer.Online` | `handleGetPeers()` — peer online status |
| `peer.OS` | `handleGetPeers()` — peer OS |
| `peer.CurAddr` | `handleGetPeers()` — direct address (empty = relayed) -- **NEW in RFC 008** |
| `peer.Relay` | `handleGetPeers()` — DERP region name -- **NEW in RFC 008** |
| `peer.LastSeen` | `handleGetPeers()` — last seen timestamp -- **NEW in RFC 008** |
| `peer.KeyExpiry` | `handleGetPeers()` — key expiry timestamp -- **NEW in RFC 008** |
| `peer.Expired` | `handleGetPeers()` — expired flag -- **NEW in RFC 008** |
| `peer.Tags` | `handleGetPeers()` — node tags -- **NEW in RFC 008** |

#### WhoIs Fields Consumed (NEW in RFC 008)

| Field | Where |
|-------|-------|
| `whois.Node.ID` | `resolvePeerIdentity()` — node identity |
| `whois.UserProfile.ID` | `resolvePeerIdentity()` — user identity |
| `whois.UserProfile.LoginName` | `resolvePeerIdentity()` — user login |
| `whois.UserProfile.DisplayName` | `resolvePeerIdentity()` — display name |
| `whois.Node.Addresses` | `resolvePeerIdentity()` — Tailscale IPs |
| `whois.Node.Name` | `resolvePeerIdentity()` — DNS name |
| `whois.Node.Hostinfo.OS()` | `resolvePeerIdentity()` — OS |
| `whois.Node.Tags` | `resolvePeerIdentity()` — node tags |

### 2.2 Rust Side (truffle-core)

#### Commands Sent to Go Shim

| Command | Rust Source | Purpose |
|---------|-------------|---------|
| `tsnet:start` | `shim.rs` `run_child()` | Start tsnet with hostname, stateDir, authKey, bridgePort, sessionToken, ephemeral, tags |
| `tsnet:stop` | `shim.rs` `stop()` | Graceful shutdown |
| `tsnet:getPeers` | `shim.rs` `get_peers()` | Request peer list (with rich info) |
| `bridge:dial` | `shim.rs` `dial_raw()` | Dial remote peer with requestId, target, port |

#### Events Handled from Go Shim

| Event | Rust Handler | Action |
|-------|-------------|--------|
| `tsnet:started` | `ShimLifecycleEvent::Started` | Notify application layer |
| `tsnet:stopped` | `ShimLifecycleEvent::Stopped` | Notify application layer |
| `tsnet:status` | `ShimLifecycleEvent::Status(data)` | Forward status updates |
| `tsnet:authRequired` | `ShimLifecycleEvent::AuthRequired` | Forward auth URL for user login |
| `tsnet:peers` | `ShimLifecycleEvent::Peers(data)` | Forward peer list (with rich peer info) |
| `tsnet:stateChange` | `ShimLifecycleEvent::StateChanged` | Backend state transitions -- **NEW in RFC 008** |
| `tsnet:needsApproval` | `ShimLifecycleEvent::NeedsApproval` | NeedsMachineAuth state -- **NEW in RFC 008** |
| `tsnet:keyExpiring` | `ShimLifecycleEvent::KeyExpiring` | Key expiry warning -- **NEW in RFC 008** |
| `tsnet:healthWarning` | `ShimLifecycleEvent::HealthWarning` | Health warnings from Tailscale -- **NEW in RFC 008** |
| `bridge:dialResult` | `ShimLifecycleEvent::DialFailed` (on failure) | Report dial failures; success handled via bridge TCP |
| `tsnet:error` | Logged via `tracing::error!` | Log errors |

### 2.3 Original Sidecar (p008-claude-on-the-go) — Features It Had

| Feature | Present in Original | Present in Truffle Slim |
|---------|-------------------|----------------------|
| tsnet Start/Stop | Yes | Yes |
| Auth URL forwarding | Yes | Yes |
| Status polling | Yes | Yes |
| Peer listing (with hostname prefix filter) | Yes (`GetPeersFiltered`) | Yes (unfiltered) |
| TLS listener (:443) | Yes | Yes |
| Plain TCP listener | No | Yes (:9417) |
| Dial (outgoing connections) | Yes (WebSocket-based) | Yes (raw TCP bridge) |
| WebSocket connection management | Yes (gorilla/websocket) | No (Rust handles WebSocket) |
| Reverse proxy management | Yes (ProxyManager) | No (Rust handles via separate module) |
| PWA serving | Yes (static file server) | No (not needed) |
| Health/status HTTP endpoints | Yes (/health, /status) | No (not needed) |
| VAPID key endpoint | Yes (/vapid) | No (not needed) |
| WebSocket message forwarding | Yes (IPC relay) | No (not needed) |
| Signal handling (SIGINT/SIGTERM) | Yes | No (stdin EOF = shutdown) |
| Connection ID tracking | Yes (per-WebSocket) | No (bridge header has request ID) |
| GetDNSName helper | Yes | No (done via StatusWithoutPeers) |

---

## Part 3: Gap Analysis

### 3.1 MUST HAVE — Will Cause Bugs If Not Handled

#### 1. Key Expiry Detection and Handling
**Gap:** We poll `StatusWithoutPeers` only during startup (in `waitForRunning`). After the node is running, we never check again. If a node's key expires while running, we lose connectivity silently.

**What Tailscale provides:**
- `PeerStatus.KeyExpiry` — exact expiry time
- `PeerStatus.Expired` — boolean flag
- `BackendState` transitions to `"NeedsLogin"` on key expiry
- `WatchIPNBus()` — subscribe to state change notifications

**Recommendation:** After reaching `Running` state, either:
- (a) Start a periodic status check (every 5 minutes) to detect `BackendState` changes, OR
- (b) Use `WatchIPNBus()` to get push notifications of state changes (preferred)

#### 2. Re-authentication After Key Expiry
**Gap:** If the node's key expires mid-session, the shim has no mechanism to detect this and emit a new `tsnet:authRequired` event. The Rust side would see connections fail but wouldn't know why.

**Recommendation:** Add a background goroutine that monitors `BackendState`. When it transitions from `"Running"` to `"NeedsLogin"`, emit `tsnet:authRequired` with the new `AuthURL`.

#### 3. NeedsMachineAuth State
**Gap:** We only check for `BackendState == "Running"` and `AuthURL != ""`. We never handle `"NeedsMachineAuth"`, which occurs when the admin has enabled device approval. The node would appear stuck in a "starting" state forever.

**Recommendation:** Add handling for `"NeedsMachineAuth"` state — emit a new event type (e.g., `tsnet:needsApproval`) so the UI can inform the user.

#### 4. Graceful Shutdown Race Condition
**Gap:** We use `server.Close()` but never `server.Up()` which would give us a cleaner start-and-wait. More critically, there is no graceful listener shutdown — `listenTLS` and `listenTCP` goroutines only check `s.ctx.Err()` in their accept loops. The listeners are not explicitly closed before `server.Close()`, which could leave connections hanging.

**Recommendation:** Track listeners and close them explicitly in `handleStop()` before calling `server.Close()`. The original sidecar's `Node.Stop()` did this correctly.

#### 5. resolvePeerDNS Calls Status() on Every Connection
**Gap:** `resolvePeerDNS()` calls `lc.Status(ctx)` (with full peer enumeration) for every incoming connection. This is expensive — it fetches the entire peer map each time.

**What Tailscale provides:**
- `WhoIs(ctx, remoteAddr)` — purpose-built for exactly this: identify a connecting peer by their address. Returns user profile + node info in a single lightweight call.

**Recommendation:** Replace `resolvePeerDNS()` with `lc.WhoIs(ctx, remoteAddr)`. This is a critical performance fix.

### 3.2 SHOULD HAVE — Significant Product Improvement

#### 6. WhoIs for Peer Authentication
**Gap:** We identify peers only by their IP address (via full `Status()` call). We have no way to verify the identity (user, capabilities, tags) of a connecting peer.

**What Tailscale provides:**
- `WhoIs()` returns `WhoIsResponse` with:
  - `Node` — full node info (tags, capabilities, key)
  - `UserProfile` — user who owns the device
  - `CapMap` — capability map for authorization decisions

**Recommendation:** Use `WhoIs()` on incoming connections to:
- Verify peer identity before accepting bridge connections
- Include user/node info in bridge headers for Rust-side authorization
- Reject connections from unauthorized nodes

#### 7. Health Monitoring
**Gap:** We have no health checks. If the tailnet connection degrades (DERP relay issues, DNS resolution failures, etc.), we don't know about it.

**What Tailscale provides:**
- `Status().Health` — array of health warning strings
- `DaemonMetrics()` — Prometheus-format metrics (connection stats, packet counts, etc.)
- `Ping()` — active probing of specific peers

**Recommendation:** Periodically emit health status events. At minimum, include `Status().Health` warnings in status updates.

#### 8. Connection Quality Information
**Gap:** When reporting peers, we only report hostname, DNS name, IPs, online status, and OS. We miss critical networking information.

**What Tailscale provides (via PeerStatus):**
- `CurAddr` — current direct address (empty = relayed via DERP)
- `Relay` — DERP region if relayed
- `RxBytes`/`TxBytes` — traffic statistics
- `LastSeen`/`LastWrite`/`LastHandshake` — timing info
- `Active` — recent traffic indicator

**Recommendation:** Include connection quality info in peer reports. At minimum: whether the connection is direct or relayed, which DERP region, and last seen time.

#### 9. Ephemeral Node Support
**Gap:** We never set `Ephemeral = true` on the `tsnet.Server`. All truffle nodes are persistent, which means:
- They accumulate in the Tailscale admin console
- They require manual cleanup if abandoned
- Key expiry must be managed

**Recommendation:** Make `Ephemeral` configurable via the start command. For development/testing scenarios, ephemeral nodes auto-cleanup on disconnect.

#### 10. Up() Instead of Start() + Polling
**Gap:** We use `Start()` then poll `StatusWithoutPeers()` in a busy-wait loop with 500ms sleep. This is fragile and adds latency.

**What Tailscale provides:**
- `Up(ctx)` — blocks until running, returns status directly. Single call replaces Start() + polling loop.

**Recommendation:** Replace the `Start()` + polling pattern with `Up(ctx)`. However, note that `Up()` blocks, so we need to handle the `AuthURL` detection differently (perhaps via `WatchIPNBus` concurrently).

#### 11. TailscaleIPs() Direct Access
**Gap:** We extract IPs from `StatusWithoutPeers().TailscaleIPs`. There is a simpler method.

**What Tailscale provides:**
- `Server.TailscaleIPs()` returns `(ip4, ip6 netip.Addr)` directly

**Recommendation:** Use `TailscaleIPs()` after startup for a simpler, cheaper IP retrieval.

#### 12. Tags / AdvertiseTags for ACL Enforcement
**Gap:** Truffle nodes have no tags. All access control is implicit (anyone on the tailnet can connect).

**What Tailscale provides:**
- `Server.AdvertiseTags` — request tags at node creation
- Tags enable ACL-based access control
- Tags can restrict which nodes can connect to which

**Recommendation:** Add tag support. For example, `tag:truffle-node` could be used to restrict mesh connectivity to only truffle nodes, preventing other tailnet devices from connecting.

#### 13. Peer Key Expiry Reporting
**Gap:** When listing peers, we don't report `KeyExpiry` or `Expired`. A peer might appear "online" but actually have an expired key and be unreachable.

**Recommendation:** Include `KeyExpiry` and `Expired` in peer info sent to Rust.

### 3.3 NICE TO HAVE — Advanced Features for Later

#### 14. ListenFunnel — Public Internet Exposure

Implemented in sidecar protocol 15. Truffle passes `FunnelOnly()` and a TLS
configuration offering HTTP/2 and HTTP/1.1. TLS terminates at the node. The
sidecar checks the node's capabilities and granted ports, uses
`QueryFeature("funnel")` for approval instructions, and reads the public
visitor's address from `ipn.FunnelConn.Src`. See RFC 033.

#### 15. Taildrop File Transfer
**What it does:** `PushFile()` / `WaitingFiles()` / `GetWaitingFile()` — native file transfer between Tailscale nodes.

**Use case:** Truffle already has its own file transfer system. But Taildrop could be used for out-of-band file drops when the mesh protocol isn't active.

#### 16. Taildrive File Sharing
**What it does:** Mount remote directories as network drives across the tailnet.

**Use case:** Share working directories between truffle nodes. Could complement the clipboard/file sync that cheeseboard will provide.

#### 17. Ping for Connection Diagnostics
**What it does:** `Ping(ip, type)` — active probing (TSMP, ICMP, or disco).

**Use case:** Built-in connectivity diagnostics. Help users debug "why can't I connect to my other machine?"

#### 18. Loopback() for External Tool Integration
**What it does:** Starts a SOCKS5 proxy + LocalAPI on a loopback address. External tools can route traffic through Tailscale.

**Use case:** Let other applications on the machine route traffic through the truffle node's tailnet connection.

#### 19. HTTPClient() for Tailnet-Aware HTTP
**What it does:** Returns an `*http.Client` that routes through the tailnet.

**Use case:** Making HTTP requests to other tailnet nodes without manual Dial + TLS setup.

#### 20. ListenPacket for UDP
**What it does:** UDP listener on the tailnet.

**Use case:** Low-latency data channels, real-time sync, or game state replication.

#### 21. ListenService for Tailscale Services
**What it does:** Register as a Tailscale Service (like a load-balanced endpoint).

**Use case:** Multiple truffle nodes could register as a single service, with Tailscale handling discovery and routing.

#### 22. CapturePcap for Debugging
**What it does:** Save packet capture for analysis.

**Use case:** Debug connectivity issues at the packet level.

#### 23. Profile Management (Multi-Account)
**What it does:** `ProfileStatus()`, `SwitchProfile()`, `DeleteProfile()` — manage multiple Tailscale accounts.

**Use case:** Users with personal + work tailnets could switch between them.

#### 24. WatchIPNBus for Real-Time State Updates
**What it does:** Subscribe to IPN bus for push notifications of state changes.

**Use case:** Replace polling with push-based monitoring. Detect key expiry, network changes, peer joins/leaves in real time.

#### 25. Control Plane API Integration
**What it does:** Programmatic access to `https://api.tailscale.com/api/v2`.

**Use case:**
- Auto-create auth keys for easy onboarding
- Manage device lifecycle (delete stale nodes)
- Configure ACLs programmatically
- Set up DNS entries

---

## Part 4: Lifecycle State Machine

### 4.1 Complete tsnet Node State Machine

```
                          ┌──────────────────────────────────────────────────────────┐
                          │                                                          │
                          v                                                          │
    ┌──────────────┐    Start()    ┌──────────────┐                                  │
    │              │──────────────>│              │                                  │
    │ Uninitialized│               │   Starting   │                                  │
    │              │               │              │                                  │
    └──────────────┘               └──────┬───────┘                                  │
                                          │                                          │
                              ┌───────────┼───────────┐                              │
                              │           │           │                              │
                              v           v           v                              │
                    ┌──────────────┐ ┌──────────┐ ┌───────────────────┐              │
                    │              │ │          │ │                   │              │
                    │  NeedsLogin  │ │  Error   │ │ NeedsMachineAuth  │              │
                    │              │ │          │ │                   │              │
                    └──────┬───────┘ └──────────┘ └────────┬──────────┘              │
                           │                               │                         │
                    User authenticates              Admin approves                   │
                           │                               │                         │
                           v                               v                         │
                    ┌──────────────┐                                                 │
                    │              │                                                 │
                    │   Running    │<────────────────────────────────────┐            │
                    │              │                                     │            │
                    └──────┬───────┘                                     │            │
                           │                                            │            │
              ┌────────────┼────────────┐                               │            │
              │            │            │                               │            │
              v            v            v                               │            │
      ┌────────────┐ ┌──────────┐ ┌──────────────┐              ┌──────┴───────┐     │
      │            │ │          │ │              │              │              │     │
      │ KeyExpired │ │ Stopping │ │ NetworkLost  │──reconnect──>│  Reconnecting│     │
      │            │ │          │ │              │              │              │     │
      └──────┬─────┘ └────┬─────┘ └──────────────┘              └──────────────┘     │
             │             │                                                          │
             │             v                                                          │
             │      ┌──────────────┐                                                 │
             │      │              │                                                 │
             │      │   Stopped    │─────────────── Restart ─────────────────────────┘
             │      │              │
             │      └──────────────┘
             │
             └──────> NeedsLogin (re-auth required)
```

### 4.2 State Transitions

| From | To | Trigger | API Signal |
|------|----|---------|------------|
| Uninitialized | Starting | `Start()` or `Up()` called | — |
| Starting | Running | Control plane connected, key valid | `BackendState == "Running"` |
| Starting | NeedsLogin | First run or key expired | `BackendState == "NeedsLogin"`, `AuthURL != ""` |
| Starting | NeedsMachineAuth | Admin approval required | `BackendState == "NeedsMachineAuth"` |
| Starting | Error | Network unreachable, bad config | `Start()` returns error |
| NeedsLogin | Running | User completes auth flow | `BackendState` changes to `"Running"` |
| NeedsMachineAuth | Running | Admin approves device | `BackendState` changes to `"Running"` |
| Running | NeedsLogin | Key expires | `BackendState` changes to `"NeedsLogin"` |
| Running | Stopped | `Close()` called | — |
| Running | NetworkLost | Network interface down, DERP unreachable | Health warnings, connection failures |
| NetworkLost | Running | Network restored, DERP reconnected | `BackendState` returns to `"Running"` |
| Stopped | Starting | New `Start()` call | — |
| KeyExpired | NeedsLogin | Automatic transition | `BackendState == "NeedsLogin"` |

### 4.3 Events Truffle Currently Emits

| State Transition | Event Emitted | Data |
|-----------------|---------------|------|
| -> Starting | `tsnet:status` | `{state: "starting", hostname}` |
| Starting -> NeedsLogin | `tsnet:authRequired` | `{authUrl}` |
| Starting -> Running | `tsnet:status` + `tsnet:started` | `{state: "running", hostname, dnsName, tailscaleIP}` |
| Starting -> Error | `tsnet:status` | `{state: "error", error}` |
| -> Stopped | `tsnet:stopped` | `null` |
| Any -> Error (listen) | `tsnet:error` | `{code: "LISTEN_ERROR", message}` |
| Dial failure | `bridge:dialResult` | `{requestId, success: false, error}` |

### 4.4 Events Truffle is MISSING

| State Transition | Missing Event | Impact |
|-----------------|---------------|--------|
| Running -> NeedsLogin (key expiry) | **No event** | Silent connectivity loss. User sees failed connections with no explanation. |
| Running -> NeedsMachineAuth | **No event** | Impossible state for current code. Would appear stuck. |
| Network quality change (direct -> relayed) | **No event** | Performance degradation invisible to user. |
| Peer goes offline/online | **No event** (must poll) | Stale peer list until explicit `getPeers` request. |
| Peer key expires | **No event** | Peer appears online but is unreachable. |
| Health warnings | **No event** | Tailscale health issues invisible. |
| DERP region change | **No event** | Relay change invisible. |

### 4.5 Recommended New Events

| Event | Data | When to Emit |
|-------|------|-------------|
| `tsnet:stateChange` | `{previousState, newState, authUrl?, error?}` | Any `BackendState` change detected via `WatchIPNBus` or polling |
| `tsnet:needsApproval` | `{}` | `BackendState == "NeedsMachineAuth"` |
| `tsnet:keyExpiring` | `{expiresAt, expiresIn}` | Key expiry within 7 days (proactive warning) |
| `tsnet:keyExpired` | `{}` | Key has expired, node needs re-auth |
| `tsnet:healthWarning` | `{warnings: string[]}` | Health warnings detected |
| `tsnet:peerChanged` | `{peerId, change: "online"/"offline"/"expired"}` | Peer status change (via `WatchIPNBus`) |
| `tsnet:connectionQuality` | `{peerId, isDirect, relay?, latency?}` | Connection path changes for active peers |

---

## Part 5: Recommendations

### Priority 1 — Critical (Fix Before Production Use)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| 1 | **Replace `resolvePeerDNS` with `WhoIs()`** | 30 min | Performance: eliminates full peer list fetch per connection. Correctness: purpose-built API for this. |
| 2 | **Add background state monitor** | 2-3 hrs | Detect key expiry, re-auth needs, state transitions. Emit `tsnet:stateChange`, `tsnet:keyExpired`, `tsnet:authRequired` (re-emit). Use `WatchIPNBus()` or periodic `StatusWithoutPeers()`. |
| 3 | **Handle `NeedsMachineAuth` state** | 30 min | Prevent stuck-in-starting bug for tailnets with device approval enabled. |
| 4 | **Fix listener cleanup in `handleStop()`** | 30 min | Track listeners, close them explicitly before `server.Close()`. Prevents hanging connections on shutdown. |

### Priority 2 — High Value (Next Sprint)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| 5 | **Add `WhoIs()` for peer identity verification** | 1-2 hrs | Security: verify peer identity on incoming connections. Include user profile in bridge headers. |
| 6 | **Rich peer info (connection quality)** | 1 hr | UX: show direct vs relayed, DERP region, last seen, key expiry in peer list. |
| 7 | **Health monitoring** | 1 hr | Reliability: forward `Status().Health` warnings. Emit periodic health events. |
| 8 | **Ephemeral node option** | 30 min | Add `Ephemeral` field to start command. Important for CI/testing/dev. |
| 9 | **Proactive key expiry warning** | 1 hr | UX: warn users 7 days before key expires. Check `Self.KeyExpiry` periodically. |
| 10 | **Consider `Up()` instead of `Start()` + poll** | 1-2 hrs | Cleaner startup path. But requires handling auth URL detection differently. |

### Priority 3 — Medium Value (Future Roadmap)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| 11 | **AdvertiseTags for ACL enforcement** | 2 hrs | Security: restrict mesh to `tag:truffle` nodes only. Requires ACL policy changes. |
| 12 | **Ping diagnostics** | 1 hr | UX: let users test connectivity to specific peers. |
| 13 | **Metrics export** | 2 hrs | Observability: expose Tailscale metrics via `DaemonMetrics()` / `UserMetrics()`. |
| 14 | **ListenFunnel for public access** | Implemented | Protocol 15, RFC 033. |
| 15 | **Taildrop integration** | 4-6 hrs | Feature: native file transfer as fallback/alternative to truffle's file transfer protocol. |

### Priority 4 — Exploratory (If Needed)

| # | Item | Effort | Impact |
|---|------|--------|--------|
| 16 | **ListenService for service registration** | 4-6 hrs | Feature: truffle nodes as discoverable Tailscale Services. |
| 17 | **Control Plane API for auth key generation** | 4-6 hrs | UX: auto-create auth keys for easy onboarding. |
| 18 | **Taildrive for directory sharing** | 6-8 hrs | Feature: mount remote directories across truffle mesh. |
| 19 | **Profile management (multi-account)** | 4-6 hrs | Feature: switch between personal/work tailnets. |
| 20 | **ListenPacket for UDP channels** | 4-6 hrs | Feature: low-latency data channels for real-time sync. |

### Quick Wins (< 1 hour each)

1. **Use `TailscaleIPs()` instead of extracting from status** — simpler, cheaper.
2. **Report `Self.KeyExpiry` in status events** — zero-cost information.
3. **Include `CurAddr` and `Relay` in peer info** — already available in `Status()` response.
4. **Log DERP region for debugging** — `PeerStatus.Relay` field.
5. **Add `Expired` field to peer info** — filter out expired peers from "online" peers.

---

## Appendix A: Original Sidecar Features Not in Truffle (Deliberate Omissions)

These were in the original `p008-claude-on-the-go` sidecar but are **intentionally not** in the slim sidecar because Rust handles them:

- **WebSocket management** — Rust's transport layer handles WebSocket connections
- **Reverse proxy management** — Rust has `reverse_proxy` module
- **PWA serving** — Not needed in truffle architecture
- **HTTP health/status endpoints** — Not needed (status flows via IPC)
- **VAPID key endpoint** — Web Push not used
- **Connection ID tracking** — Bridge header system replaces this
- **Hostname prefix filtering** — Rust side can filter peers

## Appendix B: Deprecated APIs to Watch

- `tailscale.com/client/tailscale.LocalClient` is now a **type alias** for `tailscale.com/client/local.Client`. The `client/tailscale` package is deprecated. Import `tailscale.com/client/local` directly.
- The official control plane client has moved to `tailscale.com/client/tailscale/v2`.

## Appendix C: Key Tailscale Version Notes (v1.94.1)

- `ListenService` — relatively new API for Tailscale Services (requires tagged nodes)
- `ServiceMode` variants: `ServiceModeHTTP` and `ServiceModeTCP`
- `WatchIPNBus` / `StreamBusEvents` — event-driven monitoring
- `DriveShare*` methods — Taildrive file sharing
- Workload identity federation (`ClientID`, `ClientSecret`, `IDToken`, `Audience`)
