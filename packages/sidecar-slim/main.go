// sidecar-slim is a thin Go shim that wraps tsnet for the Rust truffle-core.
//
// It provides two communication channels:
//   - Command channel: stdin/stdout JSON lines for lifecycle commands
//   - Data bridge: local TCP connections to Rust's bridge port with binary headers
//
// All application logic (WebSocket, mesh, file transfer) lives in Rust.
// This shim only handles tsnet lifecycle and transparent TCP proxying.
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"tailscale.com/client/tailscale"
	"tailscale.com/ipn"
	"tailscale.com/ipn/ipnstate"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

// Bridge header constants (must match Rust truffle-core/src/bridge/header.rs)
const (
	headerMagic   = 0x54524646 // "TRFF"
	headerVersion = 0x01

	// maxRemoteDNSNameLen bounds the RemoteDNSName header field, which carries
	// JSON-encoded peer identity. Must match MAX_REMOTE_DNS_NAME_LEN in
	// crates/truffle-core/src/network/tailscale/header.rs.
	maxRemoteDNSNameLen = 4096

	dirIncoming = 0x01
	dirOutgoing = 0x02

	// idleDeadline reaps bridged/relayed connections that go idle in BOTH
	// directions, so a peer that opens a connection and stops talking cannot
	// pin a goroutine + fds forever.
	idleDeadline = 10 * time.Minute
	// dialTimeout bounds an outbound tsnet dial so a blackholed peer can't hang
	// a dial goroutine until stop.
	dialTimeout = 30 * time.Second
	// whoisTimeout bounds the per-connection WhoIs identity lookup.
	whoisTimeout = 3 * time.Second
	// statusTimeout bounds LocalClient.Status() calls.
	statusTimeout = 10 * time.Second
	// maxCommandBytes caps a single stdin command line. Over-size lines are
	// skipped with an error rather than killing the sidecar.
	maxCommandBytes = 8 * 1024 * 1024
	// maxWaitingFileBytes caps a single received Taildrop file (F8).
	maxWaitingFileBytes = 10 << 30 // 10 GiB
)

// Command/Event JSON types
type command struct {
	Command string          `json:"command"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type event struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data,omitempty"`
}

type startData struct {
	Hostname     string   `json:"hostname"`
	StateDir     string   `json:"stateDir"`
	AuthKey      string   `json:"authKey,omitempty"`
	BridgePort   uint16   `json:"bridgePort"`
	SessionToken string   `json:"sessionToken"` // 32-byte hex
	Ephemeral    bool     `json:"ephemeral,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	// IdleTimeoutSecs overrides the bridged-connection idle-reap deadline.
	// nil or <=0 falls back to idleDeadline (RFC 021 §6.5).
	IdleTimeoutSecs *int `json:"idleTimeoutSecs,omitempty"`
}

type dialData struct {
	RequestID string `json:"requestId"`
	Target    string `json:"target"`
	Port      uint16 `json:"port"`
	// Tls overrides TLS wrapping of the dial. nil keeps the legacy behavior
	// (wrap iff port==443); non-nil is used verbatim (RFC 021 §6.4).
	Tls *bool `json:"tls,omitempty"`
}

type dialResultData struct {
	RequestID string `json:"requestId"`
	Success   bool   `json:"success"`
	Error     string `json:"error,omitempty"`
}

// sidecarProtocolVersion is the control-protocol version this sidecar speaks.
// It is advertised in every status/started event so the core can gate features
// on sidecar capability: v2 = RFC 023 serve engine (path routes, allow-lists,
// tls:false); v3 = tsnet:whois query + node identity headers on proxied
// requests. An older or absent value makes the core treat the sidecar as v1.
// Bump on any addition to the command surface the core must not send blind.
const sidecarProtocolVersion = 3

type statusData struct {
	State       string `json:"state"`
	Hostname    string `json:"hostname,omitempty"`
	DNSName     string `json:"dnsName,omitempty"`
	TailscaleIP string `json:"tailscaleIP,omitempty"`
	NodeID      string `json:"nodeId,omitempty"`
	Error       string `json:"error,omitempty"`
	// ProtocolVersion advertises sidecarProtocolVersion on every emission (no
	// omitempty: a status event must always carry it, and the "running" one is
	// what the core reads to gate v2 features).
	ProtocolVersion int `json:"protocolVersion"`
}

type authRequiredData struct {
	AuthURL string `json:"authUrl"`
}

type peerInfo struct {
	ID           string   `json:"id"`
	Hostname     string   `json:"hostname"`
	DNSName      string   `json:"dnsName"`
	TailscaleIPs []string `json:"tailscaleIPs"`
	Online       bool     `json:"online"`
	OS           string   `json:"os,omitempty"`
	CurAddr      string   `json:"curAddr,omitempty"`
	Relay        string   `json:"relay,omitempty"`
	LastSeen     string   `json:"lastSeen,omitempty"`
	KeyExpiry    string   `json:"keyExpiry,omitempty"`
	Expired      bool     `json:"expired,omitempty"`
}

type peersData struct {
	Peers []peerInfo `json:"peers"`
}

type errorData struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// peerIdentityData is the JSON structure encoded into the bridge header's
// remoteDNS field when WhoIs lookup succeeds. Rust deserializes this to
// extract identity information about the connecting peer.
type peerIdentityData struct {
	DNSName       string `json:"dnsName"`
	LoginName     string `json:"loginName,omitempty"`
	DisplayName   string `json:"displayName,omitempty"`
	ProfilePicURL string `json:"profilePicUrl,omitempty"`
	NodeID        string `json:"nodeId,omitempty"`
}

// listenData is the payload for tsnet:listen commands.
type listenData struct {
	Port uint16 `json:"port"`
	TLS  bool   `json:"tls,omitempty"`
}

// listeningData is the payload for tsnet:listening events.
type listeningData struct {
	Port uint16 `json:"port"`
}

// unlistenData is the payload for tsnet:unlisten commands.
type unlistenData struct {
	Port uint16 `json:"port"`
}

// unlistenedData is the payload for tsnet:unlistened events.
type unlistenedData struct {
	Port uint16 `json:"port"`
}

// pingData is the payload for tsnet:ping commands.
type pingData struct {
	Target    string `json:"target"`
	PingType  string `json:"pingType,omitempty"`  // "TSMP", "Disco", "ICMP" (default: "TSMP")
	RequestID string `json:"requestId,omitempty"` // optional correlation id echoed back
}

// pingResultData is the payload for tsnet:pingResult events.
type pingResultData struct {
	Target    string  `json:"target"`
	LatencyMs float64 `json:"latencyMs"`
	Direct    bool    `json:"direct"`
	Relay     string  `json:"relay,omitempty"`
	PeerAddr  string  `json:"peerAddr,omitempty"`
	Error     string  `json:"error,omitempty"`
	RequestID string  `json:"requestId,omitempty"` // echoes pingData.RequestID (P12)
}

// whoisData is the payload for tsnet:whois commands.
type whoisData struct {
	Addr      string `json:"addr"`                // tailnet IP or ip:port to look up
	RequestID string `json:"requestId,omitempty"` // optional correlation id echoed back
}

// whoisResultData is the payload for tsnet:whoisResult events. A nil Identity
// with an empty Error means the lookup found nothing — the caller is anonymous
// (same "zero identity" semantics as the serve proxy path).
type whoisResultData struct {
	Addr      string            `json:"addr"`
	Identity  *peerIdentityData `json:"identity,omitempty"`
	Error     string            `json:"error,omitempty"`
	RequestID string            `json:"requestId,omitempty"` // echoes whoisData.RequestID
}

// pushFileData is the payload for tsnet:pushFile commands.
type pushFileData struct {
	TargetNodeID string `json:"targetNodeId"`
	FileName     string `json:"fileName"`
	FilePath     string `json:"filePath"`
}

// pushFileResultData is the payload for tsnet:pushFileResult events.
type pushFileResultData struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// waitingFileInfo represents a single waiting file in Taildrop.
type waitingFileInfo struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// waitingFilesResultData is the payload for tsnet:waitingFilesResult events.
type waitingFilesResultData struct {
	Files []waitingFileInfo `json:"files"`
}

// getWaitingFileData is the payload for tsnet:getWaitingFile commands.
type getWaitingFileData struct {
	FileName string `json:"fileName"`
	SavePath string `json:"savePath"`
}

// getWaitingFileResultData is the payload for tsnet:getWaitingFileResult events.
type getWaitingFileResultData struct {
	Success  bool   `json:"success"`
	FileName string `json:"fileName"`
	SavePath string `json:"savePath"`
	Error    string `json:"error,omitempty"`
}

// deleteWaitingFileData is the payload for tsnet:deleteWaitingFile commands.
type deleteWaitingFileData struct {
	FileName string `json:"fileName"`
}

// deleteWaitingFileResultData is the payload for tsnet:deleteWaitingFileResult events.
type deleteWaitingFileResultData struct {
	Success  bool   `json:"success"`
	FileName string `json:"fileName,omitempty"`
	Error    string `json:"error,omitempty"`
}

// stateChangeData is the payload for tsnet:stateChange events.
type stateChangeData struct {
	State string `json:"state"`
}

// watchPeersData is the payload for tsnet:watchPeers commands.
type watchPeersData struct {
	IncludeAll bool `json:"includeAll,omitempty"`
}

// peerChangedData is the payload for tsnet:peerChanged events.
type peerChangedData struct {
	ChangeType string    `json:"changeType"` // "joined", "left", "updated"
	PeerID     string    `json:"peerId"`
	Peer       *peerInfo `json:"peer,omitempty"` // nil for "left" events
}

// keyExpiringData is the payload for tsnet:keyExpiring events.
type keyExpiringData struct {
	ExpiresAt string `json:"expiresAt"`
	ExpiresIn int64  `json:"expiresInSecs"`
}

// healthWarningData is the payload for tsnet:healthWarning events.
type healthWarningData struct {
	Warnings []string `json:"warnings"`
}

// proxyAddData is the payload for proxy:add commands.
type proxyAddData struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ListenPort   uint16 `json:"listenPort"`
	TargetHost   string `json:"targetHost"`
	TargetPort   uint16 `json:"targetPort"`
	TargetScheme string `json:"targetScheme"`

	// RFC 023 engine v2 fields. Absent on v1 cores → v1 behavior exactly.

	// Tls: nil preserves the v1 always-TLS listener; explicit false serves
	// plain HTTP (D5 — WireGuard already encrypts; TLS is for browsers).
	Tls *bool `json:"tls"`
	// AllowNonLoopback permits proxy targets beyond 127.0.0.1/localhost
	// (§9.3 default-deny: a LAN target turns this node into a pivot).
	AllowNonLoopback bool `json:"allowNonLoopback,omitempty"`
	// Allow is the config-level loginName glob gate (§9.7); empty = whole
	// tailnet. Routes may override per-route.
	Allow []string `json:"allow,omitempty"`
	// Routes replaces the single target with path-prefix mounts (§7).
	Routes []proxyRouteData `json:"routes,omitempty"`
}

// proxyRouteData is one path-prefix route of a v2 proxy (RFC 023 §7).
// Exactly one of targetUrl/dir is set. The Rust core validates shapes, but
// the sidecar re-checks — it must not trust the wire.
type proxyRouteData struct {
	Prefix      string   `json:"prefix"`
	TargetURL   string   `json:"targetUrl,omitempty"`
	Dir         string   `json:"dir,omitempty"`
	Fallback    string   `json:"fallback,omitempty"`
	StripPrefix bool     `json:"stripPrefix,omitempty"`
	Allow       []string `json:"allow,omitempty"`
}

// proxyRemoveData is the payload for proxy:remove commands.
type proxyRemoveData struct {
	ID string `json:"id"`
}

// proxyAddedEventData is the payload for proxy:added events.
type proxyAddedEventData struct {
	ID         string `json:"id"`
	ListenPort uint16 `json:"listenPort"`
	URL        string `json:"url"`
}

// proxyRemovedEventData is the payload for proxy:removed events.
type proxyRemovedEventData struct {
	ID string `json:"id"`
}

// proxyErrorEventData is the payload for proxy:error events.
type proxyErrorEventData struct {
	ID      string `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// proxyInfoData is the payload for each proxy in proxy:list events.
type proxyInfoData struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	ListenPort   uint16 `json:"listenPort"`
	TargetHost   string `json:"targetHost"`
	TargetPort   uint16 `json:"targetPort"`
	TargetScheme string `json:"targetScheme"`
	URL          string `json:"url"`
}

// proxyListEventData is the payload for proxy:list events.
type proxyListEventData struct {
	Proxies []proxyInfoData `json:"proxies"`
}

// halfCloser is implemented by connections that support half-close (CloseWrite).
// *net.TCPConn implements this, but *tls.Conn does not.
type halfCloser interface {
	CloseWrite() error
}

// proxyEntry is the internal state for a running reverse proxy.
type proxyEntry struct {
	id           string
	name         string
	listenPort   uint16
	targetHost   string
	targetPort   uint16
	targetScheme string
	targetURL    *url.URL // first URL target; nil in routes mode (RFC 023)
	tlsOn        bool     // whether the tailnet listener terminates TLS
	listener     net.Listener
	server       *http.Server
	cancel       context.CancelFunc

	// wsConns tracks live hijacked WebSocket connections. http.Server.Shutdown
	// does NOT close hijacked conns, so we close them explicitly on teardown (P1).
	wsMu    sync.Mutex
	wsConns map[net.Conn]struct{}
}

func (e *proxyEntry) addWS(c net.Conn) {
	e.wsMu.Lock()
	if e.wsConns == nil {
		e.wsConns = make(map[net.Conn]struct{})
	}
	e.wsConns[c] = struct{}{}
	e.wsMu.Unlock()
}

func (e *proxyEntry) removeWS(c net.Conn) {
	e.wsMu.Lock()
	delete(e.wsConns, c)
	e.wsMu.Unlock()
}

// shutdown tears the proxy down in the correct order (P5): cancel in-flight
// round-trips first, then graceful HTTP shutdown, then close the listener and
// any hijacked WebSocket conns Shutdown does not touch (P1).
func (e *proxyEntry) shutdown(timeout time.Duration) {
	if e.cancel != nil {
		e.cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	e.server.Shutdown(ctx)
	cancel()
	if e.listener != nil {
		e.listener.Close()
	}
	e.wsMu.Lock()
	for c := range e.wsConns {
		c.Close()
		delete(e.wsConns, c)
	}
	e.wsMu.Unlock()
}

// listenPacketData is the payload for tsnet:listenPacket commands.
type listenPacketData struct {
	Port uint16 `json:"port"`
}

// listeningPacketData is the payload for tsnet:listeningPacket events.
type listeningPacketData struct {
	Port      uint16 `json:"port"`
	LocalPort uint16 `json:"localPort"`
}

// udpRelay manages a tsnet PacketConn <-> local UDP socket relay.
type udpRelay struct {
	port      uint16         // tsnet-bound port
	localPort uint16         // local relay port (127.0.0.1)
	tsnetConn net.PacketConn // tsnet PacketConn
	localConn net.PacketConn // local UDP socket
	cancel    context.CancelFunc
}

// shim is the main application state.
type shim struct {
	// serverMu guards server, dnsName, sessionToken, bridgePort, idleTimeout,
	// ctx, and cancel — all written from the dispatch loop (start/stop) and read
	// from many spawned goroutines.
	serverMu     sync.RWMutex
	server       *tsnet.Server
	dnsName      string // set when status becomes "running"
	sessionToken []byte // 32 bytes
	bridgePort   uint16
	idleTimeout  time.Duration // bridged-conn idle-reap deadline (RFC 021 §6.5)
	ctx          context.Context
	cancel       context.CancelFunc

	writeMu sync.Mutex // protects stdout writes
	writer  *json.Encoder

	listenerMu sync.Mutex     // protects listeners
	listeners  []net.Listener // active listeners (dynamic), closed on stop

	// dynamicListeners tracks listeners created via tsnet:listen, keyed by port.
	dynamicListenerMu sync.Mutex
	dynamicListeners  map[uint16]net.Listener

	// udpRelays tracks active UDP relays created via tsnet:listenPacket, keyed by port.
	udpRelayMu sync.Mutex
	udpRelays  map[uint16]*udpRelay

	// proxies tracks active reverse proxies created via proxy:add, keyed by ID.
	proxyMu sync.Mutex
	proxies map[string]*proxyEntry

	// watchCancel stops a running WatchIPNBus goroutine (guarded by watchMu).
	watchMu     sync.Mutex
	watchCancel context.CancelFunc

	// certWarmed dedupes cert pre-warms per node domain: a TLS listener created
	// via tsnet:listen/proxy:add fires a one-shot CertPair fetch so ACME issuance
	// happens at listen time, not on the first visitor's handshake (RFC 023 §7).
	certWarmMu sync.Mutex
	certWarmed map[string]struct{}

	// identityCache bounds WhoIs lookups on the proxy request path: keep-alive
	// connections re-present the same RemoteAddr for every request, so a short
	// TTL avoids a 3s-budget RPC per request without letting identity go stale
	// past a minute (RFC 023 §7 identity headers).
	identityCacheMu sync.Mutex
	identityCache   map[string]cachedIdentity
}

// cachedIdentity is one identityCache slot.
type cachedIdentity struct {
	identity peerIdentityData
	expires  time.Time
}

// getServer returns the current tsnet server (nil if not started/stopped).
func (s *shim) getServer() *tsnet.Server {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.server
}

func (s *shim) setServer(srv *tsnet.Server) {
	s.serverMu.Lock()
	s.server = srv
	s.serverMu.Unlock()
}

// lifecycleCtx returns the current lifecycle context. Long-lived goroutines
// must capture a ctx at spawn (as a parameter) instead of calling this
// repeatedly, so a restart's re-armed context cannot keep previous-lifecycle
// goroutines alive.
func (s *shim) lifecycleCtx() context.Context {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.ctx
}

// bridgeParams returns the session token and bridge port under lock.
func (s *shim) bridgeParams() ([]byte, uint16) {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.sessionToken, s.bridgePort
}

// armLifecycle records the session token + bridge port for a start and returns
// the lifecycle context long-lived goroutines should capture at spawn. serverMu
// guards these fields so a restart's writes can't race previous-lifecycle
// readers.
func (s *shim) armLifecycle(token []byte, bridgePort uint16) context.Context {
	s.serverMu.Lock()
	defer s.serverMu.Unlock()
	s.sessionToken = token
	s.bridgePort = bridgePort
	// F10: re-arm the root context if a previous tsnet:stop cancelled it, so a
	// restart-in-place doesn't leave every ctx-gated goroutine dead on arrival.
	if s.ctx.Err() != nil {
		s.ctx, s.cancel = context.WithCancel(context.Background())
	}
	return s.ctx
}

func (s *shim) getDNSName() string {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	return s.dnsName
}

func (s *shim) setDNSName(name string) {
	s.serverMu.Lock()
	s.dnsName = name
	s.serverMu.Unlock()
}

// setIdleTimeout records the bridged-connection idle-reap deadline for this
// lifecycle. Guarded by serverMu like the other lifecycle fields so a restart's
// write can't race the bridge goroutines that read it.
func (s *shim) setIdleTimeout(d time.Duration) {
	s.serverMu.Lock()
	s.idleTimeout = d
	s.serverMu.Unlock()
}

// idleTimeoutOrDefault returns the configured idle-reap deadline, falling back
// to idleDeadline when unset.
func (s *shim) idleTimeoutOrDefault() time.Duration {
	s.serverMu.RLock()
	defer s.serverMu.RUnlock()
	if s.idleTimeout > 0 {
		return s.idleTimeout
	}
	return idleDeadline
}

// recoverPanic contains a panic in a spawned goroutine so a single bad
// connection/command can't crash the whole sidecar (dropping the node off the
// mesh). Deferred at the top of every goroutine and bridgeToRust.
func (s *shim) recoverPanic(where string) {
	if r := recover(); r != nil {
		log.Printf("panic recovered in %s: %v", where, r)
	}
}

// debugEnabled is read once at startup from TRUFFLE_DEBUG (any non-empty value).
// When false, per-packet/per-connection debug chatter is suppressed and the
// tsnet backend logger is left unset (tsnet discards it). Genuine errors and
// warnings, plus startup milestones, are logged unconditionally via log.Printf.
var debugEnabled = os.Getenv("TRUFFLE_DEBUG") != ""

// debugf routes verbose per-packet/per-connection tracing to stderr only when
// TRUFFLE_DEBUG is set. Real errors and warnings must call log.Printf directly
// so they stay visible in the default (quiet) mode.
func debugf(format string, args ...any) {
	if debugEnabled {
		log.Printf(format, args...)
	}
}

func main() {
	log.SetOutput(os.Stderr) // all logs go to stderr; stdout is JSON events only

	ctx, cancel := context.WithCancel(context.Background())
	s := &shim{
		writer:           json.NewEncoder(os.Stdout),
		dynamicListeners: make(map[uint16]net.Listener),
		udpRelays:        make(map[uint16]*udpRelay),
		proxies:          make(map[string]*proxyEntry),
		ctx:              ctx,
		cancel:           cancel,
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		line, tooLong, err := readCommandLine(reader, maxCommandBytes)
		if err != nil {
			if err != io.EOF {
				log.Printf("stdin read error: %v", err)
			}
			break
		}
		if tooLong {
			s.sendError("COMMAND_TOO_LARGE", fmt.Sprintf("command exceeded %d bytes; skipped", maxCommandBytes))
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var cmd command
		if err := json.Unmarshal([]byte(line), &cmd); err != nil {
			s.sendError("PARSE_ERROR", fmt.Sprintf("failed to parse command: %v", err))
			continue
		}

		switch cmd.Command {
		case "tsnet:start":
			s.handleStart(cmd.Data)
		case "tsnet:stop":
			s.handleStop()
		case "tsnet:getPeers":
			s.handleGetPeers()
		case "bridge:dial":
			s.handleDial(cmd.Data)
		case "tsnet:listen":
			s.handleListen(cmd.Data)
		case "tsnet:unlisten":
			s.handleUnlisten(cmd.Data)
		case "tsnet:ping":
			s.handlePing(cmd.Data)
		case "tsnet:whois":
			s.handleWhois(cmd.Data)
		case "tsnet:watchPeers":
			s.handleWatchPeers(cmd.Data)
		case "tsnet:listenPacket":
			s.handleListenPacket(cmd.Data)
		case "tsnet:unlistenPacket":
			s.handleUnlistenPacket(cmd.Data)
		case "tsnet:pushFile":
			s.handlePushFile(cmd.Data)
		case "tsnet:waitingFiles":
			s.handleWaitingFiles()
		case "tsnet:getWaitingFile":
			s.handleGetWaitingFile(cmd.Data)
		case "tsnet:deleteWaitingFile":
			s.handleDeleteWaitingFile(cmd.Data)
		case "proxy:add":
			s.handleProxyAdd(cmd.Data)
		case "proxy:remove":
			s.handleProxyRemove(cmd.Data)
		case "proxy:list":
			s.handleProxyList()
		default:
			s.sendError("UNKNOWN_CMD", fmt.Sprintf("unknown command: %s", cmd.Command))
		}
	}

	// Stdin closed — clean shutdown
	s.handleStop()
}

// readCommandLine reads one '\n'-terminated command line, bounding memory usage
// to max bytes. A line longer than max is fully drained (so the stream stays in
// sync) and reported as tooLong, instead of killing the process — the old
// bufio.Scanner 1MB cap turned a single over-size line into a full shutdown.
func readCommandLine(r *bufio.Reader, max int) (line string, tooLong bool, err error) {
	var sb strings.Builder
	for {
		chunk, isPrefix, e := r.ReadLine()
		if e != nil {
			return "", false, e
		}
		if !tooLong && sb.Len()+len(chunk) <= max {
			sb.Write(chunk)
		} else {
			tooLong = true // keep draining to end-of-line without buffering
		}
		if !isPrefix {
			break
		}
	}
	if tooLong {
		return "", true, nil
	}
	return sb.String(), false, nil
}

// resolveIdleTimeout converts the optional idleTimeoutSecs start knob into a
// duration, falling back to idleDeadline when unset or non-positive.
func resolveIdleTimeout(secs *int) time.Duration {
	if secs != nil && *secs > 0 {
		return time.Duration(*secs) * time.Second
	}
	return idleDeadline
}

func (s *shim) handleStart(data json.RawMessage) {
	var d startData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("START_ERROR", fmt.Sprintf("invalid start data: %v", err))
		return
	}

	// G12: reject a double start rather than orphaning the previous server.
	if s.getServer() != nil {
		s.sendError("START_ERROR", "node already started")
		return
	}

	token, err := hex.DecodeString(d.SessionToken)
	if err != nil || len(token) != 32 {
		s.sendError("START_ERROR", "sessionToken must be 64 hex chars (32 bytes)")
		return
	}
	ctx := s.armLifecycle(token, d.BridgePort)
	s.setIdleTimeout(resolveIdleTimeout(d.IdleTimeoutSecs))

	s.sendStatus("starting", d.Hostname, "", "", "")

	srv := &tsnet.Server{
		Hostname:  d.Hostname,
		Dir:       d.StateDir,
		Ephemeral: d.Ephemeral,
	}
	// tsnet's backend Logf is a verbose firehose (magicsock/netcheck/netmap,
	// "fake tun", etc.). Leaving it nil makes tsnet discard those logs; only
	// wire it to stderr under TRUFFLE_DEBUG. UserLogf is intentionally left
	// unset so tsnet still surfaces user-facing messages (the login AuthURL)
	// via its log.Printf default.
	if debugEnabled {
		srv.Logf = log.Printf
	}
	if d.AuthKey != "" {
		srv.AuthKey = d.AuthKey
	}
	if len(d.Tags) > 0 {
		srv.AdvertiseTags = d.Tags
	}

	// M4: only publish the server after a successful Start(), so a failed start
	// doesn't leave a dead server visible to later commands.
	if err := srv.Start(); err != nil {
		s.sendStatus("error", "", "", "", err.Error())
		return
	}
	s.setServer(srv)

	// Wait for running state in background
	go s.waitForRunning(ctx, d.Hostname)
}

func (s *shim) waitForRunning(ctx context.Context, hostname string) {
	defer s.recoverPanic("waitForRunning")

	srv := s.getServer()
	if srv == nil {
		return
	}
	lc, err := srv.LocalClient()
	if err != nil {
		log.Printf("failed to get local client: %v", err)
		s.sendStatus("error", "", "", "", err.Error())
		return
	}

	authURLSent := false
	needsApprovalSent := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		status, err := lc.StatusWithoutPeers(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("status check failed: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if !authURLSent && status.AuthURL != "" {
			s.sendEvent("tsnet:authRequired", authRequiredData{AuthURL: status.AuthURL})
			authURLSent = true
		}

		// G10: emit needsApproval only when entering the state, not every 500ms.
		if status.BackendState == "NeedsMachineAuth" {
			if !needsApprovalSent {
				s.sendEvent("tsnet:needsApproval", nil)
				needsApprovalSent = true
			}
		} else {
			needsApprovalSent = false
		}

		if status.BackendState == "Running" {
			var ip string
			if len(status.TailscaleIPs) > 0 {
				ip = status.TailscaleIPs[0].String()
			}
			dnsName := strings.TrimSuffix(status.Self.DNSName, ".")
			nodeID := string(status.Self.ID)
			s.setDNSName(dnsName)

			s.sendStatus("running", hostname, dnsName, ip, "")
			s.sendEvent("tsnet:started", statusData{
				State:           "running",
				Hostname:        hostname,
				DNSName:         dnsName,
				TailscaleIP:     ip,
				NodeID:          nodeID,
				ProtocolVersion: sidecarProtocolVersion,
			})

			// The sidecar opens no listeners at startup. The Rust session layer
			// starts the :9417 TCP listener dynamically via tsnet:listen (avoids a
			// double-bind with that dynamic listener); serving listeners are
			// created on demand by tsnet:listen/proxy:add. RFC 023 removed the
			// v1-era fossil :443 TLS listener that used to start here.

			// Start background state monitor
			go s.monitorState(ctx, lc)
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// trackListener adds a listener to the tracked set for cleanup on stop.
func (s *shim) trackListener(ln net.Listener) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	s.listeners = append(s.listeners, ln)
}

func (s *shim) handleStop() {
	s.serverMu.RLock()
	cancel := s.cancel
	s.serverMu.RUnlock()
	if cancel != nil {
		cancel()
	}

	// Stop any running peer watcher (F4).
	s.watchMu.Lock()
	if s.watchCancel != nil {
		s.watchCancel()
		s.watchCancel = nil
	}
	s.watchMu.Unlock()

	// Close listeners first so accept loops exit before server teardown
	s.listenerMu.Lock()
	for _, ln := range s.listeners {
		if err := ln.Close(); err != nil {
			log.Printf("listener close error: %v", err)
		}
	}
	s.listeners = nil
	s.listenerMu.Unlock()

	// Close dynamic listeners
	s.dynamicListenerMu.Lock()
	for port, ln := range s.dynamicListeners {
		if err := ln.Close(); err != nil {
			log.Printf("dynamic listener :%d close error: %v", port, err)
		}
	}
	s.dynamicListeners = make(map[uint16]net.Listener)
	s.dynamicListenerMu.Unlock()

	// Close UDP relays
	s.udpRelayMu.Lock()
	for port, relay := range s.udpRelays {
		debugf("closing UDP relay :%d", port)
		relay.cancel()
		relay.tsnetConn.Close()
		relay.localConn.Close()
	}
	s.udpRelays = make(map[uint16]*udpRelay)
	s.udpRelayMu.Unlock()

	// Close proxies — snapshot while holding lock, then shut down without lock
	// to avoid blocking other goroutines during potentially slow Shutdown calls.
	s.proxyMu.Lock()
	proxyEntries := make([]*proxyEntry, 0, len(s.proxies))
	for _, entry := range s.proxies {
		if entry != nil {
			proxyEntries = append(proxyEntries, entry)
		}
	}
	s.proxies = make(map[string]*proxyEntry)
	s.proxyMu.Unlock()

	for _, entry := range proxyEntries {
		debugf("shutting down proxy %s", entry.id)
		entry.shutdown(2 * time.Second)
	}

	if srv := s.getServer(); srv != nil {
		// Explicitly disconnect from the control plane before closing.
		// This makes the control server detect the disconnect immediately
		// and push Online=false to other peers within seconds.
		// We use EditPrefs to set WantRunning=false which tells the control
		// server we're intentionally going offline (vs a crash/network issue).
		lc, lcErr := srv.LocalClient()
		if lcErr == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, _ = lc.EditPrefs(ctx, &ipn.MaskedPrefs{
				Prefs: ipn.Prefs{
					WantRunning: false,
				},
				WantRunningSet: true,
			})
			cancel()
		}
		if err := srv.Close(); err != nil {
			log.Printf("server close error: %v", err)
		}
		s.setServer(nil)
	}
	s.sendEvent("tsnet:stopped", nil)
}

func (s *shim) handleGetPeers() {
	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	// G3: run off the dispatch loop so a slow Status() can't stall every other
	// command (including tsnet:stop). F11: reuse statusPeerToInfo.
	go func() {
		defer s.recoverPanic("handleGetPeers")

		lc, err := srv.LocalClient()
		if err != nil {
			s.sendError("PEERS_ERROR", err.Error())
			return
		}

		ctx, cancel := context.WithTimeout(s.lifecycleCtx(), statusTimeout)
		defer cancel()
		status, err := lc.Status(ctx)
		if err != nil {
			s.sendError("PEERS_ERROR", err.Error())
			return
		}

		peers := make([]peerInfo, 0, len(status.Peer))
		for _, peer := range status.Peer {
			peers = append(peers, statusPeerToInfo(peer))
		}

		s.sendEvent("tsnet:peers", peersData{Peers: peers})
	}()
}

// shouldWrapTLS decides whether an outbound dial is TLS-wrapped. An explicit
// tls flag from the dial command wins in both directions; when absent (nil) the
// dial is never wrapped. RFC 023 (D4) retired the legacy "wrap iff port==443"
// rule alongside the fossil :443 listener, so port no longer influences the
// decision (kept in the signature to mirror the dial's target port).
func shouldWrapTLS(port uint16, tls *bool) bool {
	if tls != nil {
		return *tls
	}
	return false
}

func (s *shim) handleDial(data json.RawMessage) {
	var d dialData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("DIAL_ERROR", fmt.Sprintf("invalid dial data: %v", err))
		return
	}

	go func() {
		defer s.recoverPanic("handleDial")

		srv := s.getServer()
		if srv == nil {
			debugf("[handleDial] rid=%s FAIL: node not running", d.RequestID)
			s.sendEvent("bridge:dialResult", dialResultData{
				RequestID: d.RequestID,
				Success:   false,
				Error:     "node not running",
			})
			return
		}

		// G8: bound the dial so a blackholed peer can't hang this goroutine.
		dialCtx, cancel := context.WithTimeout(s.lifecycleCtx(), dialTimeout)
		defer cancel()

		// Dial via tsnet
		addr := fmt.Sprintf("%s:%d", d.Target, d.Port)
		debugf("[handleDial] rid=%s dialing %s", d.RequestID, addr)
		tsnetConn, err := srv.Dial(dialCtx, "tcp", addr)
		if err != nil {
			debugf("[handleDial] rid=%s DIAL FAILED: %v", d.RequestID, err)
			s.sendEvent("bridge:dialResult", dialResultData{
				RequestID: d.RequestID,
				Success:   false,
				Error:     err.Error(),
			})
			return
		}
		debugf("[handleDial] rid=%s dial succeeded, bridging to Rust", d.RequestID)

		// TLS-wrap when requested (explicit tls flag, or legacy port==443).
		var conn net.Conn = tsnetConn
		if shouldWrapTLS(d.Port, d.Tls) {
			tlsConn := tls.Client(tsnetConn, &tls.Config{
				ServerName: d.Target, // SNI = peer's DNS name
			})
			if err := tlsConn.HandshakeContext(dialCtx); err != nil {
				tsnetConn.Close()
				s.sendEvent("bridge:dialResult", dialResultData{
					RequestID: d.RequestID,
					Success:   false,
					Error:     fmt.Sprintf("TLS handshake failed: %v", err),
				})
				return
			}
			conn = tlsConn
		}

		// Bridge to Rust
		s.bridgeToRust(conn, d.Port, dirOutgoing, d.RequestID, addr, d.Target)
	}()
}

func (s *shim) handleListen(data json.RawMessage) {
	var d listenData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("LISTEN_ERROR", fmt.Sprintf("invalid listen data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	// Check if already listening on this port
	s.dynamicListenerMu.Lock()
	if _, exists := s.dynamicListeners[d.Port]; exists {
		s.dynamicListenerMu.Unlock()
		s.sendError("LISTEN_ERROR", fmt.Sprintf("already listening on port %d", d.Port))
		return
	}
	s.dynamicListenerMu.Unlock()

	go func() {
		defer s.recoverPanic("handleListen")

		ctx := s.lifecycleCtx()

		lc, err := srv.LocalClient()
		if err != nil {
			s.sendError("LISTEN_ERROR", fmt.Sprintf("failed to get local client: %v", err))
			return
		}

		addr := fmt.Sprintf(":%d", d.Port)
		var ln net.Listener

		if d.TLS {
			ln, err = srv.ListenTLS("tcp", addr)
		} else {
			ln, err = srv.Listen("tcp", addr)
		}
		if err != nil {
			s.sendError("LISTEN_ERROR", fmt.Sprintf("Listen :%d: %v", d.Port, err))
			return
		}

		if d.TLS {
			// RFC 023: warm the node cert now so ACME issuance happens at listen
			// time, not inside the first visitor's TLS handshake.
			go s.prewarmCert(ctx, srv)
		}

		// Resolve the actual port (important when d.Port is 0 and the OS
		// assigns an ephemeral port). tsnet's listener may wrap the raw
		// socket, so the *net.TCPAddr assertion can fail — fall back to
		// parsing the address string in that case.
		actualPort := d.Port
		if tcpAddr, ok := ln.Addr().(*net.TCPAddr); ok {
			if tcpAddr.Port > 0 && tcpAddr.Port <= 65535 {
				actualPort = uint16(tcpAddr.Port)
			}
		} else {
			if _, portStr, err := net.SplitHostPort(ln.Addr().String()); err == nil {
				// Bounded parse: ports are always in [1, 65535]; anything
				// outside that is a sidecar bug we'd rather surface as
				// "listen confirmation timed out" on the Rust side.
				if p, err := strconv.ParseUint(portStr, 10, 16); err == nil && p > 0 {
					actualPort = uint16(p)
				}
			}
		}

		// G13: re-check under the lock at insert time to close the check→insert
		// TOCTOU window (the OS bind already rejects duplicate fixed ports).
		s.dynamicListenerMu.Lock()
		if _, exists := s.dynamicListeners[actualPort]; exists {
			s.dynamicListenerMu.Unlock()
			ln.Close()
			s.sendError("LISTEN_ERROR", fmt.Sprintf("already listening on port %d", actualPort))
			return
		}
		s.dynamicListeners[actualPort] = ln
		s.dynamicListenerMu.Unlock()

		// Also track in the main listener list for cleanup on stop
		s.trackListener(ln)

		s.sendEvent("tsnet:listening", listeningData{Port: actualPort})

		proto := "TCP"
		if d.TLS {
			proto = "TLS"
		}
		debugf("listening %s on :%d (dynamic, requested :%d)", proto, actualPort, d.Port)

		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Check if this was a deliberate close (unlisten). Use
				// actualPort — d.Port may be 0 for ephemeral listeners,
				// and dynamicListeners is keyed by the real port.
				s.dynamicListenerMu.Lock()
				_, stillActive := s.dynamicListeners[actualPort]
				s.dynamicListenerMu.Unlock()
				if !stillActive {
					return
				}
				log.Printf("accept :%d error: %v", actualPort, err)
				continue
			}
			// Route using actualPort — the Rust bridge registered its
			// listener channel under the real port, not the requested 0.
			// G7: resolve identity inside the goroutine, off the accept path.
			go func(c net.Conn) {
				peerIdentity := s.resolvePeerIdentity(lc, c.RemoteAddr().String())
				s.bridgeToRust(c, actualPort, dirIncoming, "", c.RemoteAddr().String(), peerIdentity)
			}(conn)
		}
	}()
}

func (s *shim) handleUnlisten(data json.RawMessage) {
	var d unlistenData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("UNLISTEN_ERROR", fmt.Sprintf("invalid unlisten data: %v", err))
		return
	}

	s.dynamicListenerMu.Lock()
	ln, exists := s.dynamicListeners[d.Port]
	if !exists {
		s.dynamicListenerMu.Unlock()
		s.sendError("UNLISTEN_ERROR", fmt.Sprintf("no listener on port %d", d.Port))
		return
	}
	delete(s.dynamicListeners, d.Port)
	s.dynamicListenerMu.Unlock()

	if err := ln.Close(); err != nil {
		log.Printf("close listener :%d error: %v", d.Port, err)
	}

	debugf("stopped listening on :%d (dynamic)", d.Port)
	s.sendEvent("tsnet:unlistened", unlistenedData{Port: d.Port})
}

func (s *shim) handleListenPacket(data json.RawMessage) {
	var d listenPacketData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("invalid listenPacket data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	// Check if already relaying on this port
	s.udpRelayMu.Lock()
	if _, exists := s.udpRelays[d.Port]; exists {
		s.udpRelayMu.Unlock()
		s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("already listening UDP on port %d", d.Port))
		return
	}
	s.udpRelayMu.Unlock()

	go func() {
		defer s.recoverPanic("handleListenPacket")

		ctx := s.lifecycleCtx()

		// Get the tailscale IP for binding
		status, err := srv.LocalClient()
		if err != nil {
			s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("failed to get local client: %v", err))
			return
		}

		st, err := status.StatusWithoutPeers(ctx)
		if err != nil {
			s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("failed to get status: %v", err))
			return
		}

		if len(st.TailscaleIPs) == 0 {
			s.sendError("LISTEN_PACKET_ERROR", "no Tailscale IPs available")
			return
		}

		tsIP := st.TailscaleIPs[0].String()
		listenAddr := fmt.Sprintf("%s:%d", tsIP, d.Port)

		// Bind tsnet PacketConn
		debugf("UDP relay: calling ListenPacket(%q, %q)", "udp", listenAddr)
		tsnetPC, err := srv.ListenPacket("udp", listenAddr)
		if err != nil {
			s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("ListenPacket %s: %v", listenAddr, err))
			return
		}
		debugf("UDP relay: ListenPacket succeeded, local addr = %v", tsnetPC.LocalAddr())

		// Bind local relay UDP socket on ephemeral port
		localPC, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			tsnetPC.Close()
			s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("local UDP bind: %v", err))
			return
		}

		localAddr := localPC.LocalAddr().(*net.UDPAddr)
		localPort := uint16(localAddr.Port)

		relayCtx, relayCancel := context.WithCancel(ctx)

		relay := &udpRelay{
			port:      d.Port,
			localPort: localPort,
			tsnetConn: tsnetPC,
			localConn: localPC,
			cancel:    relayCancel,
		}

		// B4: re-check under the lock before inserting (closes the check→insert
		// TOCTOU; the tsnet ListenPacket bind above also rejects duplicates).
		s.udpRelayMu.Lock()
		if _, exists := s.udpRelays[d.Port]; exists {
			s.udpRelayMu.Unlock()
			relayCancel()
			tsnetPC.Close()
			localPC.Close()
			s.sendError("LISTEN_PACKET_ERROR", fmt.Sprintf("already listening UDP on port %d", d.Port))
			return
		}
		s.udpRelays[d.Port] = relay
		s.udpRelayMu.Unlock()

		s.sendEvent("tsnet:listeningPacket", listeningPacketData{
			Port:      d.Port,
			LocalPort: localPort,
		})

		debugf("UDP relay started: tsnet %s <-> 127.0.0.1:%d", listenAddr, localPort)

		// Self-test: verify the tsnet PacketConn can send to itself.
		// This catches misconfigurations early (wrong address format, etc).
		// Use the actual bound address from LocalAddr so ephemeral port 0 works.
		go func() {
			selfAddr := tsnetPC.LocalAddr()
			testPayload := []byte("truffle-udp-selftest")
			debugf("UDP relay self-test: sending %d bytes to self at %v", len(testPayload), selfAddr)
			nw, werr := tsnetPC.WriteTo(testPayload, selfAddr)
			if werr != nil {
				debugf("UDP relay self-test: WriteTo FAILED: %v", werr)
			} else {
				debugf("UDP relay self-test: WriteTo sent %d bytes to self OK", nw)
			}
		}()

		// We need to track the Rust peer's address so we can relay inbound packets to it.
		// The Rust side "connects" its UDP socket to our local relay, so we learn its
		// address from the first outbound packet it sends.
		var rustAddr net.Addr
		var rustAddrMu sync.Mutex

		// Goroutine: tsnet -> local (inbound datagrams)
		go func() {
			buf := make([]byte, 65536)
			for {
				select {
				case <-relayCtx.Done():
					return
				default:
				}

				n, remoteAddr, err := tsnetPC.ReadFrom(buf)
				if err != nil {
					if relayCtx.Err() != nil {
						return
					}
					log.Printf("UDP relay tsnet read error: %v", err)
					continue
				}

				debugf("UDP relay inbound: %d bytes from %v", n, remoteAddr)

				// Parse remote address to get IP and port for the header
				udpAddr, ok := remoteAddr.(*net.UDPAddr)
				if !ok {
					debugf("UDP relay: unexpected remote addr type: %T", remoteAddr)
					continue
				}

				ip4 := udpAddr.IP.To4()
				if ip4 == nil {
					debugf("UDP relay: non-IPv4 remote addr: %v", udpAddr)
					continue
				}

				// Frame: [4-byte IPv4][2-byte port BE][payload]
				framed := make([]byte, 6+n)
				copy(framed[0:4], ip4)
				binary.BigEndian.PutUint16(framed[4:6], uint16(udpAddr.Port))
				copy(framed[6:], buf[:n])

				rustAddrMu.Lock()
				ra := rustAddr
				rustAddrMu.Unlock()

				if ra == nil {
					debugf("UDP relay: no Rust peer address yet, dropping inbound packet from %v", remoteAddr)
					continue
				}

				debugf("UDP relay inbound: forwarding %d framed bytes to Rust at %v", len(framed), ra)
				if _, err := localPC.WriteTo(framed, ra); err != nil {
					if relayCtx.Err() != nil {
						return
					}
					log.Printf("UDP relay local write error: %v", err)
				}
			}
		}()

		// Main goroutine: local -> tsnet (outbound datagrams)
		buf := make([]byte, 65536)
		for {
			select {
			case <-relayCtx.Done():
				return
			default:
			}

			n, senderAddr, err := localPC.ReadFrom(buf)
			if err != nil {
				if relayCtx.Err() != nil {
					return
				}
				log.Printf("UDP relay local read error: %v", err)
				continue
			}

			isRegister := n >= 20 && string(buf[:20]) == "TRUFFLE_UDP_REGISTER"

			// P11: the loopback relay socket is otherwise unauthenticated, so
			// only (re)learn the trusted Rust peer address from an explicit
			// REGISTER packet, and only forward datagrams that come from it —
			// otherwise another local process could hijack the relay by racing
			// a datagram to the loopback port.
			rustAddrMu.Lock()
			if isRegister {
				if rustAddr == nil {
					debugf("UDP relay: learned Rust peer address: %v", senderAddr)
				}
				rustAddr = senderAddr
			}
			trusted := rustAddr != nil && senderAddr.String() == rustAddr.String()
			rustAddrMu.Unlock()

			if isRegister {
				debugf("UDP relay: registration packet from Rust at %v", senderAddr)
				continue
			}

			if !trusted {
				debugf("UDP relay: dropping datagram from untrusted local sender %v", senderAddr)
				continue
			}

			if n < 6 {
				debugf("UDP relay: outbound packet too short (%d bytes)", n)
				continue
			}

			// Parse header: [4-byte IPv4][2-byte port BE][payload]
			// IMPORTANT: Use a raw 4-byte net.IP slice instead of net.IPv4()
			// which returns a 16-byte IPv4-mapped IPv6 address (::ffff:x.x.x.x).
			// gvisor's gonet.UDPConn.WriteTo passes the IP to tcpip.AddrFromSlice
			// which treats 16-byte IPs as IPv6. Since our tsnet PacketConn is
			// bound as udp4, writing to an IPv6 dest would fail silently or
			// produce a network-unreachable error.
			targetIP := net.IP(append([]byte(nil), buf[0:4]...))
			targetPort := binary.BigEndian.Uint16(buf[4:6])
			payload := buf[6:n]

			targetAddr := &net.UDPAddr{IP: targetIP, Port: int(targetPort)}
			debugf("UDP relay outbound: %d payload bytes -> %v (IP len=%d, raw IP=%x)", len(payload), targetAddr, len(targetIP), []byte(targetIP))
			if _, err := tsnetPC.WriteTo(payload, targetAddr); err != nil {
				if relayCtx.Err() != nil {
					return
				}
				log.Printf("UDP relay tsnet write error to %v: %v", targetAddr, err)
			} else {
				debugf("UDP relay outbound: sent %d bytes to %v OK", len(payload), targetAddr)
			}
		}
	}()
}

func (s *shim) handleUnlistenPacket(data json.RawMessage) {
	var d listenPacketData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("UNLISTEN_PACKET_ERROR", fmt.Sprintf("invalid unlistenPacket data: %v", err))
		return
	}

	s.udpRelayMu.Lock()
	relay, exists := s.udpRelays[d.Port]
	if !exists {
		s.udpRelayMu.Unlock()
		s.sendError("UNLISTEN_PACKET_ERROR", fmt.Sprintf("no UDP relay on port %d", d.Port))
		return
	}
	delete(s.udpRelays, d.Port)
	s.udpRelayMu.Unlock()

	// P9/P10: cancel AND close both conns so the relay goroutines (blocked in
	// ReadFrom, which ctx cancellation alone cannot interrupt) exit promptly.
	relay.cancel()
	relay.tsnetConn.Close()
	relay.localConn.Close()

	debugf("stopped UDP relay on :%d", d.Port)
	s.sendEvent("tsnet:unlistenedPacket", listenPacketData{Port: d.Port})
}

func (s *shim) handlePing(data json.RawMessage) {
	var d pingData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("PING_ERROR", fmt.Sprintf("invalid ping data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handlePing")

		// P12: stamp Target + RequestID on every result so concurrent pings can
		// be correlated by the caller.
		emit := func(r pingResultData) {
			r.Target = d.Target
			r.RequestID = d.RequestID
			s.sendEvent("tsnet:pingResult", r)
		}

		lc, err := srv.LocalClient()
		if err != nil {
			emit(pingResultData{Error: fmt.Sprintf("failed to get local client: %v", err)})
			return
		}

		addr, err := netip.ParseAddr(d.Target)
		if err != nil {
			emit(pingResultData{Error: fmt.Sprintf("failed to parse target IP: %v", err)})
			return
		}

		// Map user-facing ping type to tailcfg.PingType.
		// Valid values: "TSMP" (default), "disco", "ICMP", "peerapi".
		var pt tailcfg.PingType
		switch strings.ToUpper(d.PingType) {
		case "DISCO":
			pt = tailcfg.PingDisco
		case "ICMP":
			pt = tailcfg.PingICMP
		case "PEERAPI":
			pt = tailcfg.PingPeerAPI
		case "TSMP", "":
			pt = tailcfg.PingTSMP
		default:
			emit(pingResultData{Error: fmt.Sprintf("unknown ping type: %s (valid: TSMP, disco, ICMP, peerapi)", d.PingType)})
			return
		}

		ctx, cancel := context.WithTimeout(s.lifecycleCtx(), 10*time.Second)
		defer cancel()

		result, err := lc.Ping(ctx, addr, pt)
		if err != nil {
			emit(pingResultData{Error: fmt.Sprintf("ping failed: %v", err)})
			return
		}

		// If the PingResult contains an error string, report it.
		if result.Err != "" {
			emit(pingResultData{Error: result.Err})
			return
		}

		emit(pingResultData{
			LatencyMs: result.LatencySeconds * 1000.0,
			Direct:    result.Endpoint != "" && result.DERPRegionID == 0,
			Relay:     result.DERPRegionCode,
			PeerAddr:  result.Endpoint,
		})
	}()
}

// handleWhois answers a tsnet:whois query: the tailnet identity of the node
// that owns an address. Reaches ANY tailnet device — not just mesh peers —
// which is what makes it a query API rather than a peers() lookup. Results
// ride the same TTL cache as the proxy path; unlike that path, a failed
// lookup is surfaced on the wire (Error), never folded into "anonymous" —
// callers must be able to tell "no owner" from "the lookup broke".
func (s *shim) handleWhois(data json.RawMessage) {
	var d whoisData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("WHOIS_ERROR", fmt.Sprintf("invalid whois data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handleWhois")

		emit := func(r whoisResultData) {
			r.Addr = d.Addr
			r.RequestID = d.RequestID
			s.sendEvent("tsnet:whoisResult", r)
		}

		lc, err := srv.LocalClient()
		if err != nil {
			emit(whoisResultData{Error: fmt.Sprintf("failed to get local client: %v", err)})
			return
		}

		identity, err := s.cachedWhoisErr(lc, d.Addr)
		if err != nil {
			emit(whoisResultData{Error: fmt.Sprintf("whois lookup failed: %v", err)})
			return
		}
		if identity == (peerIdentityData{}) {
			emit(whoisResultData{}) // anonymous: absent, not fabricated
			return
		}
		emit(whoisResultData{Identity: &identity})
	}()
}

func (s *shim) handleWatchPeers(data json.RawMessage) {
	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	lc, err := srv.LocalClient()
	if err != nil {
		s.sendError("WATCH_PEERS_ERROR", fmt.Sprintf("failed to get local client: %v", err))
		return
	}

	// F4: only one watcher at a time — cancel any previous one and start fresh.
	watchCtx, watchCancel := context.WithCancel(s.lifecycleCtx())
	s.watchMu.Lock()
	if s.watchCancel != nil {
		s.watchCancel()
	}
	s.watchCancel = watchCancel
	s.watchMu.Unlock()

	go func() {
		defer s.recoverPanic("handleWatchPeers")
		defer watchCancel()

		// knownPeers persists across watcher reconnects so a transient error
		// doesn't cause a spurious "joined" storm (F3).
		knownPeers := make(map[string]peerInfo)
		seeded := false

		// diff re-fetches full status and emits joined/left/updated. It also
		// backs the periodic poll (F1): Tailscale's delta path only pushes a
		// NetMap notify for Online/LastSeen, NOT relay/endpoint changes, so a
		// timer-driven re-diff is the safety net that catches peer roaming.
		diff := func() {
			ctx, cancel := context.WithTimeout(watchCtx, statusTimeout)
			defer cancel()
			newStatus, err := lc.Status(ctx)
			if err != nil {
				if watchCtx.Err() == nil {
					log.Printf("WatchIPNBus: status fetch failed: %v", err)
				}
				return
			}
			current := make(map[string]peerInfo, len(newStatus.Peer))
			for _, peer := range newStatus.Peer {
				pi := statusPeerToInfo(peer)
				current[pi.ID] = pi
			}
			if !seeded {
				// First successful fetch is the baseline; don't report every
				// existing peer as "joined".
				knownPeers = current
				seeded = true
				return
			}
			for id, pi := range current {
				if _, ok := knownPeers[id]; !ok {
					piCopy := pi
					s.sendEvent("tsnet:peerChanged", peerChangedData{ChangeType: "joined", PeerID: id, Peer: &piCopy})
				}
			}
			for id := range knownPeers {
				if _, ok := current[id]; !ok {
					s.sendEvent("tsnet:peerChanged", peerChangedData{ChangeType: "left", PeerID: id})
				}
			}
			for id, newPi := range current {
				if oldPi, ok := knownPeers[id]; ok && watchPeerChanged(oldPi, newPi) {
					piCopy := newPi
					s.sendEvent("tsnet:peerChanged", peerChangedData{ChangeType: "updated", PeerID: id, Peer: &piCopy})
				}
			}
			knownPeers = current
		}

		// F1: periodic re-diff as a safety net for missed delta notifies.
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		// F2: (re)establish the watcher, retrying transient errors with backoff
		// instead of dying permanently on the first hiccup.
		backoff := time.Second
		for {
			if watchCtx.Err() != nil {
				return
			}

			watcher, err := lc.WatchIPNBus(watchCtx, ipn.NotifyWatchEngineUpdates)
			if err != nil {
				if watchCtx.Err() != nil {
					return
				}
				log.Printf("WatchIPNBus failed: %v", err)
				s.sendEvent("tsnet:watchPeersError", errorData{Code: "WATCH_PEERS_ERROR", Message: err.Error()})
				select {
				case <-watchCtx.Done():
					return
				case <-time.After(backoff):
				}
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second // reset on a successful (re)connect
			debugf("WatchIPNBus started, listening for peer changes")

			// Seed / re-baseline on (re)connect.
			diff()

			// Pump blocking watcher.Next() into channels so the select below can
			// also service the periodic ticker and context cancellation.
			notifyCh := make(chan struct{}, 1)
			watchErrCh := make(chan error, 1)
			go func() {
				for {
					n, err := watcher.Next()
					if err != nil {
						watchErrCh <- err
						return
					}
					if n.NetMap == nil {
						continue
					}
					select {
					case notifyCh <- struct{}{}:
					default:
					}
				}
			}()

		inner:
			for {
				select {
				case <-watchCtx.Done():
					watcher.Close()
					return
				case <-ticker.C:
					diff()
				case <-notifyCh:
					diff()
				case werr := <-watchErrCh:
					watcher.Close()
					if watchCtx.Err() != nil {
						return
					}
					log.Printf("WatchIPNBus error: %v", werr)
					s.sendEvent("tsnet:watchPeersError", errorData{Code: "WATCH_PEERS_ERROR", Message: werr.Error()})
					break inner // reconnect
				}
			}
		}
	}()
}

// statusPeerToInfo converts an ipnstate.PeerStatus to our peerInfo type.
// This uses the same field access as handleGetPeers for consistency.
func statusPeerToInfo(peer *ipnstate.PeerStatus) peerInfo {
	var ips []string
	for _, ip := range peer.TailscaleIPs {
		ips = append(ips, ip.String())
	}
	p := peerInfo{
		ID:           string(peer.ID),
		Hostname:     peer.HostName,
		DNSName:      strings.TrimSuffix(peer.DNSName, "."),
		TailscaleIPs: ips,
		Online:       peer.Online,
		OS:           peer.OS,
		CurAddr:      peer.CurAddr,
		Relay:        peer.Relay,
		Expired:      peer.Expired,
	}
	if !peer.LastSeen.IsZero() {
		p.LastSeen = peer.LastSeen.UTC().Format(time.RFC3339)
	}
	if peer.KeyExpiry != nil && !peer.KeyExpiry.IsZero() {
		p.KeyExpiry = peer.KeyExpiry.UTC().Format(time.RFC3339)
	}
	return p
}

// watchPeerChanged returns true if any observable property of the peer has changed.
func watchPeerChanged(old, new peerInfo) bool {
	if old.Online != new.Online {
		return true
	}
	if old.CurAddr != new.CurAddr {
		return true
	}
	if old.Relay != new.Relay {
		return true
	}
	if old.Hostname != new.Hostname {
		return true
	}
	if old.DNSName != new.DNSName {
		return true
	}
	if old.Expired != new.Expired {
		return true
	}
	return false
}

func (s *shim) handlePushFile(data json.RawMessage) {
	var d pushFileData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("PUSH_FILE_ERROR", fmt.Sprintf("invalid pushFile data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handlePushFile")

		lc, err := srv.LocalClient()
		if err != nil {
			s.sendEvent("tsnet:pushFileResult", pushFileResultData{
				Success: false,
				Error:   fmt.Sprintf("failed to get local client: %v", err),
			})
			return
		}

		f, err := os.Open(d.FilePath)
		if err != nil {
			s.sendEvent("tsnet:pushFileResult", pushFileResultData{
				Success: false,
				Error:   fmt.Sprintf("failed to open file: %v", err),
			})
			return
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			s.sendEvent("tsnet:pushFileResult", pushFileResultData{
				Success: false,
				Error:   fmt.Sprintf("failed to stat file: %v", err),
			})
			return
		}

		ctx, cancel := context.WithTimeout(s.lifecycleCtx(), 5*time.Minute)
		defer cancel()

		err = lc.PushFile(ctx, tailcfg.StableNodeID(d.TargetNodeID), fi.Size(), d.FileName, f)
		if err != nil {
			s.sendEvent("tsnet:pushFileResult", pushFileResultData{
				Success: false,
				Error:   fmt.Sprintf("push file failed: %v", err),
			})
			return
		}

		s.sendEvent("tsnet:pushFileResult", pushFileResultData{
			Success: true,
		})
	}()
}

func (s *shim) handleWaitingFiles() {
	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handleWaitingFiles")

		lc, err := srv.LocalClient()
		if err != nil {
			s.sendError("WAITING_FILES_ERROR", fmt.Sprintf("failed to get local client: %v", err))
			return
		}

		files, err := lc.WaitingFiles(s.lifecycleCtx())
		if err != nil {
			s.sendError("WAITING_FILES_ERROR", fmt.Sprintf("waiting files failed: %v", err))
			return
		}

		var infos []waitingFileInfo
		for _, f := range files {
			infos = append(infos, waitingFileInfo{
				Name: f.Name,
				Size: f.Size,
			})
		}
		if infos == nil {
			infos = []waitingFileInfo{} // ensure non-null JSON array
		}

		s.sendEvent("tsnet:waitingFilesResult", waitingFilesResultData{
			Files: infos,
		})
	}()
}

func (s *shim) handleGetWaitingFile(data json.RawMessage) {
	var d getWaitingFileData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("GET_WAITING_FILE_ERROR", fmt.Sprintf("invalid getWaitingFile data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handleGetWaitingFile")

		fail := func(msg string) {
			s.sendEvent("tsnet:getWaitingFileResult", getWaitingFileResultData{
				Success:  false,
				FileName: d.FileName,
				SavePath: d.SavePath,
				Error:    msg,
			})
		}

		lc, err := srv.LocalClient()
		if err != nil {
			fail(fmt.Sprintf("failed to get local client: %v", err))
			return
		}

		// F7: bound the fetch so a stalled transfer can't hang forever.
		ctx, cancel := context.WithTimeout(s.lifecycleCtx(), 10*time.Minute)
		defer cancel()

		rc, size, err := lc.GetWaitingFile(ctx, d.FileName)
		if err != nil {
			fail(fmt.Sprintf("get waiting file failed: %v", err))
			return
		}
		defer rc.Close()

		// F8: reject an oversized file rather than exhausting local disk.
		if size > maxWaitingFileBytes {
			fail(fmt.Sprintf("waiting file too large: %d bytes (max %d)", size, maxWaitingFileBytes))
			return
		}

		outFile, err := os.Create(d.SavePath)
		if err != nil {
			fail(fmt.Sprintf("failed to create save file: %v", err))
			return
		}

		// F8/F9: copy at most `size` bytes; remove the partial file on failure.
		if _, err := io.CopyN(outFile, rc, size); err != nil {
			outFile.Close()
			os.Remove(d.SavePath)
			fail(fmt.Sprintf("failed to write file: %v", err))
			return
		}
		if err := outFile.Close(); err != nil {
			os.Remove(d.SavePath)
			fail(fmt.Sprintf("failed to finalize file: %v", err))
			return
		}

		s.sendEvent("tsnet:getWaitingFileResult", getWaitingFileResultData{
			Success:  true,
			FileName: d.FileName,
			SavePath: d.SavePath,
		})
	}()
}

func (s *shim) handleDeleteWaitingFile(data json.RawMessage) {
	var d deleteWaitingFileData
	if err := json.Unmarshal(data, &d); err != nil {
		s.sendError("DELETE_WAITING_FILE_ERROR", fmt.Sprintf("invalid deleteWaitingFile data: %v", err))
		return
	}

	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	go func() {
		defer s.recoverPanic("handleDeleteWaitingFile")

		lc, err := srv.LocalClient()
		if err != nil {
			s.sendEvent("tsnet:deleteWaitingFileResult", deleteWaitingFileResultData{
				Success:  false,
				FileName: d.FileName,
				Error:    fmt.Sprintf("failed to get local client: %v", err),
			})
			return
		}

		err = lc.DeleteWaitingFile(s.lifecycleCtx(), d.FileName)
		if err != nil {
			s.sendEvent("tsnet:deleteWaitingFileResult", deleteWaitingFileResultData{
				Success:  false,
				FileName: d.FileName,
				Error:    fmt.Sprintf("delete waiting file failed: %v", err),
			})
			return
		}

		s.sendEvent("tsnet:deleteWaitingFileResult", deleteWaitingFileResultData{
			Success:  true,
			FileName: d.FileName,
		})
	}()
}

func (s *shim) handleProxyAdd(raw json.RawMessage) {
	srv := s.getServer()
	if srv == nil {
		s.sendError("NOT_RUNNING", "node not running")
		return
	}

	var data proxyAddData
	if err := json.Unmarshal(raw, &data); err != nil {
		s.sendError("INVALID_COMMAND", "invalid proxy:add data: "+err.Error())
		return
	}

	if data.TargetHost == "" {
		data.TargetHost = "localhost"
	}
	if data.TargetScheme == "" {
		data.TargetScheme = "http"
	}

	// RFC 023 D5: nil preserves the v1 always-TLS listener.
	tlsOn := data.Tls == nil || *data.Tls

	// boundRoute is a validated, ready-to-serve mount. v1 single-target
	// configs become one "/" route so a single handler path serves both.
	type boundRoute struct {
		prefix      string
		allow       []string
		handler     http.Handler
		target      *url.URL // nil for dir routes (no WebSocket dispatch)
		stripPrefix bool
	}

	fail := func(code, msg string) {
		s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: code, Message: msg})
	}

	lc, err := srv.LocalClient()
	if err != nil {
		fail("INTERNAL", "local client unavailable: "+err.Error())
		return
	}

	// Validate + bind routes BEFORE any state is inserted, so failures need
	// no placeholder cleanup. The Rust core validates shapes too, but the
	// sidecar re-checks — it must not trust the wire (§9).
	var routes []boundRoute
	if len(data.Routes) == 0 {
		targetURL := &url.URL{Scheme: data.TargetScheme, Host: fmt.Sprintf("%s:%d", data.TargetHost, data.TargetPort)}
		if !data.AllowNonLoopback && !isLoopbackHost(data.TargetHost) {
			fail("TARGET_NOT_LOOPBACK", fmt.Sprintf("target %q is not loopback; a non-loopback target makes this node a pivot into its network — set allowNonLoopback to opt in (RFC 023 §9.3)", data.TargetHost))
			return
		}
		routes = append(routes, boundRoute{
			prefix:  "/",
			handler: s.buildReverseProxy(data.ID, targetURL, ""),
			target:  targetURL,
		})
	} else {
		for _, rt := range data.Routes {
			if !strings.HasPrefix(rt.Prefix, "/") {
				fail("INVALID_ROUTE", fmt.Sprintf("route prefix %q must start with '/'", rt.Prefix))
				return
			}
			switch {
			case rt.TargetURL != "" && rt.Dir != "":
				fail("INVALID_ROUTE", fmt.Sprintf("route %q sets both targetUrl and dir", rt.Prefix))
				return
			case rt.TargetURL != "":
				u, err := url.Parse(rt.TargetURL)
				if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
					fail("INVALID_ROUTE", fmt.Sprintf("route %q: targetUrl must be an absolute http(s) URL", rt.Prefix))
					return
				}
				if !data.AllowNonLoopback && !isLoopbackHost(u.Hostname()) {
					fail("TARGET_NOT_LOOPBACK", fmt.Sprintf("route %q target %q is not loopback — set allowNonLoopback to opt in (RFC 023 §9.3)", rt.Prefix, u.Hostname()))
					return
				}
				strip := ""
				if rt.StripPrefix {
					strip = rt.Prefix
				}
				routes = append(routes, boundRoute{
					prefix:      rt.Prefix,
					allow:       rt.Allow,
					handler:     s.buildReverseProxy(data.ID, u, strip),
					target:      u,
					stripPrefix: rt.StripPrefix,
				})
			case rt.Dir != "":
				if !filepath.IsAbs(rt.Dir) {
					fail("INVALID_ROUTE", fmt.Sprintf("route %q: dir must be an absolute path", rt.Prefix))
					return
				}
				routes = append(routes, boundRoute{
					prefix:  rt.Prefix,
					allow:   rt.Allow,
					handler: spaFileServer(rt.Dir, rt.Fallback),
				})
			default:
				fail("INVALID_ROUTE", fmt.Sprintf("route %q needs a targetUrl or a dir", rt.Prefix))
				return
			}
		}
		// Longest prefix wins, order-independent (D11): sort once, match
		// first-hit at request time.
		sort.Slice(routes, func(i, j int) bool { return len(routes[i].prefix) > len(routes[j].prefix) })
	}

	s.proxyMu.Lock()
	if _, exists := s.proxies[data.ID]; exists {
		s.proxyMu.Unlock()
		s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "PROXY_EXISTS", Message: "proxy with this ID already exists"})
		return
	}
	for _, entry := range s.proxies {
		if entry != nil && entry.listenPort == data.ListenPort {
			s.proxyMu.Unlock()
			s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "PORT_IN_USE", Message: fmt.Sprintf("port %d already used by proxy %s", data.ListenPort, entry.id)})
			return
		}
	}
	// Insert nil placeholder to prevent TOCTOU race — concurrent proxy:add
	// with the same ID will see this entry and return PROXY_EXISTS.
	s.proxies[data.ID] = nil
	s.proxyMu.Unlock()

	// Check dynamic listeners for port conflicts
	s.dynamicListenerMu.Lock()
	if _, exists := s.dynamicListeners[data.ListenPort]; exists {
		s.dynamicListenerMu.Unlock()
		// Clean up placeholder
		s.proxyMu.Lock()
		delete(s.proxies, data.ID)
		s.proxyMu.Unlock()
		s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "PORT_IN_USE", Message: fmt.Sprintf("port %d already used by a dynamic listener", data.ListenPort)})
		return
	}
	s.dynamicListenerMu.Unlock()

	forwardedProto := "http"
	if tlsOn {
		forwardedProto = "https"
	}

	// The proxyEntry keeps the v1 single-target URL for list/debug; routes
	// mode has no single target.
	var entryTargetURL *url.URL
	if len(data.Routes) == 0 {
		entryTargetURL = routes[0].target
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Identity first (§9.2): resolve who is calling from the WireGuard
		// tunnel, strip anything they claimed in our header namespace, gate,
		// then inject the verified values for the backend.
		identity := s.proxyWhois(lc, r.RemoteAddr)
		sanitizeTailscaleHeaders(r.Header)

		var route *boundRoute
		for i := range routes {
			if pathMatchesPrefix(r.URL.Path, routes[i].prefix) {
				route = &routes[i]
				break
			}
		}
		if route == nil {
			http.NotFound(w, r)
			return
		}

		if !allowedLogin(effectiveAllow(route.allow, data.Allow), identity.LoginName) {
			// Bare 403 (§9.7): no detail about the gate to non-matching callers.
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		injectIdentityHeaders(r.Header, identity)
		r.Header.Set("X-Forwarded-Proto", forwardedProto)

		// WebSocket upgrades bypass ReverseProxy via hijack — only for URL
		// targets (an upgrade against a static dir just gets the file 404s).
		// The request is already sanitized/injected, so the raw r.Write on
		// the hijack path forwards the verified headers.
		if route.target != nil && isProxyWebSocketRequest(r) {
			if route.stripPrefix {
				r.URL.Path = stripPathPrefix(r.URL.Path, route.prefix)
			}
			s.proxyMu.Lock()
			entry := s.proxies[data.ID]
			s.proxyMu.Unlock()
			s.handleProxyWebSocket(w, r, entry, route.target.Hostname(), urlPort(route.target), route.target.Scheme)
			return
		}

		// Dir mounts always strip their prefix: the mount point maps to the
		// directory ROOT ("/app/x" on a "/app" dir mount serves dir/x, never
		// dir/app/x). URL targets keep stripping explicit (D11).
		if route.target == nil && route.prefix != "/" {
			r.URL.Path = stripPathPrefix(r.URL.Path, route.prefix)
		}

		route.handler.ServeHTTP(w, r)
	})

	go func() {
		defer s.recoverPanic("handleProxyAdd")

		addr := fmt.Sprintf(":%d", data.ListenPort)
		var ln net.Listener
		var err error
		if tlsOn {
			ln, err = srv.ListenTLS("tcp", addr)
		} else {
			// RFC 023 D5: explicit tls:false serves plain HTTP — WireGuard
			// already encrypts the path; TLS is a browser-facing concern.
			ln, err = srv.Listen("tcp", addr)
		}
		if err != nil {
			// Clean up placeholder on listen failure
			s.proxyMu.Lock()
			delete(s.proxies, data.ID)
			s.proxyMu.Unlock()
			s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "LISTEN_ERROR", Message: err.Error()})
			return
		}

		// Guard against empty dnsName (node not fully started yet)
		if s.getDNSName() == "" {
			ln.Close()
			s.proxyMu.Lock()
			delete(s.proxies, data.ID)
			s.proxyMu.Unlock()
			s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "NOT_READY", Message: "node not fully started, DNS name not available"})
			return
		}

		if tlsOn {
			// RFC 023: warm the node cert now so ACME issuance happens at
			// proxy-add time, not inside the first visitor's TLS handshake.
			// Use the lifecycle context (not this proxy's serve ctx) — the
			// cert is shared by every TLS listener, so removing this one
			// proxy shouldn't cancel an in-flight warm.
			go s.prewarmCert(s.lifecycleCtx(), srv)
		}

		ctx, cancel := context.WithCancel(s.lifecycleCtx())
		// P4: bound header reads (slow-loris) and idle keep-alives. ReadTimeout
		// is intentionally unset so long-lived streaming/WebSocket bodies work.
		httpSrv := &http.Server{
			Handler:           handler,
			BaseContext:       func(net.Listener) context.Context { return ctx },
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		// Replace the nil placeholder with the real entry
		s.proxyMu.Lock()
		s.proxies[data.ID] = &proxyEntry{
			id: data.ID, name: data.Name, listenPort: data.ListenPort,
			targetHost: data.TargetHost, targetPort: data.TargetPort,
			targetScheme: data.TargetScheme, targetURL: entryTargetURL, tlsOn: tlsOn,
			listener: ln, server: httpSrv, cancel: cancel,
		}
		s.proxyMu.Unlock()

		proxyURL := publicURL(tlsOn, s.getDNSName(), data.ListenPort)
		s.sendEvent("proxy:added", proxyAddedEventData{ID: data.ID, ListenPort: data.ListenPort, URL: proxyURL})

		if err := httpSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "SERVE_ERROR", Message: err.Error()})
		}
	}()
}

func isProxyWebSocketRequest(r *http.Request) bool {
	connection := strings.ToLower(r.Header.Get("Connection"))
	upgrade := strings.ToLower(r.Header.Get("Upgrade"))
	return strings.Contains(connection, "upgrade") && upgrade == "websocket"
}

func (s *shim) handleProxyWebSocket(w http.ResponseWriter, r *http.Request, entry *proxyEntry, targetHost string, targetPort uint16, targetScheme string) {
	targetAddr := net.JoinHostPort(targetHost, strconv.Itoa(int(targetPort)))

	var targetConn net.Conn
	var err error
	if targetScheme == "https" {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		// P6: only skip TLS verification for loopback targets.
		targetConn, err = tls.DialWithDialer(dialer, "tcp", targetAddr, &tls.Config{InsecureSkipVerify: isLoopbackHost(targetHost)})
	} else {
		targetConn, err = net.DialTimeout("tcp", targetAddr, 10*time.Second)
	}
	if err != nil {
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		targetConn.Close()
		http.Error(w, "WebSocket hijack not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		targetConn.Close() // close backend conn on hijack failure
		// http.Error may not work after partial hijack, but attempt it
		http.Error(w, "WebSocket hijack failed", http.StatusInternalServerError)
		return
	}

	// P1: register both conns so proxy:remove / tsnet:stop can force them closed
	// (http.Server.Shutdown does not touch hijacked connections).
	if entry != nil {
		entry.addWS(clientConn)
		entry.addWS(targetConn)
		defer func() {
			entry.removeWS(clientConn)
			entry.removeWS(targetConn)
		}()
	}

	// Rewrite Host header for the target
	r.Host = targetAddr
	r.Header.Set("Host", targetAddr)

	// Forward the original request to the target
	if err := r.Write(targetConn); err != nil {
		targetConn.Close()
		clientConn.Close()
		return
	}

	// P2/P3: bidirectional copy with an idle deadline in each direction. On EOF
	// half-close the write side when supported; otherwise leave the peer to the
	// idle deadline rather than a full Close that could truncate an in-flight
	// reverse direction (the bug bridgeCopy documents).
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		idleCopy(targetConn, clientConn, idleDeadline)
		if hc, ok := targetConn.(halfCloser); ok {
			hc.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		idleCopy(clientConn, targetConn, idleDeadline)
		if hc, ok := clientConn.(halfCloser); ok {
			hc.CloseWrite()
		}
	}()
	wg.Wait()
	// Ensure both sides are fully closed after both goroutines finish
	targetConn.Close()
	clientConn.Close()
}

func (s *shim) handleProxyRemove(raw json.RawMessage) {
	var data proxyRemoveData
	if err := json.Unmarshal(raw, &data); err != nil {
		s.sendError("INVALID_COMMAND", "invalid proxy:remove data: "+err.Error())
		return
	}

	s.proxyMu.Lock()
	entry, exists := s.proxies[data.ID]
	if !exists || entry == nil {
		s.proxyMu.Unlock()
		s.sendEvent("proxy:error", proxyErrorEventData{ID: data.ID, Code: "NOT_FOUND", Message: "proxy not found"})
		return
	}
	delete(s.proxies, data.ID)
	s.proxyMu.Unlock()

	// G9: shut down off the dispatch loop so a slow Shutdown can't stall other
	// commands. entry.shutdown handles cancel→shutdown→listener→WS conns (P1/P5).
	go func() {
		defer s.recoverPanic("handleProxyRemove")
		entry.shutdown(5 * time.Second)
		s.sendEvent("proxy:removed", proxyRemovedEventData{ID: data.ID})
	}()
}

func (s *shim) handleProxyList() {
	s.proxyMu.Lock()
	proxies := make([]proxyInfoData, 0, len(s.proxies))
	for _, entry := range s.proxies {
		if entry == nil {
			continue // skip placeholders still being set up
		}
		proxyURL := publicURL(entry.tlsOn, s.getDNSName(), entry.listenPort)
		proxies = append(proxies, proxyInfoData{
			ID: entry.id, Name: entry.name,
			ListenPort: entry.listenPort,
			TargetHost: entry.targetHost, TargetPort: entry.targetPort,
			TargetScheme: entry.targetScheme, URL: proxyURL,
		})
	}
	s.proxyMu.Unlock()

	s.sendEvent("proxy:list", proxyListEventData{Proxies: proxies})
}

// bridgeToRust connects to Rust's local bridge port, sends the binary header,
// then does bidirectional io.Copy.
func (s *shim) bridgeToRust(tsnetConn net.Conn, port uint16, direction byte, requestID, remoteAddr, remoteDNS string) {
	defer s.recoverPanic("bridgeToRust")

	token, bridgePort := s.bridgeParams()

	localConn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", bridgePort))
	if err != nil {
		log.Printf("bridge connect failed: %v", err)
		tsnetConn.Close()
		// BUG-8: Report failure if this was an outgoing dial
		if direction == dirOutgoing && requestID != "" {
			s.sendEvent("bridge:dialResult", dialResultData{
				RequestID: requestID,
				Success:   false,
				Error:     fmt.Sprintf("bridge connect failed: %v", err),
			})
		}
		return
	}

	// Write binary header
	if err := writeHeader(localConn, token, direction, port, requestID, remoteAddr, remoteDNS); err != nil {
		log.Printf("header write failed: %v", err)
		localConn.Close()
		tsnetConn.Close()
		// BUG-8: Report failure if this was an outgoing dial
		if direction == dirOutgoing && requestID != "" {
			s.sendEvent("bridge:dialResult", dialResultData{
				RequestID: requestID,
				Success:   false,
				Error:     fmt.Sprintf("header write failed: %v", err),
			})
		}
		return
	}

	// Bidirectional copy with close-all pattern
	bridgeCopy(tsnetConn, localConn, s.idleTimeoutOrDefault())
}

// writeHeader writes the bridge binary header per RFC 003.
func writeHeader(w io.Writer, token []byte, direction byte, port uint16, requestID, remoteAddr, remoteDNS string) error {
	reqIDBytes := []byte(requestID)
	addrBytes := []byte(remoteAddr)
	dnsBytes := []byte(remoteDNS)

	// G11: each string is framed with a u16 length; refuse to emit a header that
	// would truncate a length field and desync the stream on the Rust side.
	if len(reqIDBytes) > 0xFFFF || len(addrBytes) > 0xFFFF || len(dnsBytes) > 0xFFFF {
		return fmt.Errorf("bridge header field too large: reqID=%d addr=%d dns=%d",
			len(reqIDBytes), len(addrBytes), len(dnsBytes))
	}

	// Tighter bound on the DNS/identity field: the Rust header parser caps it at
	// maxRemoteDNSNameLen and would reject the connection outright, so never emit
	// a field it is guaranteed to refuse.
	if len(dnsBytes) > maxRemoteDNSNameLen {
		return fmt.Errorf("bridge header RemoteDNSName %d exceeds cap %d; Rust would reject the connection", len(dnsBytes), maxRemoteDNSNameLen)
	}

	// Calculate total size
	size := 4 + 1 + 32 + 1 + 2 + 2 + len(reqIDBytes) + 2 + len(addrBytes) + 2 + len(dnsBytes)
	buf := make([]byte, size)
	offset := 0

	// Magic
	binary.BigEndian.PutUint32(buf[offset:], headerMagic)
	offset += 4

	// Version
	buf[offset] = headerVersion
	offset++

	// Session token (32 bytes)
	copy(buf[offset:], token)
	offset += 32

	// Direction
	buf[offset] = direction
	offset++

	// Service port
	binary.BigEndian.PutUint16(buf[offset:], port)
	offset += 2

	// RequestId
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(reqIDBytes)))
	offset += 2
	copy(buf[offset:], reqIDBytes)
	offset += len(reqIDBytes)

	// RemoteAddr
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(addrBytes)))
	offset += 2
	copy(buf[offset:], addrBytes)
	offset += len(addrBytes)

	// RemoteDNSName
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(dnsBytes)))
	offset += 2
	copy(buf[offset:], dnsBytes)

	_, err := w.Write(buf)
	return err
}

// bridgeCopy does bidirectional io.Copy with the close-all pattern.
// When either copy finishes, both connections are closed.
func bridgeCopy(tsnetConn, localConn net.Conn, idleTimeout time.Duration) {
	var once sync.Once
	closeAll := func() {
		once.Do(func() {
			tsnetConn.Close()
			localConn.Close()
		})
	}
	defer closeAll()

	var wg sync.WaitGroup
	wg.Add(2)

	// closeWrite shuts down the write half of a connection if supported.
	closeWrite := func(c net.Conn) {
		type closeWriter interface{ CloseWrite() error }
		if cw, ok := c.(closeWriter); ok {
			cw.CloseWrite()
		}
	}

	// G5: idleCopy applies an idle read/write deadline in each direction, so a
	// bridged connection that goes silent both ways is reaped instead of leaking
	// two goroutines + two fds until process exit. On EOF we half-close the
	// write half of the destination, preserving any in-flight data in the
	// reverse direction (e.g. a trailing ACK) rather than closing both.
	go func() {
		defer wg.Done()
		idleCopy(localConn, tsnetConn, idleTimeout)
		closeWrite(localConn)
	}()

	go func() {
		defer wg.Done()
		idleCopy(tsnetConn, localConn, idleTimeout)
		closeWrite(tsnetConn)
	}()

	wg.Wait()
	// Both directions complete -> close everything
}

// idleCopy is io.Copy with an idle timeout applied to each direction: if no
// bytes flow for `timeout`, the read deadline fires and the copy returns, so an
// idle-but-open bridged/relayed connection can't pin a goroutine + fds forever.
func idleCopy(dst, src net.Conn, timeout time.Duration) {
	buf := make([]byte, 32*1024)
	for {
		_ = src.SetReadDeadline(time.Now().Add(timeout))
		n, rerr := src.Read(buf)
		if n > 0 {
			_ = dst.SetWriteDeadline(time.Now().Add(timeout))
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

// publicURL renders a proxy's tailnet URL, omitting the scheme's default
// port — `https://name.ts.net/` is the demo URL RFC 023 exists for.
func publicURL(tlsOn bool, dnsName string, port uint16) string {
	scheme := "http"
	if tlsOn {
		scheme = "https"
	}
	if (tlsOn && port == 443) || (!tlsOn && port == 80) {
		return fmt.Sprintf("%s://%s", scheme, dnsName)
	}
	return fmt.Sprintf("%s://%s:%d", scheme, dnsName, port)
}

// urlPort resolves a parsed URL's port, defaulting by scheme.
func urlPort(u *url.URL) uint16 {
	if p := u.Port(); p != "" {
		if n, err := strconv.ParseUint(p, 10, 16); err == nil {
			return uint16(n)
		}
	}
	if u.Scheme == "https" {
		return 443
	}
	return 80
}

// isLoopbackHost reports whether host refers to the local loopback interface.
func isLoopbackHost(host string) bool {
	if host == "" || host == "localhost" {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// whoisIdentityErr maps a remote address to the peer's identity via WhoIs,
// surfacing the lookup error so callers can distinguish "the tailnet has no
// identity for this address" from "the lookup itself failed".
func (s *shim) whoisIdentityErr(lc *tailscale.LocalClient, remoteAddr string) (peerIdentityData, error) {
	ctx, cancel := context.WithTimeout(s.lifecycleCtx(), whoisTimeout)
	defer cancel()
	whois, err := lc.WhoIs(ctx, remoteAddr)
	if err != nil {
		return peerIdentityData{}, err
	}

	identity := peerIdentityData{}
	if whois.Node != nil {
		identity.DNSName = strings.TrimSuffix(whois.Node.Name, ".")
		identity.NodeID = string(whois.Node.StableID)
	}
	if whois.UserProfile != nil {
		identity.LoginName = whois.UserProfile.LoginName
		identity.DisplayName = whois.UserProfile.DisplayName
		identity.ProfilePicURL = whois.UserProfile.ProfilePicURL
	}
	return identity, nil
}

// whoisIdentity folds lookup failures into the anonymous case (zero
// identity). That is the right shape for the serve and TCP-accept paths,
// where anonymous fails closed at the gates; the tsnet:whois command uses
// whoisIdentityErr so failures reach the caller instead.
func (s *shim) whoisIdentity(lc *tailscale.LocalClient, remoteAddr string) peerIdentityData {
	identity, err := s.whoisIdentityErr(lc, remoteAddr)
	if err != nil {
		log.Printf("whoisIdentity: WhoIs(%s) failed: %v", remoteAddr, err)
		return peerIdentityData{}
	}
	return identity
}

// resolvePeerIdentity maps a remote address to a PeerIdentity JSON string via WhoIs.
// The JSON is placed into the bridge header's remoteDNS field so Rust can
// extract rich identity info about the connecting peer.
func (s *shim) resolvePeerIdentity(lc *tailscale.LocalClient, remoteAddr string) string {
	identity := s.whoisIdentity(lc, remoteAddr)
	if identity == (peerIdentityData{}) {
		return ""
	}
	return marshalPeerIdentity(identity)
}

// lookupCachedIdentity returns a fresh entry from the shared TTL cache.
func (s *shim) lookupCachedIdentity(addr string, now time.Time) (peerIdentityData, bool) {
	s.identityCacheMu.Lock()
	defer s.identityCacheMu.Unlock()
	if c, ok := s.identityCache[addr]; ok && now.Before(c.expires) {
		return c.identity, true
	}
	return peerIdentityData{}, false
}

// storeCachedIdentity records a lookup result in the shared TTL cache.
func (s *shim) storeCachedIdentity(addr string, identity peerIdentityData, now time.Time) {
	s.identityCacheMu.Lock()
	if s.identityCache == nil {
		s.identityCache = make(map[string]cachedIdentity)
	}
	// Crude size bound: reset rather than LRU — entries are tiny and a reset
	// only costs one extra WhoIs per live address.
	if len(s.identityCache) > 1024 {
		s.identityCache = make(map[string]cachedIdentity)
	}
	s.identityCache[addr] = cachedIdentity{identity: identity, expires: now.Add(identityCacheTTL)}
	s.identityCacheMu.Unlock()
}

// proxyWhois is whoisIdentity behind the TTL cache, for the proxy request
// path: keep-alive connections re-present the same RemoteAddr per request,
// and a WhoIs RPC per request would serialize handlers on a 3s budget.
// Failures cache as anonymous for the TTL — deliberate: a flapping WhoIs
// must not become a per-request RPC storm, and anonymous fails closed at
// the allow gate.
func (s *shim) proxyWhois(lc *tailscale.LocalClient, remoteAddr string) peerIdentityData {
	now := time.Now()
	if identity, ok := s.lookupCachedIdentity(remoteAddr, now); ok {
		return identity
	}
	identity := s.whoisIdentity(lc, remoteAddr)
	s.storeCachedIdentity(remoteAddr, identity, now)
	return identity
}

// cachedWhoisErr is whoisIdentityErr behind the same cache, for the
// tsnet:whois command path: successes (including a genuine anonymous
// answer) cache; failures return to the caller uncached so a retry can
// succeed as soon as the control plane recovers.
func (s *shim) cachedWhoisErr(lc *tailscale.LocalClient, addr string) (peerIdentityData, error) {
	now := time.Now()
	if identity, ok := s.lookupCachedIdentity(addr, now); ok {
		return identity, nil
	}
	identity, err := s.whoisIdentityErr(lc, addr)
	if err != nil {
		return peerIdentityData{}, err
	}
	s.storeCachedIdentity(addr, identity, now)
	return identity, nil
}

// ── RFC 023 engine v2: identity headers, allow-lists, routes, static ──────

// Every inbound header in this namespace is stripped before our own values
// are injected, so a caller can never smuggle a Tailscale-User-Login past a
// backend that trusts the proxy (§9.2).
const tailscaleHeaderPrefix = "Tailscale-"

// Identity headers injected for backends, matching Tailscale's own serve /
// nginx-auth convention. The node pair identifies callers that have no user
// profile at all (tagged nodes, future Funnel), which the login gate cannot.
const (
	hdrUserLogin  = "Tailscale-User-Login"
	hdrUserName   = "Tailscale-User-Name"
	hdrProfilePic = "Tailscale-User-Profile-Pic"
	hdrNodeID     = "Tailscale-Node-Id"
	hdrNodeName   = "Tailscale-Node-Name"
)

// identityCacheTTL bounds identity staleness on the proxy request path.
const identityCacheTTL = 60 * time.Second

func sanitizeTailscaleHeaders(h http.Header) {
	for name := range h {
		if strings.HasPrefix(http.CanonicalHeaderKey(name), tailscaleHeaderPrefix) {
			h.Del(name)
		}
	}
}

func injectIdentityHeaders(h http.Header, id peerIdentityData) {
	if id.LoginName != "" {
		h.Set(hdrUserLogin, id.LoginName)
	}
	if id.DisplayName != "" {
		h.Set(hdrUserName, id.DisplayName)
	}
	if id.ProfilePicURL != "" {
		h.Set(hdrProfilePic, id.ProfilePicURL)
	}
	if id.NodeID != "" {
		h.Set(hdrNodeID, id.NodeID)
	}
	if id.DNSName != "" {
		h.Set(hdrNodeName, id.DNSName)
	}
}

// allowedLogin evaluates loginName against shell-style globs (path.Match),
// case-insensitively. An empty glob list means no gate. Callers WITHOUT a
// login identity (tagged nodes, failed WhoIs, future Funnel) never pass a
// non-empty gate — fail closed (§9.7).
func allowedLogin(globs []string, login string) bool {
	if len(globs) == 0 {
		return true
	}
	l := strings.ToLower(login)
	if l == "" {
		return false
	}
	for _, g := range globs {
		if ok, err := path.Match(strings.ToLower(g), l); err == nil && ok {
			return true
		}
	}
	return false
}

// effectiveAllow picks the gate for a matched route: a per-route list
// overrides the config-level one (D14).
func effectiveAllow(routeAllowList, configAllow []string) []string {
	if len(routeAllowList) > 0 {
		return routeAllowList
	}
	return configAllow
}

// pathMatchesPrefix reports whether p is mounted under prefix. "/" matches
// everything; otherwise the match is exact or on a segment boundary, so
// "/api" matches "/api" and "/api/x" but never "/apix" (D11).
func pathMatchesPrefix(p, prefix string) bool {
	if prefix == "/" {
		return true
	}
	return p == prefix || strings.HasPrefix(p, prefix+"/")
}

// stripPathPrefix removes a matched mount prefix, always leaving an
// absolute path ("/api" request on stripped mount "/api" → "/").
func stripPathPrefix(p, prefix string) string {
	rest := strings.TrimPrefix(p, prefix)
	if !strings.HasPrefix(rest, "/") {
		rest = "/" + rest
	}
	return rest
}

// spaFileServer serves a directory with RFC 023 static semantics (§6.2):
// index.html at directory roots, no listings, dotfiles denied, optional SPA
// fallback for misses. ETag/Range/mime come from http.ServeFile.
func spaFileServer(dir, fallback string) http.Handler {
	root := http.Dir(dir)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		reqPath := path.Clean("/" + r.URL.Path)
		for _, seg := range strings.Split(reqPath, "/") {
			if len(seg) > 1 && strings.HasPrefix(seg, ".") {
				http.NotFound(w, r)
				return
			}
		}
		// serve returns false when name doesn't resolve to a servable file
		// (missing, or a directory without index.html — no listings). Every
		// file access goes through http.Dir.Open, which confines lookups to
		// the root by construction — no OS path is ever assembled from
		// request data, so there is no path-injection surface to reason
		// about. ServeContent still gives Range/conditional requests and
		// extension-based mime.
		serve := func(name string) bool {
			f, err := root.Open(name)
			if err != nil {
				return false
			}
			st, err := f.Stat()
			if err != nil {
				f.Close()
				return false
			}
			if st.IsDir() {
				f.Close()
				name = path.Join(name, "index.html")
				if f, err = root.Open(name); err != nil {
					return false
				}
				if st, err = f.Stat(); err != nil || st.IsDir() {
					f.Close()
					return false
				}
			}
			defer f.Close()
			http.ServeContent(w, r, name, st.ModTime(), f)
			return true
		}
		if serve(reqPath) {
			return
		}
		if fallback != "" && serve(path.Clean("/"+fallback)) {
			return
		}
		http.NotFound(w, r)
	})
}

// buildReverseProxy assembles the httputil.ReverseProxy for one target with
// the engine's shared policy: Host rewrite, optional mount-prefix strip,
// loopback-only TLS skip-verify, bounded transport, and the detail-free 502
// (P6/P7 carried over from v1).
func (s *shim) buildReverseProxy(proxyID string, target *url.URL, stripPrefix string) *httputil.ReverseProxy {
	rp := httputil.NewSingleHostReverseProxy(target)

	originalDirector := rp.Director
	rp.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		if stripPrefix != "" {
			req.URL.Path = stripPathPrefix(req.URL.Path, stripPrefix)
		}
	}

	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	if target.Scheme == "https" {
		// P6: only skip TLS verification for loopback targets; verify certs
		// for any non-loopback host.
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: isLoopbackHost(target.Hostname())}
	}
	rp.Transport = transport

	rp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if strings.Contains(err.Error(), "connection refused") {
			s.sendEvent("proxy:error", proxyErrorEventData{ID: proxyID, Code: "CONNECTION_REFUSED", Message: err.Error()})
		}
		// P7: don't leak internal error detail (target host/port, dial state)
		// to the remote caller; the detail is reported on the local event above.
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}
	return rp
}

// marshalPeerIdentity encodes identity as JSON bounded to maxRemoteDNSNameLen.
// Oversize OPTIONAL fields are dropped (profilePicUrl, then displayName, then
// loginName) rather than emitting a field the Rust header parser would reject;
// dnsName and nodeId are always kept so identity verification still works.
func marshalPeerIdentity(identity peerIdentityData) string {
	for _, drop := range []func(*peerIdentityData){
		func(*peerIdentityData) {},
		func(id *peerIdentityData) { id.ProfilePicURL = "" },
		func(id *peerIdentityData) { id.DisplayName = "" },
		func(id *peerIdentityData) { id.LoginName = "" },
	} {
		drop(&identity)
		data, err := json.Marshal(identity)
		if err != nil {
			log.Printf("marshalPeerIdentity: marshal failed: %v", err)
			return ""
		}
		if len(data) <= maxRemoteDNSNameLen {
			return string(data)
		}
	}
	log.Printf("marshalPeerIdentity: identity exceeds %d bytes even after dropping optional fields; omitting", maxRemoteDNSNameLen)
	return ""
}

// prewarmCert fetches the node's TLS certificate ahead of the first visitor so
// ACME issuance happens at listen time instead of inside the first browser
// handshake. Fire-and-forget and non-fatal: the TLS listener is already up and
// tsnet still issues on demand if this fails. CertPair caches to the state dir,
// so this is a no-op once a cert exists. Deduped per domain because every TLS
// listener on the node shares one cert (RFC 023 §7, D13).
//
// Failures are logged only, never sent as a tsnet:error event: ErrorEventData
// carries no correlation id (just code+message), and the Rust request loops that
// await listen/proxy confirmations treat ANY sidecar Error as their own in-flight
// failure (provider.rs listen_tcp / proxy_add), so an unsolicited warning racing
// a concurrent listen would turn a successful listen into a spurious error.
func (s *shim) prewarmCert(ctx context.Context, srv *tsnet.Server) {
	defer s.recoverPanic("prewarmCert")

	// CertDomains returns nil until the node is Running; a later listen retries.
	domains := srv.CertDomains()
	if len(domains) == 0 {
		log.Printf("cert pre-warm skipped: no cert domains yet (is HTTPS enabled for your tailnet?)")
		return
	}
	domain := domains[0]

	// Dedupe: one warm per domain per lifecycle. Marking before the fetch also
	// collapses concurrent listens racing to warm the same node cert. A failed
	// warm is not retried within the lifecycle — the listener still issues on
	// demand, and re-hitting a misconfigured tailnet would only re-fail.
	s.certWarmMu.Lock()
	if s.certWarmed == nil {
		s.certWarmed = make(map[string]struct{})
	}
	if _, done := s.certWarmed[domain]; done {
		s.certWarmMu.Unlock()
		return
	}
	s.certWarmed[domain] = struct{}{}
	s.certWarmMu.Unlock()

	lc, err := srv.LocalClient()
	if err != nil {
		log.Printf("cert pre-warm for %s skipped: %v", domain, err)
		return
	}

	// Bound the fetch: first issuance does an ACME round-trip (seconds), but a
	// misconfigured tailnet could otherwise hang this goroutine indefinitely.
	warmCtx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	if _, _, err := lc.CertPair(warmCtx, domain); err != nil {
		log.Printf("cert pre-warm for %s failed: %v (is HTTPS enabled for your tailnet?)", domain, err)
		return
	}
	debugf("cert pre-warm for %s complete", domain)
}

// monitorState polls Tailscale status every 60 seconds and emits events for
// state changes, upcoming key expiry, and health warnings.
func (s *shim) monitorState(ctx context.Context, lc *tailscale.LocalClient) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	lastState := "Running"

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		status, err := lc.StatusWithoutPeers(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("monitorState: status check failed: %v", err)
			continue
		}

		// Emit tsnet:stateChange if backend state changed
		if status.BackendState != lastState {
			s.sendEvent("tsnet:stateChange", stateChangeData{
				State: status.BackendState,
			})
			lastState = status.BackendState
		}

		// Emit tsnet:keyExpiring if our own key expires within 24 hours
		if status.Self != nil && status.Self.KeyExpiry != nil {
			expiresAt := *status.Self.KeyExpiry
			remaining := time.Until(expiresAt)
			if remaining > 0 && remaining < 24*time.Hour {
				s.sendEvent("tsnet:keyExpiring", keyExpiringData{
					ExpiresAt: expiresAt.UTC().Format(time.RFC3339),
					ExpiresIn: int64(remaining.Seconds()),
				})
			}
		}

		// Emit tsnet:healthWarning if there are health issues
		if len(status.Health) > 0 {
			s.sendEvent("tsnet:healthWarning", healthWarningData{
				Warnings: status.Health,
			})
		}
	}
}

// sendEvent writes a JSON event to stdout.
func (s *shim) sendEvent(eventType string, data interface{}) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.writer.Encode(event{Event: eventType, Data: data}); err != nil {
		log.Printf("sendEvent(%s) encode failed: %v", eventType, err)
	}
}

// sendStatus is a convenience for sending tsnet:status events.
func (s *shim) sendStatus(state, hostname, dnsName, tailscaleIP, errMsg string) {
	s.sendEvent("tsnet:status", statusData{
		State:           state,
		Hostname:        hostname,
		DNSName:         dnsName,
		TailscaleIP:     tailscaleIP,
		Error:           errMsg,
		ProtocolVersion: sidecarProtocolVersion,
	})
}

// sendError sends a tsnet:error event.
func (s *shim) sendError(code, message string) {
	s.sendEvent("tsnet:error", errorData{Code: code, Message: message})
}
