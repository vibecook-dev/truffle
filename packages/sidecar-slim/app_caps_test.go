package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"tailscale.com/client/tailscale/apitype"
	"tailscale.com/tailcfg"
)

// fluxGrant is the kind of value a tailnet policy attaches to a capability:
// {"grants":[{"src":["group:kids"],"dst":["tag:flux"],"ip":["tcp:443"],
//
//	"app":{"vibecook.dev/cap/flux":[{"role":"kid"}]}}]}
const fluxCap = "vibecook.dev/cap/flux"

func whoisFixture() *apitype.WhoIsResponse {
	return &apitype.WhoIsResponse{
		Node: &tailcfg.Node{
			Name:     "living-room-tv.tail1234.ts.net.",
			StableID: "nTVSTABLE",
			User:     4242,
			Tags:     []string{"tag:tv", "tag:kids"},
		},
		UserProfile: &tailcfg.UserProfile{
			ID:          4242,
			LoginName:   "alice@example.com",
			DisplayName: "Alice",
		},
		CapMap: tailcfg.PeerCapMap{
			fluxCap:                      {`{"role":"kid"}`, `{"libraries":["Cartoons"]}`},
			"example.com/cap/other":      {`{"x":1}`},
			"example.com/cap/empty-rule": {""},
		},
	}
}

// identityFromWhois carries userId, tags and the capability map through, in
// addition to the five v8 string fields.
func TestIdentityFromWhoisCarriesCapsTagsAndUserID(t *testing.T) {
	id := identityFromWhois(whoisFixture())
	if id.DNSName != "living-room-tv.tail1234.ts.net" || id.NodeID != "nTVSTABLE" || id.LoginName != "alice@example.com" {
		t.Fatalf("v8 fields lost: %+v", id)
	}
	if id.UserID != 4242 {
		t.Errorf("userId = %d, want 4242", id.UserID)
	}
	if !reflect.DeepEqual(id.Tags, []string{"tag:tv", "tag:kids"}) {
		t.Errorf("tags = %v", id.Tags)
	}
	want := map[string][]json.RawMessage{
		fluxCap:                      {json.RawMessage(`{"role":"kid"}`), json.RawMessage(`{"libraries":["Cartoons"]}`)},
		"example.com/cap/other":      {json.RawMessage(`{"x":1}`)},
		"example.com/cap/empty-rule": {json.RawMessage(`null`)},
	}
	if !reflect.DeepEqual(id.CapMap, want) {
		t.Errorf("capMap = %v, want %v", id.CapMap, want)
	}
	if identityFromWhois(nil).isEmpty() != true {
		t.Error("a nil WhoIs answer must map to the anonymous identity")
	}
}

// The bridge-header JSON round-trips the new fields under camelCase keys that
// truffle-core's TailscalePeerIdentity deserializes.
func TestMarshalPeerIdentityCarriesCapMap(t *testing.T) {
	id := identityFromWhois(whoisFixture())
	out := marshalPeerIdentity(id)
	for _, key := range []string{`"userId":4242`, `"tags":["tag:tv","tag:kids"]`, `"capMap":{`, `"vibecook.dev/cap/flux":[{"role":"kid"},{"libraries":["Cartoons"]}]`} {
		if !strings.Contains(out, key) {
			t.Errorf("identity JSON missing %s: %s", key, out)
		}
	}
	if strings.Contains(out, "capMapDropped") {
		t.Errorf("capMapDropped must be omitted when nothing was dropped: %s", out)
	}
	var back peerIdentityData
	if err := json.Unmarshal([]byte(out), &back); err != nil {
		t.Fatal(err)
	}
	if back.UserID != id.UserID || !reflect.DeepEqual(back.Tags, id.Tags) || len(back.CapMap) != len(id.CapMap) {
		t.Errorf("round trip lost fields: %+v", back)
	}
}

// A capability map too large for the bridge header is dropped AND flagged, so
// an app deriving permissions from it can fail closed; the login, node id,
// user id and tags survive.
func TestMarshalPeerIdentityDropsOversizeCapMapWithFlag(t *testing.T) {
	id := identityFromWhois(whoisFixture())
	id.CapMap["example.com/cap/huge"] = []json.RawMessage{json.RawMessage(`"` + strings.Repeat("x", maxRemoteDNSNameLen) + `"`)}
	out := marshalPeerIdentity(id)
	if len(out) == 0 || len(out) > maxRemoteDNSNameLen {
		t.Fatalf("marshal produced %d bytes (cap %d)", len(out), maxRemoteDNSNameLen)
	}
	var back peerIdentityData
	if err := json.Unmarshal([]byte(out), &back); err != nil {
		t.Fatal(err)
	}
	if back.CapMap != nil || !back.CapMapDropped {
		t.Errorf("oversize capMap must be dropped and flagged: %+v", back)
	}
	if back.LoginName != "alice@example.com" || back.NodeID != "nTVSTABLE" || back.UserID != 4242 || len(back.Tags) != 2 {
		t.Errorf("identity fields must survive a capMap drop: %+v", back)
	}
}

func TestPeerIdentityIsEmpty(t *testing.T) {
	if !(peerIdentityData{}).isEmpty() {
		t.Error("zero identity must be empty")
	}
	for name, id := range map[string]peerIdentityData{
		"tags only":   {Tags: []string{"tag:x"}},
		"userId only": {UserID: 1},
		"caps only":   {CapMap: map[string][]json.RawMessage{"a/b": nil}},
		"dropped":     {CapMapDropped: true},
	} {
		if id.isEmpty() {
			t.Errorf("%s: identity with information reported empty", name)
		}
	}
}

// The whoisResult event (Node::whois) carries the same new fields.
func TestWhoisResultCarriesCapMap(t *testing.T) {
	id := identityFromWhois(whoisFixture())
	b, err := json.Marshal(whoisResultData{Addr: "100.64.0.7", Identity: &id})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"capMap":{`, `"tags":[`, `"userId":4242`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("whoisResult missing %s: %s", want, b)
		}
	}
}

// Only allow-listed capabilities reach the backend, as JSON in
// Tailscale-App-Capabilities; nothing is sent when no allow-list is configured
// or the caller is anonymous; an identified caller with none of the accepted
// capabilities gets "{}".
func TestInjectAppCapabilities(t *testing.T) {
	id := identityFromWhois(whoisFixture())

	h := http.Header{}
	injectAppCapabilities(h, id, []string{fluxCap, "example.com/cap/absent"})
	var got map[string][]map[string]any
	if err := json.Unmarshal([]byte(h.Get(hdrAppCapabilities)), &got); err != nil {
		t.Fatalf("header is not JSON: %q: %v", h.Get(hdrAppCapabilities), err)
	}
	if len(got) != 1 || len(got[fluxCap]) != 2 || got[fluxCap][0]["role"] != "kid" {
		t.Errorf("filtered caps = %v", got)
	}

	h = http.Header{}
	injectAppCapabilities(h, id, nil)
	if _, ok := h[hdrAppCapabilities]; ok {
		t.Error("no allow-list must send no header")
	}

	h = http.Header{}
	injectAppCapabilities(h, peerIdentityData{}, []string{fluxCap})
	if _, ok := h[hdrAppCapabilities]; ok {
		t.Error("an anonymous caller must get no header")
	}

	h = http.Header{}
	injectAppCapabilities(h, peerIdentityData{LoginName: "bob@example.com"}, []string{fluxCap})
	if v := h.Get(hdrAppCapabilities); v != "{}" {
		t.Errorf("identified caller without caps: header = %q, want {}", v)
	}
}

// A spoofed inbound header never survives: the namespace is stripped before
// the verified value is injected.
func TestAppCapabilitiesHeaderCannotBeSpoofed(t *testing.T) {
	h := http.Header{}
	h.Set(hdrAppCapabilities, `{"vibecook.dev/cap/flux":[{"role":"admin"}]}`)
	sanitizeTailscaleHeaders(h)
	injectAppCapabilities(h, peerIdentityData{LoginName: "mallory@example.com"}, []string{fluxCap})
	if v := h.Get(hdrAppCapabilities); v != "{}" {
		t.Errorf("spoofed capabilities survived: %q", v)
	}
}

// Non-ASCII values are escaped to \uXXXX so the header value is pure ASCII yet
// decodes to the identical JSON.
func TestAsciiJSONPreservesMeaning(t *testing.T) {
	in := []byte(`{"libraries":["Kinderfilme ü","日本","🎬"]}`)
	out := asciiJSON(in)
	for _, r := range out {
		if r >= 0x80 {
			t.Fatalf("non-ASCII rune %q in %s", r, out)
		}
	}
	var a, b any
	if err := json.Unmarshal(in, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(out), &b); err != nil {
		t.Fatalf("escaped JSON does not parse: %s: %v", out, err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("escaping changed meaning: %v vs %v", a, b)
	}
}

func TestValidateAcceptAppCaps(t *testing.T) {
	if f := validateAcceptAppCaps([]string{fluxCap, "tailscale.com/cap/whatever"}); f != nil {
		t.Errorf("valid names rejected: %+v", f)
	}
	for _, bad := range []string{"", "noslash", "has space/x", "tab\t/x", strings.Repeat("a", 250) + "/abcdefgh"} {
		if f := validateAcceptAppCaps([]string{bad}); f == nil || f.code != "INVALID_APP_CAP" {
			t.Errorf("%q accepted", bad)
		}
	}
}
