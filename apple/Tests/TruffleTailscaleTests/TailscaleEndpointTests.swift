import Testing

@testable import TruffleTailscale

@Suite struct TailscaleEndpointTests {
    @Test(arguments: [
        ("100.64.0.2", "100.64.0.2", "100.64.0.2"),
        ("100.64.0.2:12345", "100.64.0.2", "100.64.0.2:12345"),
        ("[fd7a:115c:a1e0::2]", "fd7a:115c:a1e0::2", "fd7a:115c:a1e0::2"),
        ("fd7a:115c:a1e0::2", "fd7a:115c:a1e0::2", "fd7a:115c:a1e0::2"),
        ("[fd7a:115c:a1e0::2]:12345", "fd7a:115c:a1e0::2", "[fd7a:115c:a1e0::2]:12345"),
        ("[FD7A:115C:A1E0:0:0:0:0:2]", "fd7a:115c:a1e0::2", "fd7a:115c:a1e0::2"),
    ])
    func formatsAcceptedAddressForWhoIs(input: String, ip: String, whoIs: String) throws {
        let endpoint = try #require(TailscaleEndpoint(input))
        #expect(endpoint.ip == ip)
        #expect(endpoint.whoIsAddress == whoIs)
    }

    @Test(arguments: [
        "", "hostname", "999.1.2.3", "[100.64.0.2]", "[fd7a::2", "[fd7a::2]junk",
        "[fd7a::2]:", "[fd7a::2]:65536", "[fd7a::2]:-1", "100.64.0.2:abc",
        "100.64.0.2:1:2", "fd7a:::2", "100.64.0.2\0untrusted",
    ])
    func rejectsMalformedAddresses(input: String) {
        #expect(TailscaleEndpoint(input) == nil)
    }
}
