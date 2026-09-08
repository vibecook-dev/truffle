import Darwin
import Foundation

/// The C binding returns IPv4 or bracketed IPv6 without a port. Other
/// backends may supply an IP:port endpoint; preserve its port for WhoIs.
struct TailscaleEndpoint: Equatable, Sendable {
    let ip: String
    let whoIsAddress: String

    init?(_ value: String) {
        let host: String
        let port: UInt16?
        if value.hasPrefix("["), let close = value.firstIndex(of: "]") {
            host = String(value[value.index(after: value.startIndex)..<close])
            let suffix = value[value.index(after: close)...]
            guard host.contains(":") else { return nil }
            if suffix.isEmpty {
                port = nil
            } else {
                guard let parsedPort = Self.port(suffix) else { return nil }
                port = parsedPort
            }
        } else if let ip = Self.normalizeIP(value) {
            self.ip = ip
            self.whoIsAddress = ip
            return
        } else if let colon = value.lastIndex(of: ":") {
            host = String(value[..<colon])
            guard !host.contains(":"), let parsedPort = Self.port(value[colon...]) else {
                return nil
            }
            port = parsedPort
        } else {
            return nil
        }
        guard let ip = Self.normalizeIP(host) else { return nil }
        self.ip = ip
        if let port {
            self.whoIsAddress = ip.contains(":") ? "[\(ip)]:\(port)" : "\(ip):\(port)"
        } else {
            self.whoIsAddress = ip
        }
    }

    private static func port(_ suffix: Substring) -> UInt16? {
        guard suffix.first == ":", !suffix.dropFirst().isEmpty,
            suffix.dropFirst().allSatisfy({ $0.isASCII && $0.isNumber }),
            let port = UInt16(suffix.dropFirst())
        else { return nil }
        return port
    }

    private static func normalizeIP(_ value: String) -> String? {
        guard !value.utf8.contains(0) else { return nil }
        var bytes = [CChar](repeating: 0, count: Int(INET6_ADDRSTRLEN))
        let capacity = socklen_t(bytes.count)
        var ipv4 = in_addr()
        var ipv6 = in6_addr()
        if value.withCString({ inet_pton(AF_INET, $0, &ipv4) }) == 1 {
            guard inet_ntop(AF_INET, &ipv4, &bytes, capacity) != nil else { return nil }
        } else if value.withCString({ inet_pton(AF_INET6, $0, &ipv6) }) == 1 {
            guard inet_ntop(AF_INET6, &ipv6, &bytes, capacity) != nil else { return nil }
        } else {
            return nil
        }
        return String(decoding: bytes.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }, as: UTF8.self)
    }
}
