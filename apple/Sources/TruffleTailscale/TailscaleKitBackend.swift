#if os(iOS) && canImport(TailscaleKit)
    import Darwin
    import Foundation
    import Network
    import TailscaleKit

    /// Production Layer 0–1 runtime backed by the pinned TailscaleKit
    /// XCFramework. Every blocking libtailscale operation runs on a dedicated
    /// dispatch queue; no C socket call blocks Swift's cooperative executor.
    public actor TailscaleKitBackend: NetworkBackend {
        private let config: MeshConfiguration
        private let logger: TailscaleLogAdapter
        private var node: TailscaleNode?
        private var handle: TailscaleHandle?
        private var localAPI: LocalAPIClient?
        private var busConsumer: TailscaleBusConsumer?
        private var busProcessor: MessageProcessor?
        private var pollTask: Task<Void, Never>?
        private var upTask: Task<Void, Never>?
        private var busRestartTask: Task<Void, Never>?
        private var subscribers: [UUID: AsyncStream<BackendEvent>.Continuation] = [:]
        private var latest = BackendStatus()
        private var stopped = false

        public init(configuration: MeshConfiguration) {
            self.config = configuration
            self.logger = TailscaleLogAdapter(configuration.logger)
        }

        public func start() async throws {
            guard node == nil else { return }
            stopped = false
            let appId = try AppId(parsing: config.appId)
            let deviceName = DeviceName(config.deviceName)
            let hostname = Hostname.tailscaleHostname(appId: appId, deviceName: deviceName)
            let stateDirectory = config.stateDirectory
                ?? FileManager.default.urls(
                    for: .applicationSupportDirectory, in: .userDomainMask)[0]
                    .appendingPathComponent("truffle", isDirectory: true)
                    .appendingPathComponent(config.appId, isDirectory: true)
            try FileManager.default.createDirectory(
                at: stateDirectory, withIntermediateDirectories: true)
            var values = URLResourceValues()
            values.isExcludedFromBackup = true
            var mutableStateDirectory = stateDirectory
            try? mutableStateDirectory.setResourceValues(values)

            let authKey: String?
            switch config.auth {
            case .authKey(let value): authKey = value
            case .authKeyProvider(let provider): authKey = try await provider()
            case .interactive, .existingState: authKey = nil
            }
            let kitConfig = Configuration(
                hostName: hostname,
                path: stateDirectory.path,
                authKey: authKey,
                controlURL: config.controlURL?.absoluteString ?? kDefaultControlURL,
                ephemeral: config.ephemeral)
            let logger = self.logger
            let createdNode = try await Self.blocking("create node") {
                try TailscaleNode(config: kitConfig, logger: logger)
            }
            guard let createdHandle = await createdNode.tailscale else {
                throw MeshError.transport("TailscaleKit returned no node handle")
            }
            node = createdNode
            handle = createdHandle
            _ = try await createdNode.loopback()
            let api = LocalAPIClient(localNode: createdNode, logger: logger)
            localAPI = api

            let consumer = TailscaleBusConsumer(
                onNotify: { [weak self] notify in await self?.handle(notify: notify) },
                onError: { [weak self] error in await self?.handleBus(error: error) })
            busConsumer = consumer
            try await startBus()
            try await refreshStatus(emitChange: true)

            pollTask = Task { [weak self] in await self?.pollLoop() }
            upTask = Task { [weak self] in await self?.bringUp(handle: createdHandle) }
            if latest.needsLogin, case .interactive = config.auth {
                try? await api.startLoginInteractive()
            }
        }

        public func stop() async {
            guard !stopped else { return }
            stopped = true
            pollTask?.cancel()
            upTask?.cancel()
            busRestartTask?.cancel()
            pollTask = nil
            upTask = nil
            busRestartTask = nil
            busProcessor?.cancel()
            busProcessor = nil
            busConsumer = nil
            localAPI = nil
            if let handle {
                _ = await Self.blockingNoThrow { tailscale_close(handle) }
            }
            handle = nil
            node = nil
            for continuation in subscribers.values { continuation.finish() }
            subscribers.removeAll()
        }

        public func status() async throws -> BackendStatus {
            try await refreshStatus(emitChange: false)
            return latest
        }

        public func events() async -> AsyncStream<BackendEvent> {
            let id = UUID()
            return AsyncStream(bufferingPolicy: .bufferingNewest(64)) { continuation in
                subscribers[id] = continuation
                continuation.yield(.status(latest))
                continuation.onTermination = { [weak self] _ in
                    Task { await self?.removeSubscriber(id) }
                }
            }
        }

        public func dial(host: String, port: UInt16) async throws -> any MeshConnection {
            guard let handle else { throw MeshError.stopped }
            let endpoint = Self.endpoint(host: host, port: port)
            let fd: Int32 = try await Self.blocking("dial \(endpoint)") {
                var connection: tailscale_conn = -1
                let result = tailscale_dial(handle, "tcp", endpoint, &connection)
                guard result == 0, connection >= 0 else {
                    throw MeshError.transport(Self.errorMessage(handle, operation: "dial"))
                }
                return connection
            }
            return TailscaleFDConnection(fd: fd)
        }

        public func listen(port: UInt16) async throws -> any MeshListener {
            guard let handle else { throw MeshError.stopped }
            let endpoint = ":\(port)"
            let fd: Int32 = try await Self.blocking("listen \(endpoint)") {
                var listener: tailscale_listener = -1
                let result = tailscale_listen(handle, "tcp", endpoint, &listener)
                guard result == 0, listener >= 0 else {
                    throw MeshError.transport(Self.errorMessage(handle, operation: "listen"))
                }
                return listener
            }
            return TailscaleFDListener(fd: fd)
        }

        public func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer {
            guard let node else { throw MeshError.stopped }
            let (sessionConfiguration, loopback) =
                try await URLSessionConfiguration.tailscaleSession(node)
            guard let ip = loopback.ip, let port = loopback.port else {
                throw MeshError.transport("invalid Tailscale LocalAPI loopback address")
            }
            var components = URLComponents()
            components.scheme = "http"
            components.host = ip
            components.port = port
            components.path = "/localapi/v0/whois"
            components.queryItems = [URLQueryItem(name: "addr", value: remoteEndpoint)]
            guard let url = components.url else {
                throw MeshError.transport("could not form LocalAPI WhoIs URL")
            }
            var request = URLRequest(url: url)
            request.timeoutInterval = 15
            let credential = Data("tsnet:\(loopback.localAPIKey)".utf8).base64EncodedString()
            request.setValue("Basic \(credential)", forHTTPHeaderField: "Authorization")
            request.setValue("localapi", forHTTPHeaderField: "Sec-Tailscale")
            let (data, response) = try await URLSession(configuration: sessionConfiguration)
                .data(for: request)
            guard let http = response as? HTTPURLResponse, http.statusCode == 200 else {
                let status = (response as? HTTPURLResponse)?.statusCode ?? -1
                throw MeshError.transport("LocalAPI WhoIs failed with HTTP \(status)")
            }
            let decoded = try JSONDecoder().decode(WhoIsResponse.self, from: data)
            let stableID = decoded.Node.StableID.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !stableID.isEmpty else {
                throw MeshError.protocolViolation("LocalAPI WhoIs returned no stable node ID")
            }
            let addresses = decoded.Node.Addresses?.map(Self.stripPrefix) ?? []
            guard let remoteIP = Self.remoteIP(remoteEndpoint),
                addresses.contains(remoteIP)
            else {
                throw MeshError.protocolViolation(
                    "LocalAPI WhoIs address does not match accepted endpoint")
            }
            return AuthenticatedPeer(
                tailscaleId: stableID,
                remoteAddresses: addresses + [remoteEndpoint])
        }

        public func makeURLSession(
            configuration: URLSessionConfiguration
        ) async throws -> URLSession {
            guard let node else { throw MeshError.stopped }
            guard configuration.identifier == nil else {
                throw MeshError.transport("background URLSession cannot use embedded Tailscale")
            }
            guard configuration.proxyConfigurations.isEmpty,
                configuration.connectionProxyDictionary?.isEmpty ?? true
            else {
                throw MeshError.transport("URLSession already has a proxy configuration")
            }
            guard let copy = configuration.copy() as? URLSessionConfiguration else {
                throw MeshError.transport("could not copy URLSession configuration")
            }
            _ = try await copy.proxyVia(node)
            return URLSession(configuration: copy)
        }

        private func pollLoop() async {
            while !Task.isCancelled, !stopped {
                try? await Task.sleep(for: latest.running ? .seconds(10) : .seconds(2))
                if Task.isCancelled || stopped { return }
                do {
                    try await refreshStatus(emitChange: true)
                    if latest.needsLogin, case .interactive = config.auth {
                        try? await localAPI?.startLoginInteractive()
                    }
                } catch {
                    emit(.health("Tailscale status poll failed: \(error)"))
                }
            }
        }

        private func bringUp(handle: TailscaleHandle) async {
            let result = await Self.blockingNoThrow { tailscale_up(handle) }
            guard !stopped else { return }
            if result != 0 {
                emit(.health(Self.errorMessage(handle, operation: "up")))
            }
            _ = try? await refreshStatus(emitChange: true)
        }

        private func startBus() async throws {
            guard let localAPI, let busConsumer else { throw MeshError.stopped }
            busProcessor?.cancel()
            // Tailscale 1.102 no longer emits ongoing full NetMap messages on
            // iOS. Every peer delta triggers handle(notify:), which refreshes
            // the canonical status snapshot, including on stream reconnect.
            let mask: Ipn.NotifyWatchOpt = [
                .initialState, .peerChanges, .noNetMap,
            ]
            busProcessor = try await localAPI.watchIPNBus(mask: mask, consumer: busConsumer)
        }

        private func handleBus(error: Error) {
            guard !stopped else { return }
            emit(.health("Tailscale IPN bus failed: \(error); restarting"))
            busProcessor?.cancel()
            busRestartTask?.cancel()
            busRestartTask = Task { [weak self] in
                guard let self else { return }
                var delay = 500
                while !Task.isCancelled, !(await self.stopped) {
                    try? await Task.sleep(for: .milliseconds(delay))
                    do {
                        try await self.startBus()
                        _ = try? await self.refreshStatus(emitChange: true)
                        return
                    } catch {
                        await self.emit(.health("Tailscale IPN bus restart failed: \(error)"))
                        delay = min(delay * 2, 5_000)
                    }
                }
            }
        }

        private func handle(notify: Ipn.Notify) async {
            if let urlString = notify.BrowseToURL, let url = URL(string: urlString) {
                emit(.authRequired(url))
            }
            if let error = notify.ErrMessage, !error.isEmpty {
                emit(.health("Tailscale: \(error)"))
            }
            _ = try? await refreshStatus(emitChange: true)
        }

        @discardableResult
        private func refreshStatus(emitChange: Bool) async throws -> BackendStatus {
            guard let localAPI else { throw MeshError.stopped }
            let raw = try await localAPI.backendStatus()
            let mapped = Self.map(raw)
            if emitChange, mapped != latest { emit(.status(mapped)) }
            if let authURL = mapped.authURL.flatMap(URL.init(string:)) {
                emit(.authRequired(authURL))
            }
            latest = mapped
            return mapped
        }

        private func emit(_ event: BackendEvent) {
            for continuation in subscribers.values { continuation.yield(event) }
        }

        private func removeSubscriber(_ id: UUID) {
            subscribers[id] = nil
        }

        private static func map(_ status: IpnState.Status) -> BackendStatus {
            let state = status.BackendState
            let peers = (status.Peer?.values ?? Dictionary<String, IpnState.PeerStatus>().values)
                .map { peer in
                    BackendPeer(
                        tailscaleId: peer.ID,
                        hostname: peer.HostName,
                        dnsName: peer.DNSName.isEmpty ? nil : peer.DNSName,
                        tailnetIPs: peer.TailscaleIPs ?? [],
                        online: peer.Online)
                }
                .sorted { $0.tailscaleId < $1.tailscaleId }
            return BackendStatus(
                running: state == "Running",
                needsLogin: state == "NeedsLogin",
                needsMachineAuth: state == "NeedsMachineAuth",
                authURL: status.AuthURL.isEmpty ? nil : status.AuthURL,
                dnsName: status.SelfStatus?.DNSName,
                tailnetIPs: status.TailscaleIPs ?? status.SelfStatus?.TailscaleIPs ?? [],
                tailscaleId: status.SelfStatus?.ID ?? "",
                peers: peers)
        }

        private static func endpoint(host: String, port: UInt16) -> String {
            host.contains(":") && !host.hasPrefix("[")
                ? "[\(host)]:\(port)" : "\(host):\(port)"
        }

        private static func remoteIP(_ endpoint: String) -> String? {
            if endpoint.hasPrefix("["), let close = endpoint.firstIndex(of: "]") {
                return String(endpoint[endpoint.index(after: endpoint.startIndex)..<close])
            }
            guard let colon = endpoint.lastIndex(of: ":") else { return endpoint }
            return String(endpoint[..<colon])
        }

        private static func stripPrefix(_ address: String) -> String {
            String(address.split(separator: "/", maxSplits: 1)[0])
        }

        private static func errorMessage(
            _ handle: TailscaleHandle, operation: String
        ) -> String {
            var bytes = [CChar](repeating: 0, count: 512)
            let result = tailscale_errmsg(handle, &bytes, bytes.count)
            let detail = result == 0 ? Self.decodeCString(bytes) : "status \(result)"
            return "Tailscale \(operation) failed: \(detail)"
        }

        private static func decodeCString(_ bytes: [CChar]) -> String {
            let prefix = bytes.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }
            return String(decoding: prefix, as: UTF8.self)
        }

        private static func blocking<T: Sendable>(
            _ label: String, _ body: @escaping @Sendable () throws -> T
        ) async throws -> T {
            try await withCheckedThrowingContinuation { continuation in
                DispatchQueue.global(qos: .userInitiated).async {
                    do { continuation.resume(returning: try body()) }
                    catch { continuation.resume(throwing: error) }
                }
            }
        }

        private static func blockingNoThrow<T: Sendable>(
            _ body: @escaping @Sendable () -> T
        ) async -> T {
            await withCheckedContinuation { continuation in
                DispatchQueue.global(qos: .utility).async {
                    continuation.resume(returning: body())
                }
            }
        }
    }

    public extension MeshNode {
        /// Production Apple entry point. The separate name avoids a target
        /// dependency cycle: `TruffleTailscale` depends on the product core,
        /// while the core remains independently testable without TailscaleKit.
        static func startTailscale(_ config: MeshConfiguration) async throws -> MeshNode {
            try await start(
                config,
                backend: TailscaleKitBackend(configuration: config),
                frameTransport: RFC6455FrameTransport(),
                identityPolicy: .failClosed)
        }
    }

    private struct WhoIsResponse: Decodable {
        struct NodeInfo: Decodable {
            let StableID: String
            let Addresses: [String]?
        }
        let Node: NodeInfo
    }

    private struct TailscaleLogAdapter: LogSink, @unchecked Sendable {
        let logFileHandle: Int32? = nil
        private let logger: (any MeshLogger)?

        init(_ logger: (any MeshLogger)?) { self.logger = logger }

        func log(_ message: String) {
            logger?.log(level: .debug, message: message)
        }
    }

    private actor TailscaleBusConsumer: MessageConsumer {
        private let onNotify: @Sendable (Ipn.Notify) async -> Void
        private let onError: @Sendable (Error) async -> Void

        init(
            onNotify: @escaping @Sendable (Ipn.Notify) async -> Void,
            onError: @escaping @Sendable (Error) async -> Void
        ) {
            self.onNotify = onNotify
            self.onError = onError
        }

        func notify(_ notify: Ipn.Notify) {
            let callback = onNotify
            Task { await callback(notify) }
        }

        func error(_ error: Error) {
            let callback = onError
            Task { await callback(error) }
        }
    }

    private final class LockedFD: @unchecked Sendable {
        private let lock = NSLock()
        private var descriptor: Int32?

        init(_ descriptor: Int32) { self.descriptor = descriptor }

        func current() -> Int32? {
            lock.lock()
            defer { lock.unlock() }
            return descriptor
        }

        func take() -> Int32? {
            lock.lock()
            defer { lock.unlock() }
            let value = descriptor
            descriptor = nil
            return value
        }
    }

    private actor TailscaleFDConnection: MeshConnection {
        private let descriptor: LockedFD
        private let readQueue = DispatchQueue(label: "io.truffle.tailscale.read")
        private let writeQueue = DispatchQueue(label: "io.truffle.tailscale.write")

        init(fd: Int32) { descriptor = LockedFD(fd) }

        func read(_ max: Int) async throws -> Data? {
            guard max > 0 else { return Data() }
            guard let fd = descriptor.current() else { return nil }
            return try await withCheckedThrowingContinuation { continuation in
                readQueue.async {
                    var storage = [UInt8](repeating: 0, count: max)
                    var count: Int
                    repeat { count = Darwin.read(fd, &storage, max) } while count < 0 && errno == EINTR
                    if count > 0 {
                        continuation.resume(returning: Data(storage.prefix(count)))
                    } else if count == 0 || errno == EBADF || errno == ECONNRESET {
                        continuation.resume(returning: nil)
                    } else {
                        continuation.resume(throwing: POSIXError(POSIXErrorCode(rawValue: errno)!))
                    }
                }
            }
        }

        func write(_ data: Data) async throws {
            guard !data.isEmpty else { return }
            guard let fd = descriptor.current() else { throw MeshError.stopped }
            try await withCheckedThrowingContinuation { continuation in
                writeQueue.async {
                    let result: Result<Void, Error> = data.withUnsafeBytes { raw in
                        guard let base = raw.baseAddress else { return .success(()) }
                        var offset = 0
                        while offset < raw.count {
                            let written = Darwin.write(fd, base.advanced(by: offset), raw.count - offset)
                            if written > 0 { offset += written; continue }
                            if written < 0 && errno == EINTR { continue }
                            let code = POSIXErrorCode(rawValue: errno) ?? .EIO
                            return .failure(POSIXError(code))
                        }
                        return .success(())
                    }
                    continuation.resume(with: result)
                }
            }
        }

        func close() async {
            guard let fd = descriptor.take() else { return }
            Darwin.shutdown(fd, SHUT_RDWR)
            Darwin.close(fd)
        }

        deinit {
            if let fd = descriptor.take() {
                Darwin.shutdown(fd, SHUT_RDWR)
                Darwin.close(fd)
            }
        }
    }

    private actor TailscaleFDListener: MeshListener {
        private let descriptor: LockedFD
        private let acceptQueue = DispatchQueue(label: "io.truffle.tailscale.accept")

        init(fd: Int32) { descriptor = LockedFD(fd) }

        func accept() async throws -> MeshAcceptedConnection {
            guard let listener = descriptor.current() else { throw MeshError.stopped }
            let accepted: (Int32, String) = try await withCheckedThrowingContinuation {
                continuation in
                acceptQueue.async {
                    var connection: tailscale_conn = -1
                    var result: Int32
                    repeat { result = tailscale_accept(listener, &connection) }
                    while result != 0 && errno == EINTR
                    guard result == 0, connection >= 0 else {
                        let code = POSIXErrorCode(rawValue: result == -1 ? errno : result) ?? .EIO
                        continuation.resume(throwing: POSIXError(code))
                        return
                    }
                    var address = [CChar](repeating: 0, count: 128)
                    let addressResult = tailscale_getremoteaddr(
                        listener, connection, &address, address.count)
                    guard addressResult == 0 else {
                        Darwin.close(connection)
                        let code = POSIXErrorCode(rawValue: addressResult) ?? .EIO
                        continuation.resume(throwing: POSIXError(code))
                        return
                    }
                    let bytes = address.prefix { $0 != 0 }.map { UInt8(bitPattern: $0) }
                    continuation.resume(
                        returning: (connection, String(decoding: bytes, as: UTF8.self)))
                }
            }
            return MeshAcceptedConnection(
                connection: TailscaleFDConnection(fd: accepted.0),
                remoteEndpoint: accepted.1)
        }

        func close() async {
            guard let fd = descriptor.take() else { return }
            Darwin.shutdown(fd, SHUT_RDWR)
            Darwin.close(fd)
        }

        deinit {
            if let fd = descriptor.take() {
                Darwin.shutdown(fd, SHUT_RDWR)
                Darwin.close(fd)
            }
        }
    }
#endif
