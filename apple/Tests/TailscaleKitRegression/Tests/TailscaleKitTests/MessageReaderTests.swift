import Foundation
import Testing

@testable import TailscaleKit

private final class WatchProtocol: URLProtocol, @unchecked Sendable {
    override class func canInit(with request: URLRequest) -> Bool { true }
    override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

    override func startLoading() {
        let path = request.url!.path
        if path == "/network-error" {
            client?.urlProtocol(self, didFailWithError: URLError(.networkConnectionLost))
            return
        }
        client?.urlProtocol(self, didReceive: HTTPURLResponse(
            url: request.url!, statusCode: 200, httpVersion: "HTTP/1.1",
            headerFields: ["Content-Type": "application/json"])!, cacheStoragePolicy: .notAllowed)
        if path == "/empty" {
            client?.urlProtocolDidFinishLoading(self)
            return
        }
        let body = path == "/watch"
            ? "{\"Version\":\"reconnected\"}\n"
            : "{\"ErrMessage\":\"IPN bus consumer fell behind; closing watch\"}\n"
        // Exercise URLSession's actual receive/completion delegate path,
        // including JSON split across multiple network deliveries.
        let data = Data(body.utf8)
        client?.urlProtocol(self, didLoad: data.prefix(7))
        client?.urlProtocol(self, didLoad: data.dropFirst(7))
        if path != "/watch" { client?.urlProtocolDidFinishLoading(self) }
    }

    override func stopLoading() {}
}

private func configuration() -> URLSessionConfiguration {
    let config = URLSessionConfiguration.ephemeral
    config.protocolClasses = [WatchProtocol.self]
    return config
}

private func request(_ path: String) -> URLRequest {
    URLRequest(url: URL(string: "http://ipn-watch.test\(path)")!)
}

private actor WatchConsumer: MessageConsumer {
    var notifications: [Ipn.Notify] = []
    var errors: [String] = []
    private weak var retryProcessor: MessageProcessor?

    func retryOnError(using processor: MessageProcessor) { retryProcessor = processor }
    func notify(_ notify: Ipn.Notify) { notifications.append(notify) }
    func error(_ error: Error) {
        errors.append(String(describing: error))
        if let processor = retryProcessor {
            retryProcessor = nil
            processor.start(request("/watch"), config: configuration())
        }
    }
}

private func eventually(_ predicate: () async -> Bool) async throws {
    let deadline = ContinuousClock.now + .seconds(3)
    while !(await predicate()) {
        guard ContinuousClock.now < deadline else {
            Issue.record("timed out waiting for IPN stream callback")
            return
        }
        try await Task.sleep(for: .milliseconds(20))
    }
}

@Suite struct MessageReaderTests {
    @Test func terminalNotificationThenCleanEOFReportsOneError() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        defer { processor.cancel() }
        processor.start(request("/terminal"), config: configuration())
        try await eventually { await consumer.errors.count == 1 }
        #expect(await consumer.notifications.first?.ErrMessage == "IPN bus consumer fell behind; closing watch")
        #expect(await consumer.errors == ["unexpectedEOF"])
        try await Task.sleep(for: .milliseconds(250))
        #expect(await consumer.errors.count == 1)
    }

    @Test func cleanEOFFiresWithoutTerminalNotification() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        defer { processor.cancel() }
        processor.start(request("/empty"), config: configuration())
        try await eventually { await consumer.errors.count == 1 }
        #expect(await consumer.errors == ["unexpectedEOF"])
    }

    @Test func consumerReconnectsAndReceivesNewNotifications() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        defer { processor.cancel() }
        await consumer.retryOnError(using: processor)
        processor.start(request("/terminal"), config: configuration())
        try await eventually { await consumer.notifications.contains { $0.Version == "reconnected" } }
        #expect(await consumer.errors.count == 1)
        #expect(await consumer.notifications.count == 2)
    }

    @Test func cancellationStopsReaderWithoutRestartError() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        processor.start(request("/watch"), config: configuration())
        try await eventually { await consumer.notifications.count == 1 }
        processor.cancel()
        let stopped = await withCheckedContinuation { continuation in
            processor.reader.workQueue.addOperation {
                continuation.resume(returning: processor.reader.dataTask == nil)
            }
        }
        #expect(stopped)
        try await Task.sleep(for: .milliseconds(250))
        #expect(await consumer.errors.isEmpty)
    }

    @Test func transportFailureStillReachesConsumer() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        defer { processor.cancel() }
        processor.start(request("/network-error"), config: configuration())
        try await eventually { await consumer.errors.count == 1 }
        #expect(await consumer.notifications.isEmpty)
        #expect(await consumer.errors.first?.contains("-1005") == true)
    }

    @Test func staleCompletionCannotInterruptReplacementWatch() async throws {
        let consumer = WatchConsumer()
        let processor = await MessageProcessor(consumer: consumer, logger: nil)
        defer { processor.cancel() }
        processor.start(request("/watch"), config: configuration())
        try await eventually { await consumer.notifications.count == 1 }
        let previous = await withCheckedContinuation { continuation in
            processor.reader.workQueue.addOperation {
                continuation.resume(returning: (
                    processor.reader.ipnWatchSession!, processor.reader.dataTask!))
            }
        }
        processor.start(request("/watch"), config: configuration())
        try await eventually { await consumer.notifications.count == 2 }
        processor.reader.urlSession(previous.0, task: previous.1, didCompleteWithError: nil)
        try await Task.sleep(for: .milliseconds(250))
        #expect(await consumer.errors.isEmpty)
    }
}
