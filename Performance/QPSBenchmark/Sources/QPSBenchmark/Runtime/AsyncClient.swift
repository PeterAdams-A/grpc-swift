/*
 * Copyright 2020, gRPC Authors All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import NIO
import GRPC
import Logging
import Foundation
import NIOConcurrencyHelpers
import BenchmarkUtils

// Note:   ClientImpl contains more logic in C++.

// TODO: config.median_latency_collection_interval_millis

class AsyncQpsClient {
    let eventLoopGroup: MultiThreadedEventLoopGroup
    let threads: Int

    let logger = Logger(label: "AsyncQpsClient")

    init(threads: Int, eventLoopGroup: MultiThreadedEventLoopGroup) {
        self.threads = threads
        self.logger.info("Sizing AsyncQpsClient", metadata: ["threads": "\(threads)"])
        self.eventLoopGroup = eventLoopGroup
    }

    static func threadsToUse(config: Grpc_Testing_ClientConfig) -> Int {
        return config.asyncClientThreads > 0 ? Int(config.asyncClientThreads) : System.coreCount
    }
}

protocol QpsClient {
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>)

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void>
}

struct Stats {
    var latencies = Histogram()
    var statuses = StatusCounts()
}

// Stats with access controlled by a lock -
// I tried implemented with event loop hopping but the driver refuses to wait shutting
// the connection immediately after the request.
struct StatsWithLock {
    private var data = Stats()
    private let lock = Lock()

    mutating func add(latency: Double) {
        self.lock.withLock { self.data.latencies.add(value: latency) }
    }

    mutating func copyData(reset: Bool) -> Stats {
        return self.lock.withLock {
            let result = self.data
            if reset {
                self.data = Stats()
            }
            return result
        }
    }
}

// Setup threads.
// tee workers onto each.
// use client impl to send
// do again when done.
// need a way of stopping
// need to look at the histogram stuff.


final class AsyncUnaryQpsClient: AsyncQpsClient, QpsClient {
    let channelRepeaters: [ChannelRepeater]

    var statsPeriodStart: Date
    var cpuStatsPeriodStart: CPUTime


    init(config: Grpc_Testing_ClientConfig) {
        let threads = AsyncQpsClient.threadsToUse(config: config)
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        let serverTargets = try! AsyncUnaryQpsClient.parseServerTargets(serverTargets: config.serverTargets)

        self.statsPeriodStart = grpcTimeNow()
        self.cpuStatsPeriodStart = getResourceUsage()

        precondition(serverTargets.count > 0)
        var channelRepeaters: [ChannelRepeater] = []
        for channelNumber in 0..<Int(config.clientChannels) {
            channelRepeaters.append(ChannelRepeater(target: serverTargets[channelNumber % serverTargets.count],
                                                    config: config,
                                                    elg: eventLoopGroup))
        }
        self.channelRepeaters = channelRepeaters

        super.init(threads: threads, eventLoopGroup: eventLoopGroup)

        // Start the train.
        for channelRepeater in self.channelRepeaters {
            channelRepeater.start()
        }
    }

    // TODO:  See Client::Mark
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>) {
        let currentTime = grpcTimeNow()
        let currentResourceUsage = getResourceUsage()
        var result = Grpc_Testing_ClientStatus()
        result.stats.timeElapsed = currentTime.timeIntervalSince(self.statsPeriodStart)
        result.stats.timeSystem = currentResourceUsage.systemTime - self.cpuStatsPeriodStart.systemTime
        result.stats.timeUser = currentResourceUsage.userTime - self.cpuStatsPeriodStart.userTime
        result.stats.cqPollCount = 0

        var latencyHistogram = Histogram()
        var statusCounts = StatusCounts()
        for channelRepeater in self.channelRepeaters {
            let stats = channelRepeater.getStats(reset: reset)
            try! latencyHistogram.merge(source: stats.latencies)
            statusCounts.merge(source: stats.statuses)
        }
        result.stats.latencies = Grpc_Testing_HistogramData(from: latencyHistogram)
        result.stats.requestResults = statusCounts.toRequestResultCounts()
        self.logger.info("Sending response")
        _ = context.sendResponse(result)

        if reset {
            self.statsPeriodStart = currentTime
            self.cpuStatsPeriodStart = currentResourceUsage
        }
    }

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise: EventLoopPromise<Void> = callbackLoop.makePromise()
        let stoppedFutures = self.channelRepeaters.map { repeater in repeater.stop() }
        let allStopped = EventLoopFuture<Void>.reduce((),
                                                      stoppedFutures,
                                                      on: callbackLoop, { (_, _) -> Void in return () } )
        _ = allStopped.always { result in
            return self.eventLoopGroup.shutdownGracefully { error in
                if let error = error {
                    promise.fail(error)
                } else {
                    promise.succeed(())
                }
            }
        }
        return promise.futureResult
    }

    struct HostAndPort {
        var host: String
        var port: Int
    }

    struct ServerTargetParseError: Error {}

    private static func parseServerTargets(serverTargets: [String]) throws -> [HostAndPort] {
        try serverTargets.map { target in
            if let splitIndex = target.lastIndex(of: ":") {
                let host = target[..<splitIndex]
                let portString = target[(target.index(after: splitIndex))...]
                if let port = Int(portString) {
                    return HostAndPort(host: String(host), port: port)
                }
            }
            throw ServerTargetParseError()
        }
    }

    class ChannelRepeater {
        let connection: ClientConnection
        let client: Grpc_Testing_BenchmarkServiceClient
        let payloadConfig: Grpc_Testing_PayloadConfig
        let logger = Logger(label: "ChannelRepeater")
        let maxPermittedOutstandingRequests: Int
        
        private var stats: StatsWithLock

        private var stopRequested = false
        private var stopComplete: EventLoopPromise<Void>
        private var numberOfOutstandingRequests = 0

        init(target: HostAndPort,
             config : Grpc_Testing_ClientConfig,
             elg: EventLoopGroup) {
            self.connection = ClientConnection.insecure(group: elg)
                .connect(host: target.host, port: target.port)
            self.client = Grpc_Testing_BenchmarkServiceClient(channel: connection)
            self.payloadConfig = config.payloadConfig
            self.maxPermittedOutstandingRequests = Int(config.outstandingRpcsPerChannel)
            self.stopComplete = connection.eventLoop.makePromise()
            self.stats = StatsWithLock()
        }

        private func launchRequests() throws {
            while !self.stopRequested && self.numberOfOutstandingRequests < self.maxPermittedOutstandingRequests {
                try makeRequestAndRepeat()
            }
        }

        private func makeRequestAndRepeat() throws {
            if self.stopRequested || self.numberOfOutstandingRequests >= self.maxPermittedOutstandingRequests {
                return
            }
            let start = grpcTimeNow()
            let request = try ChannelRepeater.createClientRequest(payloadConfig: self.payloadConfig)
            self.numberOfOutstandingRequests += 1
            let result = client.unaryCall(request)
            // TODO:  I think C++ allows a pool of outstanding requests here.
            // TODO:  Set function to run on finished.
            // For now just keep going forever.
            // TODO:  Should probably trigger below regardless of result.
            // TODO:  Does current implementation alloc?
            _ = result.status.map { status in
                self.numberOfOutstandingRequests -= 1
                if status.isOk {
                    let end = grpcTimeNow()
                    self.recordLatency(end.timeIntervalSince(start))
                    try! self.makeRequestAndRepeat()
                } else {
                    self.logger.error("Bad status from unary request", metadata: ["status": "\(status)"])
                }
                if self.stopRequested && self.numberOfOutstandingRequests == 0 {
                    self.stopIsComplete()
                }
            }
        }

        private func recordLatency(_ latency: TimeInterval) {
            self.stats.add(latency: latency * 1e9)
        }

        func getStats(reset: Bool) -> Stats {
            return self.stats.copyData(reset: reset)
        }

        static func createClientRequest(payloadConfig: Grpc_Testing_PayloadConfig) throws -> Grpc_Testing_SimpleRequest {
            if let payload = payloadConfig.payload {
                switch payload {
                case .bytebufParams(_):
                    throw GRPCStatus(code: .invalidArgument, message: "Byte buffer not supported.")
                case .simpleParams(let simpleParams):
                    var result = Grpc_Testing_SimpleRequest()
                    result.responseType = .compressable
                    result.responseSize = simpleParams.respSize
                    result.payload.type = .compressable
                    let size = Int(simpleParams.reqSize)
                    let body = Data(count: size)
                    result.payload.body = body
                    return result
                case .complexParams(_):
                    throw GRPCStatus(code: .invalidArgument, message: "Complex params not supported.")
                }
            } else {
                // Default - simple proto without payloads.
                var result = Grpc_Testing_SimpleRequest()
                result.responseType = .compressable
                result.responseSize = 0
                result.payload.type = .compressable
                return result
            }
        }

        func start() {
            try! self.launchRequests()
        }

        private func stopIsComplete() {
            assert(self.stopRequested)
            assert(self.numberOfOutstandingRequests == 0)
            // Close the connection then signal done.
            self.connection.close().cascade(to: self.stopComplete)
        }

        func stop() -> EventLoopFuture<Void> {
            self.connection.eventLoop.execute {
                self.stopRequested = true
                if self.numberOfOutstandingRequests == 0 {
                    self.stopIsComplete()
                }
            }
            return self.stopComplete.futureResult
        }
    }
}

func createAsyncClient(config : Grpc_Testing_ClientConfig) throws -> QpsClient {
    switch config.rpcType {    
    case .unary:
        return AsyncUnaryQpsClient(config: config)
    case .streaming:
        throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
    case .streamingFromClient:
        throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
    case .streamingFromServer:
        throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
    case .streamingBothWays:
        throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
    case .UNRECOGNIZED(_):
        throw GRPCStatus(code: .invalidArgument, message: "Unrecognised client rpc type")
    }
}
