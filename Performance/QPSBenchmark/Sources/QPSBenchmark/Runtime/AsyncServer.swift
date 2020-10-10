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

protocol QpsServer {
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>)

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void>
}

final class AsyncQpsServer: QpsServer {
    let eventLoopGroup: MultiThreadedEventLoopGroup
    let server: EventLoopFuture<Server>
    let threads: Int

    var statsPeriodStart: Date
    var cpuStatsPeriodStart: CPUTime

    var logger = Logger(label: "AsyncQpsServer")

    init(config: Grpc_Testing_ServerConfig) {
        // logger.logLevel = .debug
        let threads = config.asyncServerThreads > 0 ? Int(config.asyncServerThreads) : System.coreCount
        self.logger.info("Sizing AsyncQpsServer", metadata: ["threads": "\(threads)"])
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        self.threads = threads

        self.statsPeriodStart = grpcTimeNow()
        self.cpuStatsPeriodStart = getResourceUsage()

        let workerService = AsyncQpsServerImpl()

        // Start the server.
        self.server = Server.insecure(group: self.eventLoopGroup)
            .withServiceProviders([workerService])
            .withLogger(self.logger)
            .bind(host: "localhost", port: Int(config.port))
    }

    // See Server::Mark
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>) {
        let currentTime = grpcTimeNow()
        let currentResourceUsage = getResourceUsage()
        var result = Grpc_Testing_ServerStatus()
        result.stats.timeElapsed = currentTime.timeIntervalSince(self.statsPeriodStart)
        result.stats.timeSystem = currentResourceUsage.systemTime - self.cpuStatsPeriodStart.systemTime
        result.stats.timeUser = currentResourceUsage.userTime - self.cpuStatsPeriodStart.userTime
        result.stats.totalCpuTime = 0
        result.stats.idleCpuTime = 0
        result.stats.cqPollCount = 0
        // TODO:  Core stats
        self.logger.info("Sending response")
        context.sendResponse(result)
        if reset {
            self.statsPeriodStart = currentTime
            self.cpuStatsPeriodStart = currentResourceUsage
        }
    }

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise: EventLoopPromise<Void> = callbackLoop.makePromise()

        self.server.map {
            server in server.close()
        }.always { result in
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
}

func createAsyncServer(config : Grpc_Testing_ServerConfig) -> AsyncQpsServer {
    /*
     return std::unique_ptr<Server>(
           new AsyncQpsServerTest<SimpleRequest, SimpleResponse,
                                  BenchmarkService::AsyncService,
                                  grpc::ServerContext>(
               config, RegisterBenchmarkService,
               &BenchmarkService::AsyncService::RequestUnaryCall,
               &BenchmarkService::AsyncService::RequestStreamingCall,
               &BenchmarkService::AsyncService::RequestStreamingFromClient,
               &BenchmarkService::AsyncService::RequestStreamingFromServer,
               &BenchmarkService::AsyncService::RequestStreamingBothWays,
               ProcessSimpleRPC));
     */
    return AsyncQpsServer(config: config)
}
