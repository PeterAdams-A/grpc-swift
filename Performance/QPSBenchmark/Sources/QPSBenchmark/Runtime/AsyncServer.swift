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

final class AsyncQpsServer {
    let eventLoopGroup: MultiThreadedEventLoopGroup
    let server: EventLoopFuture<Server>
    let threads: Int

    let logger = Logger(label: "AsyncQpsServer")

    init(config: Grpc_Testing_ServerConfig) {
        let threads = config.asyncServerThreads > 0 ? Int(config.asyncServerThreads) : System.coreCount
        self.logger.info("Sizing AsyncQpsServer", metadata: ["threads": "\(threads)"])
        self.eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        self.threads = threads

        let workerService = AsyncQpsServerImpl()

        // Start the server.
        self.server = Server.insecure(group: self.eventLoopGroup)
            .withServiceProviders([workerService])
            .withLogger(self.logger)
            .bind(host: "localhost", port: Int(config.port))
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
