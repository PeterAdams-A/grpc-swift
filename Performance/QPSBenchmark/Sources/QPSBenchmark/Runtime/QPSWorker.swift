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

class QPSWorker {
    var driverPort: Int
    //var serverPort: Int
    //var credentialType: String

    init(driverPort: Int) { // , serverPort: Int, credentialType: String) {
        self.driverPort = driverPort
        //self.serverPort = serverPort
        //self.credentialType = credentialType
    }

    let logger = Logger(label: "QPSWorker")

    var eventLoopGroup: MultiThreadedEventLoopGroup?
    var server: EventLoopFuture<Server>?

    func start() {
        precondition(self.eventLoopGroup == nil)
        self.logger.info("Starting")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.eventLoopGroup = eventLoopGroup

        let workerService = WorkerServiceImpl()

        // Start the server.
        self.server = Server.insecure(group: eventLoopGroup)
          .withServiceProviders([workerService])
            .bind(host: "localhost", port: self.driverPort)
    }

    func syncShutdown() throws {
        precondition(self.eventLoopGroup != nil)
        self.logger.info("Stopping")
        try self.eventLoopGroup?.syncShutdownGracefully()
    }

    func Done() {
        
    }
}
