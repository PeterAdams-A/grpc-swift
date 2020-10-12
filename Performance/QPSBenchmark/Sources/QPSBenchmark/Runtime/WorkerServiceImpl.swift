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

struct ServerInfo {
    var cores: Int
    var port: Int
}

class WorkerServiceImpl: Grpc_Testing_WorkerServiceProvider {
    let finishedPromise: EventLoopPromise<Void>
    let serverPortOverride: Int?

    var runningServer: QpsServer? = nil
    var runningClient: QpsClient? = nil

    init(finishedPromise: EventLoopPromise<Void>, serverPortOverride: Int?) {
        self.finishedPromise = finishedPromise
        self.serverPortOverride = serverPortOverride
    }

    func runServer(context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_ServerArgs>) -> Void> {
        context.logger.info("runServer stream started.")
        return context.eventLoop.makeSucceededFuture({ event in
            switch event {
            case .message(let serverArgs):
                if let argType = serverArgs.argtype {
                    switch argType {
                    case .setup(let serverConfig):
                        context.logger.info("server setup requested")
                        guard self.runningServer == nil else {
                            context.logger.error("server already running")
                            context.statusPromise.fail(GRPCStatus(code: GRPCStatus.Code.resourceExhausted,
                                                                  message: "Server worker busy"))
                            return
                        }
                        self.runServerBody(context: context, serverConfig: serverConfig)

                    case .mark(let mark):
                        // TODO:  Capture stats
                        context.logger.info("server mark requested")
                        guard let runningServer = self.runningServer else {
                            context.logger.error("server not running")
                            context.statusPromise.fail(GRPCStatus(code: GRPCStatus.Code.failedPrecondition,
                                                                  message: "Server not running"))
                            return
                        }
                        runningServer.sendStatus(reset: mark.reset, context: context)
                        
                    }
                }

            case .end:
                context.logger.info("runServer stream ended.")
                if let runningServer = self.runningServer {
                    self.runningServer = nil
                    let shutdownFuture = runningServer.shutdown(callbackLoop: context.eventLoop)
                    shutdownFuture.map { () in
                        return GRPCStatus(code: .ok, message: nil)
                    }.cascade(to: context.statusPromise)

                } else {
                    context.statusPromise.succeed(.ok)
                }
            }
        })
    }

    func runClient(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_ClientArgs>) -> Void> {
        context.logger.info("runClient stream started")
        return context.eventLoop.makeSucceededFuture( { event in
            switch event {
            case .message(let clientArgs):
                if let argType = clientArgs.argtype {
                    switch argType {
                    case .setup(let clientConfig):
                        context.logger.info("client setup requested")
                        guard self.runningClient == nil else {
                            context.logger.error("client already running")
                            context.statusPromise.fail(GRPCStatus(code: GRPCStatus.Code.resourceExhausted,
                                                                  message: "Client worker busy"))
                            return
                        }
                        self.runClientBody(context: context, clientConfig: clientConfig)
                        // Initial status is the default (in C++)
                        _ = context.sendResponse(Grpc_Testing_ClientStatus())
                    case .mark(let mark):
                        // Capture stats
                        context.logger.info("client mark requested")
                        guard let runningClient = self.runningClient else {
                            context.logger.error("client not running")
                            context.statusPromise.fail(GRPCStatus(code: GRPCStatus.Code.failedPrecondition,
                                                                  message: "Client not running"))
                            return
                        }
                        runningClient.sendStatus(reset: mark.reset, context: context)
                    }
                }
            case .end:
                context.logger.info("runClient ended")
                // TODO:  Shutdown
                if let runningClient = self.runningClient {
                    self.runningClient = nil
                    let shutdownFuture = runningClient.shutdown(callbackLoop: context.eventLoop)
                    shutdownFuture.map { () in
                        return GRPCStatus(code: .ok, message: nil)
                    }.cascade(to: context.statusPromise)

                } else {
                    context.statusPromise.succeed(.ok)
                }
            }
        })
    }

    func coreCount(request: Grpc_Testing_CoreRequest, context: StatusOnlyCallContext) -> EventLoopFuture<Grpc_Testing_CoreResponse> {
        context.logger.notice("coreCount queried")
        let cores = Grpc_Testing_CoreResponse.with { $0.cores = Int32(System.coreCount) }
        return context.eventLoop.makeSucceededFuture(cores)
    }

    func quitWorker(request: Grpc_Testing_Void, context: StatusOnlyCallContext) -> EventLoopFuture<Grpc_Testing_Void> {
        context.logger.warning("quitWorker called")
        self.finishedPromise.succeed(())
        return context.eventLoop.makeSucceededFuture(Grpc_Testing_Void())
    }

    // MARK: Create Server
    private static func createServer(context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>,
                                     config : Grpc_Testing_ServerConfig) throws -> QpsServer {
        context.logger.info("Starting server", metadata: ["type": .stringConvertible(config.serverType)])

        switch config.serverType {
        case .syncServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .asyncServer:
            let asyncServer = createAsyncServer(config: config)
            asyncServer.server.whenSuccess { server in
                let port = server.channel.localAddress?.port ?? 0
                let threads = asyncServer.threads

                var response = Grpc_Testing_ServerStatus()
                response.cores = Int32(threads)
                response.port = Int32(port)

                _ = context.sendResponse(response)
            }
            return asyncServer
        case .asyncGenericServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .otherServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .callbackServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .UNRECOGNIZED(_):
            throw GRPCStatus(code: .invalidArgument, message: "Unrecognised server type")
        }
    }

    private func runServerBody(context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>,
                               serverConfig: Grpc_Testing_ServerConfig) {
        // TODO
        var serverConfig = serverConfig
        self.serverPortOverride.map { serverConfig.port = Int32($0) }

        context.logger.info("RunServerBody: about to create server")

        do {
            self.runningServer = try WorkerServiceImpl.createServer(context: context, config: serverConfig)
        }
        catch {
            context.statusPromise.fail(error)
        }
    }

    // MARK: Create Client
    private func runClientBody(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>,
                               clientConfig: Grpc_Testing_ClientConfig) {
        do {
            self.runningClient = try WorkerServiceImpl.createClient(context: context, clientConfig: clientConfig)
        }
        catch {
            context.statusPromise.fail(error)
        }
    }

    private static func createClient(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>,
                                     clientConfig: Grpc_Testing_ClientConfig) throws -> QpsClient {
        switch clientConfig.clientType {
        case .syncClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .asyncClient:
            if let payloadConfig = clientConfig.payloadConfig.payload {
                switch payloadConfig {
                case .bytebufParams(_):
                    throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
                case .simpleParams(_):
                    return try createAsyncClient(config: clientConfig)
                case .complexParams(_):
                    return try createAsyncClient(config: clientConfig)
                }
            }
            // TODO:  else what?
            // This is defaulted in C++
            throw GRPCStatus(code: .unimplemented, message: "Client without payload config")
        case .otherClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .callbackClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .UNRECOGNIZED(_):
            throw GRPCStatus(code: .invalidArgument, message: "Unrecognised client type")
        }
    }
}

