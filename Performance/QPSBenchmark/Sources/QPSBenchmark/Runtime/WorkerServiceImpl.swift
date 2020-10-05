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

    // TODO: Sort out types and record.
    var runningServer: Int? = nil
    var runningClient: Int? = nil

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
                        // TODO:  Scope the next within profile.
                        /*
                     ScopedProfile profile("qps_server.prof", false);
                     Status ret = RunServerBody(ctx, stream);
                     gpr_log(GPR_INFO, "RunServer: Returning");
                     return ret;
                     */
                        self.runServerBody(context: context, serverConfig: serverConfig)


                    case .mark(let mark):
                        // TODO:  Capture stats
                        context.logger.info("server mark requested")
                        if mark.reset {
                            // TODO: reset stats.
                        }
                    }
                }

            case .end:
                context.logger.info("runServer stream ended.")
                self.runningServer = nil    // TODO:  Shutdown?
                context.statusPromise.succeed(.ok)
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
                        // TODO:  Scoped the next within profile.
                        self.runClientBody(context: context, clientConfig: clientConfig)
                        // Initial status is the default (in C++)
                        context.sendResponse(Grpc_Testing_ClientStatus())
                    case .mark(let mark):
                        // TODO:  Capture stats
                        context.logger.info("client mark requested")
                        if mark.reset {
                            // TODO:  reset stats.
                        }
                    }
                }
            case .end:
                context.logger.info("runClient ended")
                // TODO:  Shutdown
                context.statusPromise.succeed(.ok)
            }
        })
        context.logger.warning("runClient not implemented yet")
        return context.eventLoop.makeFailedFuture(GRPCStatus(code: GRPCStatus.Code.unimplemented,
                                                             message: "Not implemented"))
     /*   Status RunClient(
              ServerContext* ctx,
              ServerReaderWriter<ClientStatus, ClientArgs>* stream) override {
            gpr_log(GPR_INFO, "RunClient: Entering");
            InstanceGuard g(this);
            if (!g.Acquired()) {
              return Status(StatusCode::RESOURCE_EXHAUSTED, "Client worker busy");
            }

            ScopedProfile profile("qps_client.prof", false);
            Status ret = RunClientBody(ctx, stream);
            gpr_log(GPR_INFO, "RunClient: Returning");
            return ret;
          } */

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
                                     config : Grpc_Testing_ServerConfig) throws {
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

                context.sendResponse(response)
            }
        case .asyncGenericServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .otherServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .callbackServer:
            throw GRPCStatus(code: .unimplemented, message: "Server Type not implemented")
        case .UNRECOGNIZED(_):
            throw GRPCStatus(code: .invalidArgument, message: "Unrecognised server type")
        }
       //  switch config.serverType.rawValue
   /*   switch (config.server_type()) {
        case ServerType::SYNC_SERVER:
          return CreateSynchronousServer(config);
        case ServerType::ASYNC_SERVER:
          return CreateAsyncServer(config);
        case ServerType::ASYNC_GENERIC_SERVER:
          return CreateAsyncGenericServer(config);
        case ServerType::CALLBACK_SERVER:
          return CreateCallbackServer(config);
        default:
          abort();
      }
      abort();*/
    }

    private func runServerBody(context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>,
                               serverConfig: Grpc_Testing_ServerConfig) {
        // TODO
        var serverConfig = serverConfig
        self.serverPortOverride.map { serverConfig.port = Int32($0) }

        context.logger.info("RunServerBody: about to create server")

        do {
            try WorkerServiceImpl.createServer(context: context, config: serverConfig)

            var status = Grpc_Testing_ServerStatus()
            // status.cores =
          //  status.hasStats = false
            //status.port =
            // status.stats
        }
        catch {
            context.statusPromise.fail(error)
        }
    }

    // MARK: Create Client
    private func runClientBody(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>,
                               clientConfig: Grpc_Testing_ClientConfig) {
        do {
            try WorkerServiceImpl.createClient(context: context, clientConfig: clientConfig)
        }
        catch {
            context.statusPromise.fail(error)
        }
    }

    private static func createClient(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>,
                                     clientConfig: Grpc_Testing_ClientConfig) throws {
        switch clientConfig.clientType {
        case .syncClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .asyncClient:
            if let payloadConfig = clientConfig.payloadConfig.payload {
                switch payloadConfig {
                case .bytebufParams(_):
                    throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
                case .simpleParams(_):
                    try createAsyncClient(config: clientConfig)
                case .complexParams(_):
                    try createAsyncClient(config: clientConfig)
                }
            }
            // TODO:  else what?
        case .otherClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .callbackClient:
            throw GRPCStatus(code: .unimplemented, message: "Client Type not implemented")
        case .UNRECOGNIZED(_):
            throw GRPCStatus(code: .invalidArgument, message: "Unrecognised client type")
        }
    /*
     tatic std::unique_ptr<Client> CreateClient(const ClientConfig& config) {
       gpr_log(GPR_INFO, "Starting client of type %s %s %d",
               ClientType_Name(config.client_type()).c_str(),
               RpcType_Name(config.rpc_type()).c_str(),
               config.payload_config().has_bytebuf_params());

       switch (config.client_type()) {
         case ClientType::SYNC_CLIENT:
           return CreateSynchronousClient(config);
         case ClientType::ASYNC_CLIENT:
           return config.payload_config().has_bytebuf_params()
                      ? CreateGenericAsyncStreamingClient(config)
                      : CreateAsyncClient(config);
         case ClientType::CALLBACK_CLIENT:
           return CreateCallbackClient(config);
         default:
           abort();
       }
       abort();
     }
     */
    }

/*
gpr_log(GPR_INFO, "RunServerBody: about to create server");
std::unique_ptr<Server> server = CreateServer(args.setup());
if (g_inproc_servers != nullptr) {
g_inproc_servers->push_back(server.get());
}
if (!server) {
return Status(StatusCode::INVALID_ARGUMENT, "Couldn't create server");
}
gpr_log(GPR_INFO, "RunServerBody: server created");
ServerStatus status;
status.set_port(server->port());
status.set_cores(server->cores());
if (!stream->Write(status)) {
return Status(StatusCode::UNKNOWN, "Server couldn't report init status");
}
gpr_log(GPR_INFO, "RunServerBody: creation status reported");
while (stream->Read(&args)) {
gpr_log(GPR_INFO, "RunServerBody: Message read");
if (!args.has_mark()) {
gpr_log(GPR_INFO, "RunServerBody: Message not a mark!");
return Status(StatusCode::INVALID_ARGUMENT, "Invalid mark");
}
*status.mutable_stats() = server->Mark(args.mark().reset());
if (!stream->Write(status)) {
return Status(StatusCode::UNKNOWN, "Server couldn't respond to mark");
}
gpr_log(GPR_INFO, "RunServerBody: Mark response given");
}

gpr_log(GPR_INFO, "RunServerBody: Returning");
return Status::OK;
}

std::mutex mu_;
bool acquired_;
int server_port_;
QpsWorker* worker_;
}; */
}

