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

/*
 template <class StubType, class RequestType>
 class AsyncClient : public ClientImpl<StubType, RequestType> {
   // Specify which protected members we are using since there is no
   // member name resolution until the template types are fully resolved
  public:
   using Client::closed_loop_;
   using Client::NextIssuer;
   using Client::SetupLoadTest;
   using ClientImpl<StubType, RequestType>::cores_;
   using ClientImpl<StubType, RequestType>::channels_;
   using ClientImpl<StubType, RequestType>::request_;
   AsyncClient(const ClientConfig& config,
               std::function<ClientRpcContext*(
                   StubType*, std::function<gpr_timespec()> next_issue,
                   const RequestType&)>
                   setup_ctx,
               std::function<std::unique_ptr<StubType>(std::shared_ptr<Channel>)>
                   create_stub)
       : ClientImpl<StubType, RequestType>(config, create_stub),
         num_async_threads_(NumThreads(config)) {
     SetupLoadTest(config, num_async_threads_);

     int tpc = std::max(1, config.threads_per_cq());      // 1 if unspecified
     int num_cqs = (num_async_threads_ + tpc - 1) / tpc;  // ceiling operator
     for (int i = 0; i < num_cqs; i++) {
       cli_cqs_.emplace_back(new CompletionQueue);
     }

     for (int i = 0; i < num_async_threads_; i++) {
       cq_.emplace_back(i % cli_cqs_.size());
       next_issuers_.emplace_back(NextIssuer(i));
       shutdown_state_.emplace_back(new PerThreadShutdownState());
     }

     int t = 0;
     for (int ch = 0; ch < config.client_channels(); ch++) {
       for (int i = 0; i < config.outstanding_rpcs_per_channel(); i++) {
         auto* cq = cli_cqs_[t].get();
         auto ctx =
             setup_ctx(channels_[ch].get_stub(), next_issuers_[t], request_);
         ctx->Start(cq, config);
       }
       t = (t + 1) % cli_cqs_.size();
     }
   }
   virtual ~AsyncClient() {
     for (auto cq = cli_cqs_.begin(); cq != cli_cqs_.end(); cq++) {
       void* got_tag;
       bool ok;
       while ((*cq)->Next(&got_tag, &ok)) {
         delete ClientRpcContext::detag(got_tag);
       }
     }
   }

   int GetPollCount() override {
     int count = 0;
     for (auto cq = cli_cqs_.begin(); cq != cli_cqs_.end(); cq++) {
       count += grpc_get_cq_poll_num((*cq)->cq());
     }
     return count;
   }

  protected:
   const int num_async_threads_;

  private:
   struct PerThreadShutdownState {
     mutable std::mutex mutex;
     bool shutdown;
     PerThreadShutdownState() : shutdown(false) {}
   };

   int NumThreads(const ClientConfig& config) {
     int num_threads = config.async_client_threads();
     if (num_threads <= 0) {  // Use dynamic sizing
       num_threads = cores_;
       gpr_log(GPR_INFO, "Sizing async client to %d threads", num_threads);
     }
     return num_threads;
   }
   void DestroyMultithreading() override final {
     for (auto ss = shutdown_state_.begin(); ss != shutdown_state_.end(); ++ss) {
       std::lock_guard<std::mutex> lock((*ss)->mutex);
       (*ss)->shutdown = true;
     }
     for (auto cq = cli_cqs_.begin(); cq != cli_cqs_.end(); cq++) {
       (*cq)->Shutdown();
     }
     this->EndThreads();  // this needed for resolution
   }

   ClientRpcContext* ProcessTag(size_t thread_idx, void* tag) {
     ClientRpcContext* ctx = ClientRpcContext::detag(tag);
     if (shutdown_state_[thread_idx]->shutdown) {
       ctx->TryCancel();
       delete ctx;
       bool ok;
       while (cli_cqs_[cq_[thread_idx]]->Next(&tag, &ok)) {
         ctx = ClientRpcContext::detag(tag);
         ctx->TryCancel();
         delete ctx;
       }
       return nullptr;
     }
     return ctx;
   }

   void ThreadFunc(size_t thread_idx, Client::Thread* t) override final {
     void* got_tag;
     bool ok;

     HistogramEntry entry;
     HistogramEntry* entry_ptr = &entry;
     if (!cli_cqs_[cq_[thread_idx]]->Next(&got_tag, &ok)) {
       return;
     }
     std::mutex* shutdown_mu = &shutdown_state_[thread_idx]->mutex;
     shutdown_mu->lock();
     ClientRpcContext* ctx = ProcessTag(thread_idx, got_tag);
     if (ctx == nullptr) {
       shutdown_mu->unlock();
       return;
     }
     while (cli_cqs_[cq_[thread_idx]]->DoThenAsyncNext(
         [&, ctx, ok, entry_ptr, shutdown_mu]() {
           if (!ctx->RunNextState(ok, entry_ptr)) {
             // The RPC and callback are done, so clone the ctx
             // and kickstart the new one
             ctx->StartNewClone(cli_cqs_[cq_[thread_idx]].get());
             delete ctx;
           }
           shutdown_mu->unlock();
         },
         &got_tag, &ok, gpr_inf_future(GPR_CLOCK_REALTIME))) {
       t->UpdateHistogram(entry_ptr);
       entry = HistogramEntry();
       shutdown_mu->lock();
       ctx = ProcessTag(thread_idx, got_tag);
       if (ctx == nullptr) {
         shutdown_mu->unlock();
         return;
       }
     }
   }

   std::vector<std::unique_ptr<CompletionQueue>> cli_cqs_;
   std::vector<int> cq_;
   std::vector<std::function<gpr_timespec()>> next_issuers_;
   std::vector<std::unique_ptr<PerThreadShutdownState>> shutdown_state_;
 };

 */

// Note:   ClientImpl contains more logic in C++.

class AsyncQpsClient {
    let eventLoopGroup: MultiThreadedEventLoopGroup
    let threads: Int

    let logger = Logger(label: "AsyncQpsClient")

    init(threads: Int, eventLoopGroup: MultiThreadedEventLoopGroup) {
        self.threads = threads
        self.logger.info("Sizing AsyncQpsClient", metadata: ["threads": "\(threads)"])
        self.eventLoopGroup = eventLoopGroup

        // TODO:  Setup workers based on
        // config.clientChannels - number of workers.
        // config.outstandingRpcsPerChannel -

       /* let workerService = AsyncQpsServerImpl()

        // Start the server.
        self.server = Server.insecure(group: self.eventLoopGroup)
            .withServiceProviders([workerService])
            .withLogger(self.logger)
            .bind(host: "localhost", port: Int(config.port))*/
    }

    static func threadsToUse(config: Grpc_Testing_ClientConfig) -> Int {
        return config.asyncClientThreads > 0 ? Int(config.asyncClientThreads) : System.coreCount
    }
}

protocol QpsClient {
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>)

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void>
}

// Setup threads.
// tee workers onto each.
// use client impl to send
// do again when done.
// need a way of stopping
// need to look at the histogram stuff.


final class AsyncUnaryQpsClient: AsyncQpsClient, QpsClient {
    let channelRepeaters: [ChannelRepeater]
    private var latencyHistogram: Histogram
    private let histogramLock: Lock

    init(config: Grpc_Testing_ClientConfig) {
        let threads = AsyncQpsClient.threadsToUse(config: config)
        // TODO:  READ THE COMMENT config.clientChannels
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        let serverTargets = try! AsyncUnaryQpsClient.parseServerTargets(serverTargets: config.serverTargets)

        precondition(serverTargets.count > 0)
        var channelRepeaters: [ChannelRepeater] = []
        for channelNumber in 0..<Int(config.clientChannels) {
            channelRepeaters.append(ChannelRepeater(target: serverTargets[channelNumber % serverTargets.count],
                                                    config: config,
                                                    elg: eventLoopGroup))
        }
        self.channelRepeaters = channelRepeaters

        self.latencyHistogram = Histogram()
        self.histogramLock = Lock()
        super.init(threads: threads, eventLoopGroup: eventLoopGroup)

        // Start the train.
        for channelRepeater in self.channelRepeaters {
            channelRepeater.start(recordLatency: { latency in
                // TODO:  Periodic capture if requested.
                self.histogramLock.withLock { self.latencyHistogram.add(value: latency * 1e9) }
            })
        }
    }

    // TODO:  See Client::Mark
    func sendStatus(reset: Bool, context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>) {
        var result = Grpc_Testing_ClientStatus()
        result.stats.timeElapsed = 0
        result.stats.timeSystem = 0
        result.stats.timeUser = 0
        result.stats.cqPollCount = 0
        // TODO:  Histograms and metrics into result.stats.coreStats.
        // TODO:  Fill in latencies.
        // Latencies
        // Need to ask the other thread to give us the data (or use locks)
        /* let latencies = requestRepeater.connection.eventLoop.submit({ () -> Histogram in
            let result = self.latencyHistogram
            // TODO:  Need to swap in new?
            return result
        }).hop(to: context.eventLoop).map({ histogram -> Void in
            result.stats.latencies = Grpc_Testing_HistogramData(from: histogram)
            self.logger.info("Sending response")
            context.sendResponse(result)
        }) */

        let latencies = self.histogramLock.withLock {
            return self.latencyHistogram
        }
        result.stats.latencies = Grpc_Testing_HistogramData(from: latencies)
        self.logger.info("Sending response")
        context.sendResponse(result)

        // EventLoopFuture fold to join.

        // TODO:  Request results
        if reset {
            // TODO:  reset stats.
        }
    }

    func shutdown(callbackLoop: EventLoop) -> EventLoopFuture<Void> {
        let stoppedFutures = self.channelRepeaters.map { repeater in repeater.stop() }
        return EventLoopFuture<Void>.reduce((), stoppedFutures, on: callbackLoop, { (x, y) -> Void in return () } )
        /* let repeaterStopped = self.requestRepeater.stop()
        repeaterStopped.always { result in
            self.eventLoopGroup.shutdownGracefully { error in
                if let error = error {
                    promise.fail(error)
                } else {
                    promise.succeed(())
                }
            }
        }
        repeaterStopped.cascade(to: promise)*/
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
            self.stopComplete = connection.eventLoop.makePromise()
        }

        private func makeRequestAndRepeat(recordLatency: @escaping (TimeInterval) -> Void) throws {
            if self.stopRequested {
                return
            }
            let start = grpcTimeNow()
            let request = try ChannelRepeater.createClientRequest(payloadConfig: self.payloadConfig)
            self.numberOfOutstandingRequests += 1
            let result = client.unaryCall(request)
            // TODO:  I think C++ allows a pool of outstanding requests here.
            // TODO:  Set function to run on finished.
            // For now just keep going forever.
            result.status.map { status in
                self.numberOfOutstandingRequests -= 1
                if status.isOk {
                    let end = grpcTimeNow()
                    recordLatency(end.timeIntervalSince(start))
                    try! self.makeRequestAndRepeat(recordLatency: recordLatency)
                } else {
                    self.logger.error("Bad status from unary request", metadata: ["status": "\(status)"])
                }
                if self.stopRequested && self.numberOfOutstandingRequests == 0 {
                    self.stopComplete.succeed(())
                }
            }
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

        func start(recordLatency: @escaping (TimeInterval) -> Void) {
            try! makeRequestAndRepeat(recordLatency: recordLatency)
        }

        func stop() -> EventLoopFuture<Void> {
            self.connection.eventLoop.execute {
                self.stopRequested = true
                if self.numberOfOutstandingRequests == 0 {
                    self.stopComplete.succeed(())
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
