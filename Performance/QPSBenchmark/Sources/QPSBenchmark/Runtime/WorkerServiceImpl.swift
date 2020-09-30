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

struct NotImplemented: Error {
}

class WorkerServiceImpl: Grpc_Testing_WorkerServiceProvider {
    let finishedPromise: EventLoopPromise<Void>

    init(finishedPromise: EventLoopPromise<Void>) {
        self.finishedPromise = finishedPromise
    }

    func runServer(context: StreamingResponseCallContext<Grpc_Testing_ServerStatus>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_ServerArgs>) -> Void> {
        context.logger.warning("runServer not implemented yet")
        return context.eventLoop.makeFailedFuture(NotImplemented())
    }

    func runClient(context: StreamingResponseCallContext<Grpc_Testing_ClientStatus>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_ClientArgs>) -> Void> {
        context.logger.warning("runClient not implemented yet")
        return context.eventLoop.makeFailedFuture(NotImplemented())
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
}
