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
import Foundation

final class AsyncQpsServerImpl: Grpc_Testing_BenchmarkServiceProvider {
    // TODO:  In C++ a singleton response is allocated and reused.
    // Check if that makes sense in swift.
    // Might make sense to recycle payload.

    func unaryCall(request: Grpc_Testing_SimpleRequest,
                   context: StatusOnlyCallContext) -> EventLoopFuture<Grpc_Testing_SimpleResponse> {
        do {
            return context.eventLoop.makeSucceededFuture(try AsyncQpsServerImpl.processSimpleRPC(request: request))
       }
        catch {
            return context.eventLoop.makeFailedFuture(error)
        }
    }

    func streamingCall(context: StreamingResponseCallContext<Grpc_Testing_SimpleResponse>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_SimpleRequest>) -> Void> {
        context.logger.warning("streamingCall not implemented yet")
        return context.eventLoop.makeFailedFuture(GRPCStatus(code: GRPCStatus.Code.unimplemented,
                                                             message: "Not implemented"))
    }

    func streamingFromClient(context: UnaryResponseCallContext<Grpc_Testing_SimpleResponse>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_SimpleRequest>) -> Void> {
        context.logger.warning("streamingFromClient not implemented yet")
        return context.eventLoop.makeFailedFuture(GRPCStatus(code: GRPCStatus.Code.unimplemented,
                                                             message: "Not implemented"))
    }

    func streamingFromServer(request: Grpc_Testing_SimpleRequest,
                             context: StreamingResponseCallContext<Grpc_Testing_SimpleResponse>) -> EventLoopFuture<GRPCStatus> {
        context.logger.warning("streamingFromServer not implemented yet")
        return context.eventLoop.makeFailedFuture(GRPCStatus(code: GRPCStatus.Code.unimplemented,
                                                             message: "Not implemented"))
    }

    func streamingBothWays(context: StreamingResponseCallContext<Grpc_Testing_SimpleResponse>) -> EventLoopFuture<(StreamEvent<Grpc_Testing_SimpleRequest>) -> Void> {
        context.logger.warning("streamingBothWays not implemented yet")
        return context.eventLoop.makeFailedFuture(GRPCStatus(code: GRPCStatus.Code.unimplemented,
                                                             message: "Not implemented"))
    }

 /*   static Status ProcessSimpleRPC(request : Grpc_Testing_SimpleRequest) -> Grpc_Testing_SimpleResponse throws {
        if request.responseSize > 0 {
            request.responseType
            request.responseSize
        /*if (!Server::SetPayload(request->response_type(), request->response_size(),
                                response->mutable_payload())) {
          return Status(grpc::StatusCode::INTERNAL, "Error creating payload.");
        }*/
            var response = Grpc_Testing_SimpleResponse()
            response.payload =
      }
    } */

    static func makePayload(type: Grpc_Testing_PayloadType, size: Int) throws -> Grpc_Testing_Payload {
        if type != .compressable {
            throw GRPCStatus(code: .internalError, message: "Failed to make payload")
        }
        var payload = Grpc_Testing_Payload()
        payload.body = Data(count: size)
        payload.type = type
        return payload
    }

    static func processSimpleRPC(request: Grpc_Testing_SimpleRequest) throws -> Grpc_Testing_SimpleResponse {
        var response = Grpc_Testing_SimpleResponse()
        // TODO: Double check nothing else needs filing out.
        if request.responseSize > 0 {
            response.payload = try makePayload(type: request.responseType, size: Int(request.responseSize))
        }
        return response
    }

}


/* class ServerRpcContextUnaryImpl final : public ServerRpcContext {
 public:
  ServerRpcContextUnaryImpl(
      std::function<void(ServerContextType*, RequestType*,
                         grpc::ServerAsyncResponseWriter<ResponseType>*,
                         void*)>
          request_method,
      std::function<grpc::Status(RequestType*, ResponseType*)> invoke_method)
      : srv_ctx_(new ServerContextType),
        next_state_(&ServerRpcContextUnaryImpl::invoker),
        request_method_(request_method),
        invoke_method_(invoke_method),
        response_writer_(srv_ctx_.get()) {
    request_method_(srv_ctx_.get(), &req_, &response_writer_,
                    AsyncQpsServerTest::tag(this));
  }
  ~ServerRpcContextUnaryImpl() override {}
  bool RunNextState(bool ok) override { return (this->*next_state_)(ok); }
  void Reset() override {
    srv_ctx_.reset(new ServerContextType);
    req_ = RequestType();
    response_writer_ =
        grpc::ServerAsyncResponseWriter<ResponseType>(srv_ctx_.get());

    // Then request the method
    next_state_ = &ServerRpcContextUnaryImpl::invoker;
    request_method_(srv_ctx_.get(), &req_, &response_writer_,
                    AsyncQpsServerTest::tag(this));
  }

 private:
  bool finisher(bool) { return false; }
  bool invoker(bool ok) {
    if (!ok) {
      return false;
    }

    // Call the RPC processing function
    grpc::Status status = invoke_method_(&req_, &response_);

    // Have the response writer work and invoke on_finish when done
    next_state_ = &ServerRpcContextUnaryImpl::finisher;
    response_writer_.Finish(response_, status, AsyncQpsServerTest::tag(this));
    return true;
  }
  std::unique_ptr<ServerContextType> srv_ctx_;
  RequestType req_;
  ResponseType response_;
  bool (ServerRpcContextUnaryImpl::*next_state_)(bool);
  std::function<void(ServerContextType*, RequestType*,
                     grpc::ServerAsyncResponseWriter<ResponseType>*, void*)>
      request_method_;
  std::function<grpc::Status(RequestType*, ResponseType*)> invoke_method_;
  grpc::ServerAsyncResponseWriter<ResponseType> response_writer_;
};*/

/*
 static Status ProcessSimpleRPC(const PayloadConfig&, SimpleRequest* request,
                                SimpleResponse* response) {
   if (request->response_size() > 0) {
     if (!Server::SetPayload(request->response_type(), request->response_size(),
                             response->mutable_payload())) {
       return Status(grpc::StatusCode::INTERNAL, "Error creating payload.");
     }
   }
   // We are done using the request. Clear it to reduce working memory.
   // This proves to reduce cache misses in large message size cases.
   request->Clear();
   return Status::OK;
 }
 */
