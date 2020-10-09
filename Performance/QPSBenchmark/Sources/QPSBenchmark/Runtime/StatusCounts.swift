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

import GRPC

/// Count the number seen of each status code.
struct StatusCounts {
    private var counts: [Int: Int64] = [:]

    mutating func add(status: GRPCStatus.Code) {
        // Only record failures
        if status != .ok {
            if let previousCount = self.counts[status.rawValue] {
                self.counts[status.rawValue] = previousCount + 1
            } else {
                self.counts[status.rawValue] = 1
            }
        }
    }

    mutating func merge(source: StatusCounts) {
        for sourceCount in source.counts {
            if let existingCount = self.counts[sourceCount.key] {
                self.counts[sourceCount.key] = existingCount + sourceCount.value
            } else {
                self.counts[sourceCount.key] = sourceCount.value
            }
        }
    }
}

extension StatusCounts {
    func toRequestResultCounts() -> [Grpc_Testing_RequestResultCount] {
        return counts.map { (key, value) -> Grpc_Testing_RequestResultCount in
            var grpc = Grpc_Testing_RequestResultCount()
            grpc.count = value
            grpc.statusCode = Int32(key)
            return grpc
        }
    }
}
