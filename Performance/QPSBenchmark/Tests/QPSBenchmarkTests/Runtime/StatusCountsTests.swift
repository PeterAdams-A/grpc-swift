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
import XCTest
@testable import QPSBenchmark

class StatusCountsTests: XCTestCase {
    /* func testMessageBuilding() throws {
        var statusCounts = StatusCounts()
        statusCounts.add(status: .aborted)
        statusCounts.add(status: .aborted)
        statusCounts.add(status: .alreadyExists)

        let message = statusCounts.toRequestResultCounts()
        XCTAssertEqual(message.count, 2)
        for stat in message {
            switch stat.statusCode {
            case Int32(GRPCStatus.Code.aborted.rawValue):
                XCTAssertEqual(stat.count, 2)
            case Int32(GRPCStatus.Code.aborted.rawValue):
                XCTAssertEqual(stat.count, 1)
            default:
                XCTAssertTrue(false)
            }
        }
    }*/
}
