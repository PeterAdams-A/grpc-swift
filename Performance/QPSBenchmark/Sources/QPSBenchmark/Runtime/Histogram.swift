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

import Logging
import Foundation

struct HistorgramShapeMismatch: Error {

}

/// Histograms are stored with exponentially increasing bucket sizes.
/// The first bucket is [0, `multiplier`) where `multiplier` = 1 + resolution
/// Bucket n (n>=1) contains [`multiplier`**n, `multiplier`**(n+1))
/// There are sufficient buckets to reach max_bucket_start
struct Histogram {
    var sum: Double
    var sumOfSquares: Double
    var countOfValuesSeen: Double
    var multiplier: Double
    var oneOnLogMultiplier: Double
    var minSeen: Double
    var maxSeen: Double
    var maxPossible: Double
    var buckets: [UInt32]

    init(resolution: Double = 0.01, maxBucketStart: Double = 60e9) {
        precondition(resolution > 0.0)
        precondition(maxBucketStart > resolution)
        self.sum = 0.0
        self.sumOfSquares = 0.0
        self.multiplier = 1.0 + resolution
        self.oneOnLogMultiplier = 1.0 / log(1.0 + resolution)
        self.maxPossible = maxBucketStart
        self.countOfValuesSeen = 0.0
        self.minSeen = maxBucketStart
        self.maxSeen = 0.0
        let numBuckets = Histogram.bucketForUnchecked(value: maxBucketStart,
                                                      oneOnLogMultiplier: self.oneOnLogMultiplier) + 1
        precondition(numBuckets > 1)
        precondition(numBuckets < 100000000)
        self.buckets = .init(repeating: 0, count: numBuckets)
    }

    /// Determine a bucket index given a value - does no bounds checking
    private static func bucketForUnchecked(value: Double, oneOnLogMultiplier: Double) -> Int {
        return Int(log(value) * oneOnLogMultiplier)
    }

    func bucketFor(value: Double) -> Int {
        let bucket = Histogram.bucketForUnchecked(value: Histogram.clamp(value: value,
                                                                         minAllowed: 0,
                                                                         maxAllowed: self.maxPossible),
                                                  oneOnLogMultiplier: self.oneOnLogMultiplier)
        assert(bucket < self.buckets.count)
        assert(bucket >= 0)
        return bucket
    }

    static func clamp(value: Double, minAllowed: Double, maxAllowed: Double) -> Double {
        return min(maxAllowed, max(minAllowed, value))
    }

    mutating func add(value: Double) {
        self.sum += value
        self.sumOfSquares += value * value
        self.countOfValuesSeen += 1
        if (value < self.minSeen) {
            self.minSeen = value
        }
        if (value > self.maxSeen) {
            self.maxSeen = value
        }
        self.buckets[self.bucketFor(value: value)] += 1
    }

    mutating func merge(source: Histogram) throws {
        guard (self.buckets.count == source.buckets.count) || (self.multiplier == source.multiplier) else {
            // Fail because these histograms don't match.
            throw HistorgramShapeMismatch()
        }

        self.sum += source.sum
        self.sumOfSquares += source.sumOfSquares
        self.countOfValuesSeen += source.countOfValuesSeen
        if (source.minSeen < self.minSeen) {
            self.minSeen = source.minSeen
        }
        if (source.maxSeen > self.maxSeen) {
            self.maxSeen = source.maxSeen
        }
        for bucket in 0..<self.buckets.count {
            self.buckets[bucket] += source.buckets[bucket]
        }
    }
}

extension Grpc_Testing_HistogramData {
    init(from: Histogram) {
        self.bucket = from.buckets
        self.minSeen = from.minSeen
        self.maxSeen = from.maxSeen
        self.sum = from.sum
        self.sumOfSquares = from.sumOfSquares
        self.count = Double(self.bucket.count)
    }
}
