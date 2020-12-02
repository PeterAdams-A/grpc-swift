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
import EchoModel
@testable import GRPC
import NIO
import NIOHTTP2
import XCTest

class ConnectionManagerTests: GRPCTestCase {
  private let loop = EmbeddedEventLoop()
  private let recorder = RecordingConnectivityDelegate()

  private var defaultConfiguration: ClientConnection.Configuration {
    return ClientConnection.Configuration(
      target: .unixDomainSocket("/ignored"),
      eventLoopGroup: self.loop,
      connectivityStateDelegate: self.recorder,
      connectionBackoff: nil,
      backgroundActivityLogger: self.clientLogger
    )
  }

  override func tearDown() {
    XCTAssertNoThrow(try self.loop.syncShutdownGracefully())
    super.tearDown()
  }

  private func waitForStateChange<Result>(
    from: ConnectivityState,
    to: ConnectivityState,
    timeout: DispatchTimeInterval = .seconds(1),
    file: StaticString = #file,
    line: UInt = #line,
    body: () throws -> Result
  ) rethrows -> Result {
    self.recorder.expectChange {
      XCTAssertEqual($0, Change(from: from, to: to), file: file, line: line)
    }
    let result = try body()
    self.recorder.waitForExpectedChanges(timeout: timeout, file: file, line: line)
    return result
  }

  private func waitForStateChanges<Result>(
    _ changes: [Change],
    timeout: DispatchTimeInterval = .seconds(1),
    file: StaticString = #file,
    line: UInt = #line,
    body: () throws -> Result
  ) rethrows -> Result {
    self.recorder.expectChanges(changes.count) {
      XCTAssertEqual($0, changes)
    }
    let result = try body()
    self.recorder.waitForExpectedChanges(timeout: timeout, file: file, line: line)
    return result
  }
}

extension ConnectionManagerTests {
  func testIdleShutdown() throws {
    let manager = ConnectionManager(configuration: self.defaultConfiguration, logger: self.logger)

    try self.waitForStateChange(from: .idle, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // Getting a multiplexer should fail.
    let multiplexer = manager.getHTTP2Multiplexer()
    self.loop.run()
    XCTAssertThrowsError(try multiplexer.wait())
  }

  func testConnectFromIdleFailsWithNoReconnect() {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    let multiplexer: EventLoopFuture<HTTP2StreamMultiplexer> = self.waitForStateChange(from: .idle, to: .connecting) {
      let channel = manager.getHTTP2Multiplexer()
      self.loop.run()
      return channel
    }

    self.waitForStateChange(from: .connecting, to: .shutdown) {
      channelPromise.fail(DoomedChannelError())
    }

    XCTAssertThrowsError(try multiplexer.wait()) {
      XCTAssertTrue($0 is DoomedChannelError)
    }
  }

  func testConnectAndDisconnect() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    self.waitForStateChange(from: .idle, to: .connecting) {
      _ = manager.getHTTP2Multiplexer()
      self.loop.run()
    }

    // Setup the real channel and activate it.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    _ = channel.pipeline.addHandler(
      GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
      ))
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a settings frame on the root stream; this'll make the channel 'ready'.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // Close the channel.
    try self.waitForStateChange(from: .ready, to: .shutdown) {
      // Now the channel should be available: shut it down.
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testConnectAndIdle() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Setup the channel.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(
      GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
      )
    ).wait()
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a settings frame on the root stream; this'll make the channel 'ready'.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
      // Wait for the channel, it _must_ be ready now.
      XCTAssertNoThrow(try readyChannelMux.wait())
    }

    // Go idle. This will shutdown the channel.
    try self.waitForStateChange(from: .ready, to: .idle) {
      self.loop.advanceTime(by: .minutes(5))
        XCTAssertNoThrow(try channel.closeFuture.wait())
    }

    // Now shutdown.
    try self.waitForStateChange(from: .idle, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testIdleTimeoutWhenThereAreActiveStreams() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Setup the channel.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()

    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a settings frame on the root stream; this'll make the channel 'ready'.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
      // Wait for the channel multiplexer, it _must_ be ready now.
      XCTAssertNoThrow(try readyChannelMux.wait())
    }

    // "create" a stream; the details don't matter here.
    let streamCreated = NIOHTTP2StreamCreatedEvent(
      streamID: 1,
      localInitialWindowSize: nil,
      remoteInitialWindowSize: nil
    )
    channel.pipeline.fireUserInboundEventTriggered(streamCreated)

    // Wait for the idle timeout: this should _not_ cause the channel to idle.
    self.loop.advanceTime(by: .minutes(5))

    // Now we're going to close the stream and wait for an idle timeout and then shutdown.
    self.waitForStateChange(from: .ready, to: .idle) {
      // Close the stream.
      let streamClosed = StreamClosedEvent(streamID: 1, reason: nil)
      channel.pipeline.fireUserInboundEventTriggered(streamClosed)
      // ... wait for the idle timeout,
      self.loop.advanceTime(by: .minutes(5))
    }

    // Now shutdown.
    try self.waitForStateChange(from: .idle, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testConnectAndThenBecomeInactive() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Setup the channel.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    try self.waitForStateChange(from: .connecting, to: .shutdown) {
      // Okay: now close the channel; the `readyChannel` future has not been completed yet.
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // We failed to get a channel and we don't have reconnect configured: we should be shutdown and
    // the `readyChannelMux` should error.
    XCTAssertThrowsError(try readyChannelMux.wait())
  }

  func testConnectOnSecondAttempt() throws {
    let channelPromise: EventLoopPromise<Channel> = self.loop.makePromise()
    let channelFutures: [EventLoopFuture<Channel>] = [
      self.loop.makeFailedFuture(DoomedChannelError()),
      channelPromise.futureResult,
    ]
    var channelFutureIterator = channelFutures.makeIterator()

    var configuration = self.defaultConfiguration
    configuration.connectionBackoff = .oneSecondFixed

    let manager = ConnectionManager.testingOnly(configuration: configuration, logger: self.logger) {
      guard let next = channelFutureIterator.next() else {
        XCTFail("Too many channels requested")
        return self.loop.makeFailedFuture(DoomedChannelError())
      }
      return next
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self.waitForStateChanges([
      Change(from: .idle, to: .connecting),
      Change(from: .connecting, to: .transientFailure),
    ]) {
      // Get a channel multiplexer.
      let readyChannelMux = manager.getHTTP2Multiplexer()
      self.loop.run()
      return readyChannelMux
    }

    // Get a channel mux from the manager: it is a future for the same channel.
    let anotherReadyChannelMux = manager.getHTTP2Multiplexer()
    self.loop.run()

    // Move time forwards by a second to start the next connection attempt.
    self.waitForStateChange(from: .transientFailure, to: .connecting) {
      self.loop.advanceTime(by: .seconds(1))
    }

    // Setup the actual channel and complete the promise.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a SETTINGS frame on the root stream.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // Wait for the channel multiplexer, it _must_ be ready now.
    XCTAssertNoThrow(try readyChannelMux.wait())
    XCTAssertNoThrow(try anotherReadyChannelMux.wait())

    // Now shutdown.
    try self.waitForStateChange(from: .ready, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testShutdownWhileConnecting() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Now shutdown.
    try self.waitForStateChange(from: .connecting, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // The channel we were requesting should fail.
    XCTAssertThrowsError(try readyChannelMux.wait())

    // We still have our channel promise to fulfil: if it succeeds then it too should be closed.
    channelPromise.succeed(EmbeddedChannel(loop: self.loop))
    let channel = try channelPromise.futureResult.wait()
    self.loop.run()
    XCTAssertNoThrow(try channel.closeFuture.wait())
  }

  func testShutdownWhileTransientFailure() throws {
    var configuration = self.defaultConfiguration
    configuration.connectionBackoff = .oneSecondFixed

    let manager = ConnectionManager.testingOnly(configuration: configuration, logger: self.logger) {
      self.loop.makeFailedFuture(DoomedChannelError())
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self.waitForStateChanges([
      Change(from: .idle, to: .connecting),
      Change(from: .connecting, to: .transientFailure),
    ]) {
      // Get a channel multiplexer.
      let readyChannelMux = manager.getHTTP2Multiplexer()
      self.loop.run()
      return readyChannelMux
    }

    // Now shutdown.
    try self.waitForStateChange(from: .transientFailure, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // The channel mux we were requesting should fail.
    XCTAssertThrowsError(try readyChannelMux.wait())
  }

  func testShutdownWhileActive() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Prepare the channel
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // (No state change expected here: active is an internal state.)

    // Now shutdown.
    try self.waitForStateChange(from: .connecting, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // The channel multiplexer we were requesting should fail.
    XCTAssertThrowsError(try readyChannelMux.wait())
  }

  func testShutdownWhileShutdown() throws {
    let manager = ConnectionManager(configuration: self.defaultConfiguration, logger: self.logger)

    try self.waitForStateChange(from: .idle, to: .shutdown) {
      let firstShutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try firstShutdown.wait())
    }

    let secondShutdown = manager.shutdown()
    self.loop.run()
    XCTAssertNoThrow(try secondShutdown.wait())
  }

  /* PPA func testTransientFailureWhileActive() throws {
    var configuration = self.defaultConfiguration
    configuration.connectionBackoff = .oneSecondFixed

    let channelPromise: EventLoopPromise<Channel> = self.loop.makePromise()
    let channelFutures: [EventLoopFuture<Channel>] = [
      channelPromise.futureResult,
      self.loop.makeFailedFuture(DoomedChannelError()),
    ]
    var channelFutureIterator = channelFutures.makeIterator()

    let manager = ConnectionManager.testingOnly(configuration: configuration, logger: self.logger) {
      guard let next = channelFutureIterator.next() else {
        XCTFail("Too many channels requested")
        return self.loop.makeFailedFuture(DoomedChannelError())
      }
      return next
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Prepare the channel
    let firstChannel = EmbeddedChannel(
      handler: GRPCIdleHandler(
        mode: .client(manager, try readyChannelMux.wait()),
        logger: self.logger,
        idleTimeout: .minutes(5)
      ),
      loop: self.loop
    )
    channelPromise.succeed(firstChannel)
    XCTAssertNoThrow(
      try firstChannel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // (No state change expected here: active is an internal state.)

    // Close the channel (simulate e.g. TLS handshake failed)
    try self.waitForStateChange(from: .connecting, to: .transientFailure) {
      XCTAssertNoThrow(try firstChannel.close().wait())
    }

    // Start connecting again.
    self.waitForStateChanges([
      Change(from: .transientFailure, to: .connecting),
      Change(from: .connecting, to: .transientFailure),
    ]) {
      self.loop.advanceTime(by: .seconds(1))
    }

    // Now shutdown
    try self.waitForStateChange(from: .transientFailure, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // The channel never came up: it should be throw.
    XCTAssertThrowsError(try readyChannelMux.wait())
  } */

  func testTransientFailureWhileReady() throws {
    var configuration = self.defaultConfiguration
    configuration.connectionBackoff = .oneSecondFixed

    let firstChannelPromise: EventLoopPromise<Channel> = self.loop.makePromise()
    let secondChannelPromise: EventLoopPromise<Channel> = self.loop.makePromise()
    let channelFutures: [EventLoopFuture<Channel>] = [
      firstChannelPromise.futureResult,
      secondChannelPromise.futureResult,
    ]
    var channelFutureIterator = channelFutures.makeIterator()

    let manager = ConnectionManager.testingOnly(configuration: configuration, logger: self.logger) {
      guard let next = channelFutureIterator.next() else {
        XCTFail("Too many channels requested")
        return self.loop.makeFailedFuture(DoomedChannelError())
      }
      return next
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Prepare the first channel
    let firstChannel = EmbeddedChannel(handler: nil, loop: self.loop)
    let firstH2mux = HTTP2StreamMultiplexer(mode: .client, channel: firstChannel, inboundStreamInitializer: nil)
    try firstChannel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, firstH2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    firstChannelPromise.succeed(firstChannel)
    XCTAssertNoThrow(
      try firstChannel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a SETTINGS frame on the root stream.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try firstChannel.writeInbound(frame))
    }

    // Channel should now be ready.
    XCTAssertNoThrow(try readyChannelMux.wait())

    // Kill the first channel. But first ensure there's an active RPC, otherwise we'll idle.
    let streamCreated = NIOHTTP2StreamCreatedEvent(
      streamID: 1,
      localInitialWindowSize: nil,
      remoteInitialWindowSize: nil
    )
    firstChannel.pipeline.fireUserInboundEventTriggered(streamCreated)

    try self.waitForStateChange(from: .ready, to: .transientFailure) {
      XCTAssertNoThrow(try firstChannel.close().wait())
    }

    // Run to start connecting again.
    self.waitForStateChange(from: .transientFailure, to: .connecting) {
      self.loop.advanceTime(by: .seconds(1))
    }

    // Prepare the second channel
    let secondChannel = EmbeddedChannel(handler: nil, loop: self.loop)
    let secondH2mux = HTTP2StreamMultiplexer(mode: .client, channel: secondChannel, inboundStreamInitializer: nil)
    try secondChannel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, secondH2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    secondChannelPromise.succeed(secondChannel)
    XCTAssertNoThrow(
      try secondChannel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a SETTINGS frame on the root stream.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try secondChannel.writeInbound(frame))
    }

    // Now shutdown
    try self.waitForStateChange(from: .ready, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testGoAwayWhenReady() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Setup the channel.
    let channel = EmbeddedChannel(handler: nil, loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    channelPromise.succeed(channel)
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    try self.waitForStateChange(from: .connecting, to: .ready) {
      // Write a SETTINGS frame on the root stream.
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // Wait for the channel multiplexer, it _must_ be ready now.
    XCTAssertNoThrow(try readyChannelMux.wait())

    // Send a GO_AWAY; the details don't matter. This will cause the connection to go idle and the
    // channel to close.
    try self.waitForStateChange(from: .ready, to: .idle) {
      let goAway = HTTP2Frame(
        streamID: .rootStream,
        payload: .goAway(lastStreamID: 1, errorCode: .noError, opaqueData: nil)
      )
      XCTAssertNoThrow(try channel.writeInbound(goAway))
    }

    self.loop.run()
    XCTAssertNoThrow(try channel.closeFuture.wait())

    // Now shutdown
    try self.waitForStateChange(from: .idle, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }
  }

  func testDoomedOptimisticChannelFromIdle() {
    var configuration = self.defaultConfiguration
    configuration.callStartBehavior = .fastFailure
    let manager = ConnectionManager.testingOnly(
      configuration: configuration,
      logger: self.logger
    ) {
      self.loop.makeFailedFuture(DoomedChannelError())
    }
    let candidate = manager.getHTTP2Multiplexer()
    self.loop.run()
    XCTAssertThrowsError(try candidate.wait())
  }

  func testDoomedOptimisticChannelFromConnecting() throws {
    var configuration = self.defaultConfiguration
    configuration.callStartBehavior = .fastFailure
    let promise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: configuration,
      logger: self.logger
    ) {
      promise.futureResult
    }

    self.waitForStateChange(from: .idle, to: .connecting) {
      // Trigger channel creation, and a connection attempt, we don't care about the channel multiplexer.
      _ = manager.getHTTP2Multiplexer()
      self.loop.run()
    }

    // We're connecting: get an optimistic channel multiplexer.
    let optimisticChannelMux = manager.getHTTP2Multiplexer()
    self.loop.run()

    // Fail the promise.
    promise.fail(DoomedChannelError())

    XCTAssertThrowsError(try optimisticChannelMux.wait())
  }

  func testOptimisticChannelFromTransientFailure() throws {
    var configuration = self.defaultConfiguration
    configuration.callStartBehavior = .fastFailure
    configuration.connectionBackoff = ConnectionBackoff()

    let manager = ConnectionManager.testingOnly(configuration: configuration, logger: self.logger) {
      self.loop.makeFailedFuture(DoomedChannelError())
    }

    self.waitForStateChanges([
      Change(from: .idle, to: .connecting),
      Change(from: .connecting, to: .transientFailure),
    ]) {
      // Trigger channel creation, and a connection attempt, we don't care about the channel multiplexer.
      _ = manager.getHTTP2Multiplexer()
      self.loop.run()
    }

    // Now we're sitting in transient failure. Get a channel mux optimistically.
    let optimisticChannelMux = manager.getHTTP2Multiplexer()
    self.loop.run()

    XCTAssertThrowsError(try optimisticChannelMux.wait()) { error in
      XCTAssertTrue(error is DoomedChannelError)
    }
  }

  func testOptimisticChannelFromShutdown() throws {
    var configuration = self.defaultConfiguration
    configuration.callStartBehavior = .fastFailure
    let manager = ConnectionManager.testingOnly(
      configuration: configuration,
      logger: self.logger
    ) {
      self.loop.makeFailedFuture(DoomedChannelError())
    }

    let shutdown = manager.shutdown()
    self.loop.run()
    XCTAssertNoThrow(try shutdown.wait())

    // Get a channel optimistically. It'll fail, obviously.
    let channelMux = manager.getHTTP2Multiplexer()
    self.loop.run()
    XCTAssertThrowsError(try channelMux.wait())
  }

  /* PPA func testDoubleIdle() throws {
    class CloseDroppingHandler: ChannelOutboundHandler {
      typealias OutboundIn = Any
      func close(context: ChannelHandlerContext, mode: CloseMode,
                 promise: EventLoopPromise<Void>?) {
        promise?
          .fail(GRPCStatus(code: .unavailable, message: "Purposefully dropping channel close"))
      }
    }

    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self
      .waitForStateChange(from: .idle, to: .connecting) {
        let readyChannelMux = manager.getHTTP2Multiplexer()
        self.loop.run()
        return readyChannelMux
      }

    // Setup the real channel and activate it.
    let channel = EmbeddedChannel(loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    try channel.pipeline.addHandler(GRPCIdleHandler(
        mode: .client(manager, h2mux),
        logger: self.logger,
        idleTimeout: .minutes(5)
        )
    ).wait()
    XCTAssertNoThrow(try channel.pipeline.addHandlers([
      CloseDroppingHandler(),
      GRPCIdleHandler(mode: .client(manager, h2mux), logger: manager.logger, idleTimeout: .minutes(5)),
    ]).wait())
    channelPromise.succeed(channel)
    self.loop.run()
    XCTAssertNoThrow(
      try channel.connect(to: SocketAddress(unixDomainSocketPath: "/ignored"))
        .wait()
    )

    // Write a SETTINGS frame on the root stream.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // The channel mux should now be ready.
    XCTAssertNoThrow(try readyChannelMux.wait())

    // Send a GO_AWAY; the details don't matter. This will cause the connection to go idle and the
    // channel to close.
    try self.waitForStateChange(from: .ready, to: .idle) {
      let goAway = HTTP2Frame(
        streamID: .rootStream,
        payload: .goAway(lastStreamID: 1, errorCode: .noError, opaqueData: nil)
      )
      XCTAssertNoThrow(try channel.writeInbound(goAway))
    }

    // We dropped the close; now wait for the scheduled idle to fire.
    //
    // Previously doing this this would fail a precondition.
    self.loop.advanceTime(by: .minutes(5))
  }*/

  func testForceIdleAfterInactive() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    let readyChannelMux: EventLoopFuture<HTTP2StreamMultiplexer> = self.waitForStateChange(
      from: .idle,
      to: .connecting
    ) {
      let readyChannelMux = manager.getHTTP2Multiplexer()
      self.loop.run()
      return readyChannelMux
    }

    // Setup the real channel and activate it.
    let channel = EmbeddedChannel(loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    XCTAssertNoThrow(try channel.pipeline.addHandlers([
      GRPCIdleHandler(mode: .client(manager, h2mux), logger: manager.logger, idleTimeout: .minutes(5)),
    ]).wait())
    channelPromise.succeed(channel)
    self.loop.run()

    let connect = channel.connect(to: try SocketAddress(unixDomainSocketPath: "/ignored"))
    XCTAssertNoThrow(try connect.wait())

    // Write a SETTINGS frame on the root stream.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // The channel should now be ready.
    XCTAssertNoThrow(try readyChannelMux.wait())

    // Now drop the connection.
    try self.waitForStateChange(from: .ready, to: .shutdown) {
      let shutdown = manager.shutdown()
      self.loop.run()
      XCTAssertNoThrow(try shutdown.wait())
    }

    // Fire a connection idled event, i.e. keepalive timeout has fired. This should be a no-op.
    // Previously this would hit a precondition failure.
    channel.pipeline.fireUserInboundEventTriggered(ConnectionIdledEvent())
  }

  func testCloseWithoutActiveRPCs() throws {
    let channelPromise = self.loop.makePromise(of: Channel.self)
    let manager = ConnectionManager.testingOnly(
      configuration: self.defaultConfiguration,
      logger: self.logger
    ) {
      channelPromise.futureResult
    }

    // Start the connection.
    let readyChannelMux = self.waitForStateChange(
      from: .idle,
      to: .connecting
    ) { () -> EventLoopFuture<HTTP2StreamMultiplexer> in
        let readyChannelMux = manager.getHTTP2Multiplexer()
      self.loop.run()
      return readyChannelMux
    }

    // Setup the actual channel and activate it.
    let channel = EmbeddedChannel(loop: self.loop)
    let h2mux = HTTP2StreamMultiplexer(mode: .client, channel: channel, inboundStreamInitializer: nil)
    XCTAssertNoThrow(try channel.pipeline.addHandlers([
      GRPCIdleHandler(mode: .client(manager, h2mux), logger: manager.logger, idleTimeout: .minutes(5)),
    ]).wait())
    channelPromise.succeed(channel)
    self.loop.run()

    let connect = channel.connect(to: try SocketAddress(unixDomainSocketPath: "/ignored"))
    XCTAssertNoThrow(try connect.wait())

    // "ready" the connection.
    try self.waitForStateChange(from: .connecting, to: .ready) {
      let frame = HTTP2Frame(streamID: .rootStream, payload: .settings(.settings([])))
      XCTAssertNoThrow(try channel.writeInbound(frame))
    }

    // The channel multiplexer should now be ready.
    XCTAssertNoThrow(try readyChannelMux.wait())

    // Close the channel. There are no active RPCs so we should idle rather than be in the transient
    // failure state.
    self.waitForStateChange(from: .ready, to: .idle) {
      channel.pipeline.fireChannelInactive()
    }
  }
}

internal struct Change: Hashable, CustomStringConvertible {
  var from: ConnectivityState
  var to: ConnectivityState

  var description: String {
    return "\(self.from) → \(self.to)"
  }
}

internal class RecordingConnectivityDelegate: ConnectivityStateDelegate {
  private let serialQueue = DispatchQueue(label: "io.grpc.testing")
  private let semaphore = DispatchSemaphore(value: 0)
  private var expectation: Expectation = .noExpectation

  private enum Expectation {
    /// We have no expectation of any changes. We'll just ignore any changes.
    case noExpectation

    /// We expect one change.
    case one((Change) -> Void)

    /// We expect 'count' changes.
    case some(count: Int, recorded: [Change], ([Change]) -> Void)

    var count: Int {
      switch self {
      case .noExpectation:
        return 0
      case .one:
        return 1
      case let .some(count, _, _):
        return count
      }
    }
  }

  func connectivityStateDidChange(from oldState: ConnectivityState,
                                  to newState: ConnectivityState) {
    self.serialQueue.async {
      switch self.expectation {
      case let .one(verify):
        // We don't care about future changes.
        self.expectation = .noExpectation

        // Verify and notify.
        verify(Change(from: oldState, to: newState))
        self.semaphore.signal()

      case .some(let count, var recorded, let verify):
        recorded.append(Change(from: oldState, to: newState))
        if recorded.count == count {
          // We don't care about future changes.
          self.expectation = .noExpectation

          // Verify and notify.
          verify(recorded)
          self.semaphore.signal()
        } else {
          // Still need more responses.
          self.expectation = .some(count: count, recorded: recorded, verify)
        }

      case .noExpectation:
        // Ignore any changes.
        ()
      }
    }
  }

  func expectChanges(_ count: Int, verify: @escaping ([Change]) -> Void) {
    self.serialQueue.async {
      self.expectation = .some(count: count, recorded: [], verify)
    }
  }

  func expectChange(verify: @escaping (Change) -> Void) {
    self.serialQueue.async {
      self.expectation = .one(verify)
    }
  }

  func waitForExpectedChanges(
    timeout: DispatchTimeInterval,
    file: StaticString = #file,
    line: UInt = #line
  ) {
    let result = self.semaphore.wait(timeout: .now() + timeout)
    switch result {
    case .success:
      ()
    case .timedOut:
      XCTFail(
        "Timed out before verifying \(self.expectation.count) change(s)",
        file: file, line: line
      )
    }
  }
}

private extension ConnectionBackoff {
  static let oneSecondFixed = ConnectionBackoff(
    initialBackoff: 1.0,
    maximumBackoff: 1.0,
    multiplier: 1.0,
    jitter: 0.0
  )
}

private struct DoomedChannelError: Error {}
