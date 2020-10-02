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

import ArgumentParser
import Logging
import Lifecycle

final class QPSWorkerApp: ParsableCommand {
    /*
     DEFINE_int32(driver_port, 0, "Port for communication with driver");
     DEFINE_int32(server_port, 0, "Port for operation as a server");
     DEFINE_string(credential_type, grpc::testing::kInsecureCredentialsType,
                   "Credential type for communication with driver");
     */

    @Option(name: .customLong("driver_port"), help: "Port for communication with driver.")
    var driverPort: Int

    @Option(name: .customLong("server_port"), help: "Port for operation as a server.")
    var serverPort: Int?

    @Option(name: .customLong("credential_type"), help: "Credential type for communication with driver.")
    var credentialType: String = "boo"  // TODO:  Default to kInsecureCredentialsType

    func run() throws {
        let logger = Logger(label: "QPSWorker")
        logger.info("Starting...")
        /*
         gpr_log(GPR_DEBUG,
                 "test slowdown factor: sanitizer=%" PRId64 ", fixture=%" PRId64
                 ", poller=%" PRId64 ", total=%" PRId64,
                 grpc_test_sanitizer_slowdown_factor(), g_fixture_slowdown_factor,
                 g_poller_slowdown_factor, grpc_test_slowdown_factor());
         */

        logger.info("Initializing the lifecycle container")
        // This installs backtrace.
        let lifecycle = ServiceLifecycle()






        // TODO:  Seed rng
        /*
         /* seed rng with pid, so we don't end up with the same random numbers as a
            concurrently running test binary */
         srand(seed());
         */

        let qpsWorker = QPSWorker(driverPort: self.driverPort,
                                  serverPort: self.serverPort)
                                 // credentialType: self.credentialType)
        qpsWorker.start()
        // TODO:  Investigate eventloop future compatibility mode.
        lifecycle.registerShutdown(label: "QPSWorker", .sync {
            () in try qpsWorker.syncShutdown()
        })

        lifecycle.start { error in
            // start completion handler.
            // if a startup error occurred you can capture it here
            if let error = error {
                logger.error("failed starting \(self) ☠️: \(error)")
            } else {
                logger.info("\(self) started successfully 🚀")
            }
        }

        // Termination from internally.
        try qpsWorker.wait()

        logger.info("Worker has finished.")

        // lifecycle.wait()
        // Wait on the server's `onClose` future to stop the program from exiting.
        // _ = try qpsWorker.server?.flatMap {
           //  $0.onClose
        // }.wait()
    }
}

QPSWorkerApp.main()




/*
 void grpc_test_init(int /*argc*/, char** /*argv*/) {
   install_crash_handler();


 }
 */

/*
 QpsWorker worker(FLAGS_driver_port, FLAGS_server_port, FLAGS_credential_type);

   while (!got_sigint && !worker.Done()) {
     gpr_sleep_until(gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                  gpr_time_from_seconds(5, GPR_TIMESPAN)));
   }
 */
