#include "cxxopts.hpp"

#include "hyrise.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "server/server.hpp"

cxxopts::Options get_server_cli_options() {
  cxxopts::Options cli_options("./hyriseServer", "Starts Hyrise Server in order to accept network requests.");

  // clang-format off
  cli_options.add_options()
    ("help", "Display this help and exit")
    ("p,port", "Specify the port number. 0 means randomly select an available one", cxxopts::value<uint16_t>()->default_value("5432"))  //NOLINT
    ("execution_info", "Send execution information after statement execution", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ;  //NOLINT
  // clang-format on

  return cli_options;
}

int main(int argc, char* argv[]) {
  auto cli_options = get_server_cli_options();
  const auto parsed_options = cli_options.parse(argc, argv);

  // Print help and exit
  if (parsed_options.count("help")) {
    std::cout << cli_options.help() << std::endl;
    return 0;
  }

  // Set scheduler so that the server can execute the tasks on separate threads.
  opossum::Hyrise::get().set_scheduler(std::make_shared<opossum::NodeQueueScheduler>());

  const auto execution_info = parsed_options["execution_info"].as<bool>();
  const auto port = parsed_options["port"].as<uint16_t>();

  auto server = opossum::Server{port, execution_info};
  server.run();

  return 0;
}
