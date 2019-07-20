#include <boost/asio/io_service.hpp>

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "server/server.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"
#include "tpch/tpch_table_generator.hpp"



#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"

int main(int argc, char* argv[]) {
  uint16_t port = 5432;

  if (argc >= 2) {
    char* endptr{nullptr};
    errno = 0;
    auto port_long = std::strtol(argv[1], &endptr, 10);
    Assert(errno == 0 && port_long != 0 && port_long <= 65535 && *endptr == 0, "invalid port number");
    port = static_cast<uint16_t>(port_long);
  }

  // Set scheduler so that the server can execute the tasks on separate threads.
  opossum::CurrentScheduler::set(std::make_shared<opossum::NodeQueueScheduler>());

  boost::asio::io_service io_service;

  // The server registers itself to the boost io_service. The io_service is the main IO control unit here and it lives
  // until the server doesn't request any IO any more, i.e. is has terminated. The server requests IO in its
  // constructor and then runs forever.
  opossum::Server server{io_service, port};
  opossum::TpchTableGenerator{0.01f, 100'000}.generate_and_store();
     const std::string sql = "CREATE TABLE lineitem1024 (L_ORDERKEY INTEGER NOT NULL,L_PARTKEY INTEGER NOT NULL,L_SUPPKEY INTEGER NOT NULL,L_LINENUMBER INTEGER,L_QUANTITY float,L_EXTENDEDPRICE float,L_DISCOUNT float,L_TAX float,L_RETURNFLAG VARCHAR(1),L_LINESTATUS VARCHAR(1),L_SHIPDATE VARCHAR(10),L_COMMITDATE VARCHAR(10),L_RECEIPTDATE VARCHAR(10),L_SHIPINSTRUCT VARCHAR(25),L_SHIPMODE VARCHAR(10),L_COMMENT VARCHAR(44)); insert into lineitem1024 select top 1024 * from lineitem;";

   auto sql_pipeline = std::make_shared<opossum::SQLPipeline>(opossum::SQLPipelineBuilder{sql}.create_pipeline());
  sql_pipeline->get_result_table();
  io_service.run();

  return 0;
}
