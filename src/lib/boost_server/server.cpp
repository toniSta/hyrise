#include "server.hpp"

#include <boost/thread.hpp>
#include <iostream>
#include "tpch/tpch_table_generator.hpp"

#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"

namespace opossum {

Server::Server(const uint16_t port)
    : _socket(_io_service), _acceptor(_io_service, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)) {

    }

void Server::_accept_new_session() {
  auto start_session = boost::bind(&Server::_start_session, this, boost::asio::placeholders::error);
  _acceptor.async_accept(_socket, start_session);
}

void Server::_start_session(boost::system::error_code error) {
  if (!error) {
    boost::thread session_thread([=] {
      // Sockets cannot be copied. After moving the _socket object the object will be in the same state as before.
      auto session = Session(std::move(_socket));
      session.start();
    });
    session_thread.detach();
  }
  _accept_new_session();
}

void Server::run() {
  TpchTableGenerator{0.01f, 100'000}.generate_and_store();

 const std::string sql = "CREATE TABLE lineitem1024 (L_ORDERKEY INTEGER NOT NULL,L_PARTKEY INTEGER NOT NULL,L_SUPPKEY INTEGER NOT NULL,L_LINENUMBER INTEGER,L_QUANTITY float,L_EXTENDEDPRICE float,L_DISCOUNT float,L_TAX float,L_RETURNFLAG VARCHAR(1),L_LINESTATUS VARCHAR(1),L_SHIPDATE VARCHAR(10),L_COMMITDATE VARCHAR(10),L_RECEIPTDATE VARCHAR(10),L_SHIPINSTRUCT VARCHAR(25),L_SHIPMODE VARCHAR(10),L_COMMENT VARCHAR(44)); insert into lineitem1024 select top 1024 * from lineitem;";

   auto sql_pipeline = std::make_shared<SQLPipeline>(SQLPipelineBuilder{sql}.create_pipeline());
  sql_pipeline->get_result_table();
  _accept_new_session();
  std::cout << "Server running on port " << get_port() << std::endl;
  _io_service.run();
}

void Server::shutdown() { _io_service.stop(); }

uint16_t Server::get_port() const { return _acceptor.local_endpoint().port(); }
}  // namespace opossum
