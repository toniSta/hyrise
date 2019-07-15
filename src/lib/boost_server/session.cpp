#include "session.hpp"

#include <iostream>

#include "hyrise_communication.cpp"
#include "network_message_types.hpp"
#include "postgres_handler.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "concurrency/transaction_manager.hpp"
#include "tasks/server/execute_prepared_statement_task.hpp"

#include <pthread.h>
#include <sched.h>
#include <unistd.h>

namespace opossum {

Session::Session(Socket socket) : _socket(std::make_shared<Socket>(std::move(socket))), _postgres_handler(_socket) {
        boost::asio::ip::tcp::no_delay tcp_no_delay(true);
        _socket->set_option(tcp_no_delay);
  
  int node = static_cast<int>(TONI_NUMA) * 15;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(node, &cpuset);
  auto rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    // This is not an Assert(), though maybe it should be. Not being able to pin the threads doesn't make the DB
    // unfunctional, but probably slower
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << std::endl;
}
  }

void Session::start() {
  _establish_connection();
  while (!_terminate_session) {
    _handle_request();
  }
}

void Session::_establish_connection() {
  const auto body_size = _postgres_handler.read_startup_packet();

  // Currently, the information available in the start up packet body (such as
  // db name, user name) is ignored
  _postgres_handler.handle_startup_packet_body(body_size);
  _postgres_handler.send_authentication();
  _postgres_handler.send_parameter("server_version", "9.5");
  _postgres_handler.send_ready_for_query();
}

void Session::_handle_request() {
  const auto header = _postgres_handler.get_packet_type();

  switch (header) {
    case NetworkMessageType::TerminateCommand: {
      _terminate_session = true;
      break;
    }
    case NetworkMessageType::SimpleQueryCommand: {
      _handle_simple_query();
      break;
    }
    case NetworkMessageType::ParseCommand: {
      _handle_parse_command();
      break;
    }
    case NetworkMessageType::SyncCommand: {
      _sync();
      break;
    }
    case NetworkMessageType::BindCommand: {
      _handle_bind_command();
      break;
    }
    case NetworkMessageType::DescribeCommand: {
      _handle_describe();
      break;
    }
    case NetworkMessageType::FlushCommand: {
      break;
    }
    case NetworkMessageType::ExecuteCommand: {
      _handle_execute();
      break;
    }
    default:
      Fail("Unknown packet type");
  }
}

void Session::_handle_simple_query() {
  const auto& query = _postgres_handler.read_query_packet();

  // A simple query command invalidates unnamed statements and portals
  if (StorageManager::get().has_prepared_plan("")) StorageManager::get().drop_prepared_plan("");
  _portals.erase("");

  auto sql_pipeline = execute_pipeline(query);
  
  auto row_description = build_row_description(sql_pipeline->get_result_table());
  auto row_count = 0u;
  if (!row_description.empty()) {
    _postgres_handler.send_row_description(row_description);
    row_count = send_query_response(sql_pipeline->get_result_table(), _postgres_handler);
  }
  _postgres_handler.command_complete(build_command_complete_message(sql_pipeline->get_physical_plans().front(), row_count));
  _postgres_handler.send_ready_for_query();
}

void Session::_handle_parse_command() {
  const auto [statement_name, query] = _postgres_handler.read_parse_packet();

  // TODO(toni): better name for this method
  setup_prepared_plan(statement_name, query);

  _postgres_handler.send_status_message(NetworkMessageType::ParseComplete);
}


void Session::_handle_bind_command() {
  const auto parameters = _postgres_handler.read_bind_packet();
      // Not using Assert() since it includes file:line info that we don't want to hard code in tests
  AssertInput(StorageManager::get().has_prepared_plan(parameters.statement_name),
              "The specified statement does not exist.");

  const auto prepared_plan = StorageManager::get().get_prepared_plan(parameters.statement_name);

  if (parameters.statement_name.empty()) StorageManager::get().drop_prepared_plan(parameters.statement_name);


  // Named portals must be explicitly closed before they can be redefined by another Bind message,
  // but this is not required for the unnamed portal.
  // https://www.postgresql.org/docs/10/static/protocol-flow.html
  auto portal_it = _portals.find(parameters.portal);
  if (portal_it != _portals.end()) {
    // Not using Assert() since it includes file:line info that we don't want to hard code in tests
    AssertInput(parameters.portal.empty(), "Named portals must be explicitly closed before they can be redefined.");
    _portals.erase(portal_it);
  }
  // TODO(toni): aargh names
  auto physical_plan = bind_plan(prepared_plan, parameters.parameters);
  _portals.emplace(parameters.portal, physical_plan);

  _postgres_handler.send_status_message(NetworkMessageType::BindComplete);
}

void Session::_handle_describe() {
  _postgres_handler.read_describe_packet();
}

void Session::_sync() {
  _postgres_handler.read_sync_packet();
   if (_transaction) {
    _transaction->commit();
    _transaction.reset();
  }
  _postgres_handler.send_ready_for_query();
}

void Session::_handle_execute() {
  const std::string& portal_name = _postgres_handler.read_execute_packet();
  auto portal_it = _portals.find(portal_name);
  Assert(portal_it != _portals.end(), "The specified portal does not exist.");

  const auto physical_plan = portal_it->second;

  if (portal_name.empty()) _portals.erase(portal_it);

  if (!_transaction) _transaction = TransactionManager::get().new_transaction_context();
  physical_plan->set_transaction_context_recursively(_transaction);


  auto task = std::make_shared<ExecutePreparedStatementTask>(physical_plan);
  task->execute();
  auto table = task->get_result_table();

  auto row_description = build_row_description(table);
  auto row_count = 0u;
  if (!row_description.empty()) {
    _postgres_handler.send_row_description(row_description);
    row_count = send_query_response(table, _postgres_handler);
  } else {
    _postgres_handler.send_status_message(NetworkMessageType::NoDataResponse);
  }
  _postgres_handler.command_complete(build_command_complete_message(physical_plan, row_count));
}
}  // namespace opossum
