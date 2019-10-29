#pragma once

#include "concurrency/transaction_context.hpp"
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "scheduler/operator_task.hpp"

namespace opossum {

// The session class implements the communication flow and stores session-specific information such as portals.
class Session {
 public:
  explicit Session(boost::asio::io_service& io_service, const SendExecutionInfo send_execution_info);

  // Start new session.
  void run();

  std::shared_ptr<Socket> get_socket();

 private:
  // Establish new connection by exchanging parameters.
  void _establish_connection();

  // Determine message and call the appropriate method.
  void _handle_request();

  // Execute plain SQL statement.
  void _handle_simple_query();

  // Parse prepared statement.
  void _handle_parse_command();

  // Bind prepared statement.
  void _handle_bind_command();

  // Read describe message. Row description will be send after execution.
  void _handle_describe();

  // Execute prepared statement and send row description.
  void _handle_execute();

  // Commit current transaction.
  void _sync();

  const std::shared_ptr<Socket> _socket;
  const std::shared_ptr<PostgresProtocolHandler<Socket>> _postgres_protocol_handler;
  const SendExecutionInfo _send_execution_info;
  bool _terminate_session = false;
  bool _sync_send_after_error = false;
  std::shared_ptr<TransactionContext> _transaction;
  std::unordered_map<std::string, std::shared_ptr<AbstractOperator>> _portals;
};
}  // namespace opossum
