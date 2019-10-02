#pragma once

#include "hyrise.hpp"
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace opossum {

struct ExecutionInformation {
  std::shared_ptr<const Table> result_table;
  OperatorType root_operator;
  std::string execution_information;
  std::string error;
};

// This class manages the interaction between the server and the database component. Furthermore, most of the SQL-based
// error handling happens in this class.
class HyriseCommunicator {
 public:
  static ExecutionInformation execute_pipeline(const std::string& sql, const bool debug_note);

  static std::optional<std::string> setup_prepared_plan(const std::string& statement_name, const std::string& query);

  static std::pair<std::string, std::shared_ptr<AbstractOperator>> bind_prepared_plan(
      const PreparedStatementDetails& statement_details);

  static std::shared_ptr<TransactionContext> get_new_transaction_context();

  static std::shared_ptr<const Table> execute_prepared_statement(
      const std::shared_ptr<AbstractOperator>& physical_plan);
};

}  // namespace opossum
