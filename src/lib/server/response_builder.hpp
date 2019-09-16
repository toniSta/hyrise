#pragma once

#include <memory>
#include "operators/abstract_operator.hpp"
#include "postgres_protocol_handler.hpp"
#include "storage/table.hpp"

namespace opossum {

class ResponseBuilder {
 public:
  static void build_row_description(std::shared_ptr<const Table> table,
                                    std::shared_ptr<PostgresProtocolHandler> postgres_protocol_handler);

  static std::string build_command_complete_message(const OperatorType root_operator_type, const uint64_t row_count);
};

}  // namespace opossum