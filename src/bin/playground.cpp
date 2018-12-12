#include <iostream>
#include <readline/readline.h>

#include "console/pagination.hpp"
#include "operators/print.hpp"
#include "optimizer/optimizer.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_translator.hpp"
#include "SQLParser.h"
#include "types.hpp"


using namespace opossum;  // NOLINT

void table_out(const std::shared_ptr<const Table>& table, uint32_t flags = 0) {
  int size_y, size_x;
  rl_get_screen_size(&size_y, &size_x);

  const bool fits_on_one_page = table->row_count() < static_cast<uint64_t>(size_y) - 1;

  static bool pagination_disabled = false;
  if (!fits_on_one_page && !std::getenv("TERM") && !pagination_disabled) {
    std::cout << "Your TERM environment variable is not set - most likely because you are running the console from an IDE. "
        "Pagination is disabled.\n\n";
    pagination_disabled = true;
  }

  // Paginate only if table has more rows that fit in the terminal
  if (fits_on_one_page || pagination_disabled) {
    Print::print(table, flags, std::cout);
  } else {
    std::stringstream stream;
    Print::print(table, flags, stream);
    Pagination(stream).display();
  }
}

void benchmark(const std::string &sql, const bool print_table) {
  auto builder = SQLPipelineBuilder{sql};
  auto _sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());

  _sql_pipeline->get_result_tables();
  Assert(!_sql_pipeline->failed_pipeline_statement(),
         "The transaction has failed. This should never happen in the console, where only one statement gets "
         "executed at a time.");

  const auto& table = _sql_pipeline->get_result_table();

  // Print result
  if (print_table && table) {
    table_out(table);
  }
}



int main(int argc, char const *argv[]) {
  const auto print_table = true;
  const auto repeats = 10'000;
  auto i = 0;
  while(i < repeats) {
    auto start = std::chrono::high_resolution_clock::now();
    benchmark("select 1;", print_table);
    auto end = std::chrono::high_resolution_clock::now();
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << ">" <<  us.count() << "<" << std::endl;
    ++i;
  }
  return 0;
}
