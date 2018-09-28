#include "typed_operator_base_test.hpp"

#include "operators/operator_scan_predicate.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"

namespace opossum {

class TableScanBetweenTest : public TypedOperatorBaseTest {  
protected:
  std::shared_ptr<AbstractOperator> _data_table_wrapper;

  void SetUp() override {
    // For the test, we create a table with the data type that is to be scanned as the first column and a control int
    // in the second column:
    //
    // a<DataType>  b<int>
    // 10.2         0
    // 12.2         1
    // 14.2 / NULL  2       (each third row is nulled if the table is marked as nullable)
    // 16.2         3
    // ...
    // 30.2         10
    //
    // As the first column is type casted, it contains 10 for an int column, the string "10.2" for a string column etc.

    const auto& [data_type, encoding, nullable] = GetParam();

    auto column_definitions = TableColumnDefinitions{{"a", data_type, nullable}, {"b", DataType::Int, nullable}};

    const auto data_table = std::make_shared<Table>(column_definitions, TableType::Data, 6);

    // `nullable=nullable` is a dirty hack to work around C++ defect 2313.
    resolve_data_type(data_type, [&, nullable=nullable](const auto type) {
      using DataType = typename decltype(type)::type;
      for (auto i = 0; i <= 10; ++i) {
        auto value = type_cast<DataType>(10.2 + i * 2.0);
        if (nullable && i % 3 == 2) {
          data_table->append({value, NullValue{}});
        } else {
          data_table->append({value, i});
        }
      }
    });

    // We have two full chunks and one open chunk, we only encode the full chunks
    for (auto chunk_id = ChunkID{0}; chunk_id < 2; ++chunk_id) {
      ChunkEncoder::encode_chunk(data_table->get_chunk(chunk_id), {data_type, DataType::Int},
                                 {encoding, EncodingType::Unencoded});
    }

    _data_table_wrapper = std::make_shared<TableWrapper>(data_table);
    _data_table_wrapper->execute();
  }
};

TEST_P(TableScanBetweenTest, ExactBoundaries) {
  auto tests = std::vector<std::tuple<AllParameterVariant, AllParameterVariant, std::vector<int>>>{
    {12.2, 16.2, {1, 2, 3}},                           // Both boundaries exact match
    {12.0, 16.2, {1, 2, 3}},                           // Left boundary open match
    {12.2, 16.5, {1, 2, 3}},                           // Right boundary open match
    {12.0, 16.5, {1, 2, 3}},                           // Both boundaries open match
    {0.0, 16.5,  {0, 1, 2, 3}},                        // Left boundary before first value
    {16.0, 50.5, {3, 4, 5, 6, 7, 8, 9, 10}},           // Right boundary after last value
    {0.2, 50.5,  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},  // Matching all value
    {0.2, 0.5,   {}}                                   // Matching no value
  };

  const auto& [data_type, encoding, nullable] = GetParam();
  resolve_data_type(data_type, [&](const auto type) {
    for (const auto test : tests) {
      const auto predicate = OperatorScanPredicate{ColumnID{0}, PredicateCondition::Between, std::get<0>(test), std::get<1>(test)};
      auto scan = std::make_shared<TableScan>(_data_table_wrapper, predicate);
      scan->execute();

      const auto& result_table = *scan->get_output();
      // TODO Compare vector
    }
  });
}

TEST_F(TableScanBetweenTest, MismatchingTypes) {}
TEST_F(TableScanBetweenTest, NullValueAsParameter) {}

INSTANTIATE_TEST_CASE_P(TableScanBetweenTestInstances, TableScanBetweenTest, testing::ValuesIn(create_param_pairs()),
                        TypedOperatorBaseTest::format);

}  // namespace opossum
