#include "gtest/gtest.h"

#include <optional>

#include "expression/array_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/extract_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/expression_factory.hpp"
#include "expression/function_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_placeholder_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_a = load_table("src/test/tables/expression_evaluator/input_a.tbl");
    chunk_a = table_a->get_chunk(ChunkID{0});
    evaluator.emplace(chunk_a);

    a = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "a"));
    b = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "b"));
    c = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "c"));
    d = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "d"));
    s1 = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "s1"));
    s2 = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "s2"));
    dates = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_a, "dates"));
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);
    a_plus_c = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, c);
    s1_gt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, s1, s2);
    s1_lt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, s1, s2);
    a_lt_b = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, b);
    a_lt_c = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, c);

    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl");
    x = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_b, "x"));

    table_bools = load_table("src/test/tables/expression_evaluator/input_bools.tbl");
    chunk_bools = table_bools->get_chunk(ChunkID{0});
    bool_a = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_bools, "a"));
    bool_b = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_bools, "b"));
    bool_c = std::make_shared<PQPColumnExpression>(PQPColumnExpression::from_table(*table_bools, "c"));
    evaluator_bools.emplace(chunk_bools);
  }

  /**
   * Turn an ExpressionResult<T> into a canoncial form "std::vector<std::optional<T>>" to make the writing of tests
   * easier.
   */
  template<typename T>
  std::vector<std::optional<T>> normalize_expression_result(const ExpressionResult<T> &result) {

    if (result.type() == typeid(NullValue)) {
      return {{std::nullopt,}};

    } else if (result.type() == typeid(NullableValue<T>)) {
      const auto& nullable_value = boost::get<NullableValue<T>>(result);

      if (nullable_value) {
        return {{*nullable_value}};
      } else {
        return {{std::nullopt}};
      }

    } else if (result.type() == typeid(NullableValues<T>)) {
      const auto& nullable_values = boost::get<NullableValues<T>>(result);

      std::vector<std::optional<T>> normalized_result(nullable_values.first.size());

      for (auto chunk_offset = ChunkOffset{0}; chunk_offset < nullable_values.first.size(); ++chunk_offset) {
        if (nullable_values.second[chunk_offset]) {
          normalized_result[chunk_offset] = std::nullopt;
        } else {
          normalized_result[chunk_offset] = nullable_values.first[chunk_offset];
        }
      }

      return normalized_result;

    } else if (result.type() == typeid(NonNullableValues<T>)) {
      const auto& non_nullable_values = boost::get<NonNullableValues<T>>(result);

      std::vector<std::optional<T>> normalized_result(non_nullable_values.begin(), non_nullable_values.end());

      return normalized_result;

    } else {
      Fail("Can't normalize this ExpressionType");

    }
  }

  template<typename R>
  bool test_expression(const std::shared_ptr<Chunk>& chunk,
                       const AbstractExpression& expression,
                       const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator{chunk}.evaluate_expression<R>(expression);
    const auto actual_normalized = normalize_expression_result(actual_result);
    return actual_normalized == expected;
  }

  template<typename R>
  bool test_expression(const AbstractExpression& expression,
                       const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator{}.evaluate_expression<R>(expression);
    const auto actual_normalized = normalize_expression_result(actual_result);
    return actual_normalized == expected;
  }

  std::shared_ptr<Table> table_a, table_b, table_bools;
  std::shared_ptr<Chunk> chunk_a, chunk_bools;
  std::optional<ExpressionEvaluator> evaluator;
  std::optional<ExpressionEvaluator> evaluator_bools;

  std::shared_ptr<PQPColumnExpression> a, b, c, d, s1, s2, dates, x, bool_a, bool_b, bool_c;
  std::shared_ptr<ArithmeticExpression> a_plus_b;
  std::shared_ptr<ArithmeticExpression> a_plus_c;
  std::shared_ptr<BinaryPredicateExpression> a_lt_b;
  std::shared_ptr<BinaryPredicateExpression> a_lt_c;
  std::shared_ptr<BinaryPredicateExpression> s1_gt_s2;
  std::shared_ptr<BinaryPredicateExpression> s1_lt_s2;
};

TEST_F(ExpressionEvaluatorTest, TernaryOrNull) {
  const auto passed = test_expression<int32_t>(*or_(NullValue{}, NullValue{}), {std::nullopt});
  EXPECT_TRUE(passed);
}

TEST_F(ExpressionEvaluatorTest, TernaryOrValue) {
  const auto passed = test_expression<int32_t>(*or_(1, NullValue{}), {1});
  EXPECT_TRUE(passed);
}

TEST_F(ExpressionEvaluatorTest, TernaryOrNonNull) {
  const auto passed = test_expression<int32_t>(chunk_bools, *or_(bool_a, bool_b),
                                               {0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1});

  EXPECT_TRUE(passed);
}

TEST_F(ExpressionEvaluatorTest, TernaryOrNullable) {
  const auto passed = test_expression<int32_t>(chunk_bools, *or_(bool_a, bool_c),
                                               {0, 1, std::nullopt, 0, 1, std::nullopt, 1, 1, 1, 1, 1, 1});

  EXPECT_TRUE(passed);
}

//TEST_F(ExpressionEvaluatorTest, ArithmeticExpression) {
//  const auto expected_result = std::vector<int32_t>({3, 5, 7, 9});
//  EXPECT_EQ(boost::get<std::vector<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_b)), expected_result);
//}
//
//TEST_F(ExpressionEvaluatorTest, ArithmeticExpressionWithNull) {
//  const auto actual_result = boost::get<NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_c));
//  const auto& actual_values = actual_result.first;
//  const auto& actual_nulls = actual_result.second;
//
//  ASSERT_EQ(actual_values.size(), 4u);
//  EXPECT_EQ(actual_values.at(0), 34);
//  EXPECT_EQ(actual_values.at(2), 37);
//
//  std::vector<bool> expected_nulls = {false, true, false, true};
//  EXPECT_EQ(actual_nulls, expected_nulls);
//}
//
//TEST_F(ExpressionEvaluatorTest, GreaterThanWithStrings) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*s1_gt_s2));
//
//  std::vector<int32_t> expected_values = {0, 0, 1, 0};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThanWithStrings) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*s1_lt_s2));
//
//  std::vector<int32_t> expected_values = {1, 1, 0, 0};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThan) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_lt_b));
//
//  std::vector<int32_t> expected_values = {1, 1, 1, 1};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThanWithNulls) {
//  const auto actual_result = boost::get<NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_lt_c));
//  const auto& actual_values = actual_result.first;
//  const auto& actual_nulls = actual_result.second;
//
//  ASSERT_EQ(actual_values.size(), 4u);
//  EXPECT_TRUE(actual_values.at(0));
//  EXPECT_TRUE(actual_values.at(2));
//
//  std::vector<bool> expected_nulls = {false, true, false, true};
//  EXPECT_EQ(actual_nulls, expected_nulls);
//}

TEST_F(ExpressionEvaluatorTest, In) {
  const auto passed = test_expression<int32_t>(chunk_a, *in(a, array(1.0, 3.0)),
                                               {1, 0, 1, 0});

  EXPECT_TRUE(passed);
}

//TEST_F(ExpressionEvaluatorTest, Case) {
//  /**
//   * SELECT
//   *    CASE a = 2 THEN b
//   *    CASE a > 3 THEN c
//   *    ELSE NULL
//   * FROM
//   *    table_a
//   */
//  const auto else_ = std::make_shared<ValueExpression>(NullValue{});
//  const auto a_eq_2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals,
//                                                                  a, std::make_shared<ValueExpression>(2));
//  const auto a_ge_3 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThanEquals,
//                                                                  a, std::make_shared<ValueExpression>(3));
//  const auto case_a_ge_3 = std::make_shared<CaseExpression>(a_ge_3, c, else_);
//
//  const auto case_a_eq_2 = std::make_shared<CaseExpression>(a_eq_2, b, case_a_ge_3);
//
//  EXPECT_EQ(case_a_ge_3->data_type(), DataType::Int);
//  EXPECT_EQ(case_a_eq_2->data_type(), DataType::Int);
//  EXPECT_TRUE(case_a_ge_3->is_nullable());
//  EXPECT_TRUE(case_a_eq_2->is_nullable());
//
//  const auto actual_result = evaluator->evaluate_expression<int32_t>(*case_a_eq_2);
//  const auto& actual_nullable_values = boost::get<NullableValues<int32_t>>(actual_result);
//  const auto& actual_values = actual_nullable_values.first;
//  const auto& actual_nulls = actual_nullable_values.second;
//
//  std::vector<bool> expected_nulls = {true, false, false, true};
//  EXPECT_EQ(actual_nulls, expected_nulls);
//
//  EXPECT_EQ(actual_values.at(1), 3);
//  EXPECT_EQ(actual_values.at(2), 34);
//}
//
//TEST_F(ExpressionEvaluatorTest, Exists) {
//  /**
//   * Test a co-related EXISTS query
//   *
//   * SELECT
//   *    EXISTS (SELECT a + x FROM table_b WHERE a + b = 13)
//   * FROM
//   *    table_a;
//   */
//  const auto table_wrapper = std::make_shared<TableWrapper>(table_b);
//  const auto a_placeholder = std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{0});
//  const auto projection_expressions = std::vector<std::shared_ptr<AbstractExpression>>({
//    std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a_placeholder, x)
//  });
//  const auto projection = std::make_shared<Projection>(table_wrapper, projection_expressions);
//  const auto a_plus_x_eq_13 = std::make_shared<TableScan>(projection, ColumnID{0}, PredicateCondition::Equals, 13);
//  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(a_plus_x_eq_13, DataType::Int, false, std::vector<ColumnID>{ColumnID{0}});
//
//  const auto exists_expression = std::make_shared<ExistsExpression>(pqp_select_expression);
//
//  const auto actual_result = evaluator->evaluate_expression<int32_t>(*exists_expression);
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(actual_result);
//
//  std::vector<int32_t> expected_values = {0, 0, 1, 1};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, Extract) {
//  const auto extract_year_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Year, dates);
//  const auto actual_years = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_year_expression));
//  const auto expected_years = std::vector<std::string>({"2017", "2014", "2011", "2010"});
//  EXPECT_EQ(actual_years, expected_years);
//
//  const auto extract_month_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Month, dates);
//  const auto actual_months = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_month_expression));
//  const auto expected_months = std::vector<std::string>({"12", "08", "09", "01"});
//  EXPECT_EQ(actual_months, expected_months);
//
//  const auto extract_day_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Day, dates);
//  const auto actual_days = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_day_expression));
//  const auto expected_days = std::vector<std::string>({"06", "05", "03", "02"});
//  EXPECT_EQ(actual_days, expected_days);
//}
//
//TEST_F(ExpressionEvaluatorTest, NullLiteral) {
//  const auto actual_result = evaluator->evaluate_expression<int32_t>(*add(a, null()));
//  ASSERT_EQ(actual_result.type(), typeid(NullValue));
//}
//
////TEST_F(ExpressionEvaluatorTest, Substring) {
////  /**
////   * SELECT
////   *    SUBSTRING(s1, a, b)
////   * FROM
////   *    table_a
////   */
////  const auto substring_expression = std::make_shared<FunctionExpression>(FunctionType::Substring, s1, a, b);
////  const auto actual_values = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*substring_expression));
////
////  const auto expected_values = std::vector<std::string>({"a", "ell", "at", "e"});
////
////  EXPECT_EQ(actual_values, expected_values);
////}
//
//TEST_F(ExpressionEvaluatorTest, PQPSelectExpression) {
//  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_b);
//  const auto external_b = std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{0});
//  const auto b_plus_x = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, external_b, x);
//  const auto inner_expressions = std::vector<std::shared_ptr<AbstractExpression>>({b_plus_x, x});
//  const auto inner_projection = std::make_shared<Projection>(table_wrapper_b, inner_expressions);
//  const auto table_scan = std::make_shared<TableScan>(inner_projection, ColumnID{0}, PredicateCondition::Equals, 12);
//  const auto aggregates = std::vector<AggregateColumnDefinition>({{AggregateFunction::Sum, ColumnID{1}, "SUM(b)"}});
//  const auto aggregate = std::make_shared<Aggregate>(table_scan, aggregates, std::vector<ColumnID>{});
//
//  const auto parameters = std::vector<ColumnID>({ColumnID{1}});
//  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(aggregate, DataType::Int, true, parameters);
//
//  const auto expected_result = std::vector<int64_t>({20, 9, 24, 7});
//  EXPECT_EQ(boost::get<std::vector<int64_t>>(evaluator->evaluate_expression<int64_t>(*pqp_select_expression)), expected_result);
//}

}  // namespace opossum