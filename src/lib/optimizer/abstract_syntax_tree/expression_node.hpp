#pragma once

#include <sstream>
#include <string>
#include <vector>


#include "all_type_variant.hpp"
#include "common.hpp"
#include "optimizer/abstract_syntax_tree/abstract_node.hpp"


namespace opossum {

enum class ExpressionType {
  ExpressionLiteral,
  ExpressionStar,
  ExpressionParameter,
  ExpressionColumnReference,
  ExpressionFunctionReference,
  ExpressionOperator,
  ExpressionSelect,
  ExpressionPlus,
  ExpressionMinus,
  ExpressionAsterisk,
  ExpressionSlash,
  ExpressionPercentage,
  ExpressionCaret,
  ExpressionEquals,
  ExpressionNotEquals,
  ExpressionLess,
  ExpressionLessEq,
  ExpressionGreater,
  ExpressionGreaterEq,
  ExpressionLike,
  ExpressionNotLike,
  ExpressionAnd,
  ExpressionOr,
  ExpressionIn,
  ExpressionNot,
  ExpressionIsNull,
  ExpressionExists,
  ExpressionBetween,
  ExpressionCase,
  ExpressionHint
};

class ExpressionNode : public AbstractNode {
public:
  ExpressionNode(const ExpressionType type);
  ExpressionNode(const ExpressionType type, const std::string &table_name, const std::string &column_name);
  ExpressionNode(const ExpressionType type, const AllTypeVariant value/*, const AllTypeVariant value2*/);

  const std::string description() const override;

  const std::string& table_name() const;

  const std::string& column_name() const;

  const AllTypeVariant value() const;

//  const AllTypeVariant value2() const;

  const ExpressionType expression_type() const;

private:
  const std::string _type_to_string() const;

  const ExpressionType _type;
  const AllTypeVariant _value;
//  const AllTypeVariant _value2;

  const std::string _name;
  const std::string _table;
//  char* alias;

};

}  // namespace opossum
