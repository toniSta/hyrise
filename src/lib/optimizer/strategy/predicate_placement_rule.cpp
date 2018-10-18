#include "predicate_placement_rule.hpp"
#include "all_parameter_variant.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "operators/operator_scan_predicate.hpp"

namespace opossum {

std::string PredicatePlacementRule::name() const { return "Predicate Pushdown Rule"; }

bool PredicatePlacementRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // traverse() requires the existence of a root of the LQP, so make sure we have that
  const auto root_node = node->type == LQPNodeType::Root ? node : LogicalPlanRootNode::make(node);

  std::vector<std::shared_ptr<PredicateNode>> push_down_nodes;
  _push_down_traversal(root_node, LQPInputSide::Left, push_down_nodes);

  // TODO(moritz) Handle multiple outputs
  _pull_up_traversal(root_node, LQPInputSide::Left);

  // No easy way to tell whether the plan changed
  return false;
}

void PredicatePlacementRule::_push_down_traversal(const std::shared_ptr<AbstractLQPNode>& current_node,
                                                  const LQPInputSide input_side,
                                                  std::vector<std::shared_ptr<PredicateNode>>& push_down_nodes) {
  const auto input_node = current_node->input(input_side);
  if (!input_node) return; // Allow calling without checks


  switch (input_node->type) {
    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(input_node);

      if (!_is_expensive_predicate(predicate_node->predicate)) {
        push_down_nodes.emplace_back(predicate_node);
        lqp_remove_node(predicate_node);
        _push_down_traversal(current_node, input_side, push_down_nodes);
      } else {
        _push_down_traversal(input_node, input_side, push_down_nodes);
      }

    } break;

    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(input_node);

      // Left empty for non-push-past joins
      auto left_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      auto right_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};

      if (join_node->join_mode == JoinMode::Inner || join_node->join_mode == JoinMode::Cross) {
        for (const auto& push_down_node : push_down_nodes) {
          const auto move_to_left = expression_evaluable_on_lqp(push_down_node->predicate, *join_node->left_input());
          const auto move_to_right = expression_evaluable_on_lqp(push_down_node->predicate, *join_node->right_input());

          if (!move_to_left && !move_to_right) {
            _insert_nodes(current_node, input_side, {push_down_node});
          }

          if (move_to_left) left_push_down_nodes.emplace_back(push_down_node);
          if (move_to_right) right_push_down_nodes.emplace_back(push_down_node);
        }

      } else {
        // We do not push past non-inner/cross joins, place all predicates here
        _insert_nodes(current_node, input_side, push_down_nodes);
      }

      _push_down_traversal(input_node, LQPInputSide::Left, left_push_down_nodes);
      _push_down_traversal(input_node, LQPInputSide::Right, right_push_down_nodes);

    } break;

    case LQPNodeType::Alias:
    case LQPNodeType::Sort:
    case LQPNodeType::Projection: {
      // These nodes types we can push all predicates past
      _push_down_traversal(input_node, LQPInputSide::Left, push_down_nodes);
    } break;

    default: {
      // All not explicitly handled node types are barriers and we do not push predicates past them.
      _insert_nodes(current_node, input_side, push_down_nodes);

      if (input_node->left_input()) {
        auto left_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
        _push_down_traversal(input_node, LQPInputSide::Left, left_push_down_nodes);
      }
      if (input_node->right_input()) {
        auto right_push_down_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
        _push_down_traversal(input_node, LQPInputSide::Right, right_push_down_nodes);
      }
    }
  }
}

std::vector<std::shared_ptr<PredicateNode>> PredicatePlacementRule::_pull_up_traversal(const std::shared_ptr<AbstractLQPNode>& current_node,
                                                                      const LQPInputSide input_side) {
  if (!current_node) return {};
  const auto input_node = current_node->input(input_side);
  if (!input_node) return {};

  auto candidate_nodes = _pull_up_traversal(current_node->input(input_side), LQPInputSide::Left);
  auto candidate_nodes_right = _pull_up_traversal(current_node->input(input_side), LQPInputSide::Right);
  candidate_nodes.insert(candidate_nodes.end(), candidate_nodes_right.begin(), candidate_nodes_right.end());

  if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(input_node);
  predicate_node && _is_expensive_predicate(predicate_node->predicate)) {

    candidate_nodes.emplace_back(predicate_node);
    lqp_remove_node(predicate_node);
  }

  if (current_node->output_count() > 1) {
    // No pull up past nodes with more than one output, because if we did, the other outputs would loose the
    // predicate we pulled up
    _insert_nodes(current_node, input_side, candidate_nodes);
    return {};
  }

  switch (current_node->type) {
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(current_node);

      if (join_node->join_mode == JoinMode::Inner || join_node->join_mode == JoinMode::Cross) {
        return candidate_nodes;
      } else {
        // No PullUp past non-inner/cross joins for now
        _insert_nodes(current_node, input_side, candidate_nodes);
      }
    } break;

    case LQPNodeType::Alias:
    case LQPNodeType::Predicate:
      return candidate_nodes;

    case LQPNodeType::Projection: {
      auto pull_up_nodes = std::vector<std::shared_ptr<PredicateNode>>{};
      auto blocked_nodes = std::vector<std::shared_ptr<PredicateNode>>{};

      for (const auto& candidate_node : candidate_nodes) {
        if (expression_evaluable_on_lqp(candidate_node->predicate, current_node)) {
          pull_up_nodes.emplace_back(pull_up_nodes);
        } else {
          blocked_nodes.emplace_back(pull_up_nodes);
        }
      }

      _insert_nodes(current_node, input_side, blocked_nodes);
      return pull_up_nodes    ;

    } break;

    default:
      // No pull up past all other node types
      _insert_nodes(current_node, input_side, candidate_nodes);
      return {};
  }
}

void PredicatePlacementRule::_insert_nodes(const std::shared_ptr<AbstractLQPNode>& node,
                          const LQPInputSide input_side,
                          const std::vector<std::shared_ptr<PredicateNode>>& predicate_nodes) {
  // First node gets inserted on the @param input_side, all others on the left side of their output.
  auto current_node = node;
  auto current_input_side = input_side;

  const auto previous_input_node = node->input(input_side);

  for (const auto& predicate_node : predicate_nodes) {
    current_node->set_input(current_input_side, predicate_node);
    current_node = predicate_node;
    current_input_side = LQPInputSide::Left;
  }

  current_node->set_input(current_input_side, previous_input_node);
}

bool PredicatePlacementRule::_is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate) {
  /**
   * We (heuristically) consider a predicate to be expensive if it contains a correlated subselect. Otherwise, we
   * consider it to be cheap
   */
  auto predicate_contains_correlated_subselect = false;
  visit_expression(predicate, [&](const auto& sub_expression) {
    if (const auto select_expression = std::dynamic_pointer_cast<LQPSelectExpression>(sub_expression);
    select_expression && !select_expression->arguments.empty()) {
      predicate_contains_correlated_subselect = true;
      return ExpressionVisitation::DoNotVisitArguments;
    } else {
      return ExpressionVisitation::VisitArguments;
    }
  });
  return predicate_contains_correlated_subselect;
}

}  // namespace opossum
