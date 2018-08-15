#pragma once

#include <map>
#include <vector>

#include "abstract_cost_model.hpp"
#include "cost_feature.hpp"

namespace opossum {

/**
 * CostModelLinear has different models for different kind of TableScans.
 */
enum class CostModelLinearTableScanType {
  ColumnValueNumeric,
  ColumnColumnNumeric,
  ColumnValueString,
  ColumnColumnString,
  Like
};

/**
 * Weights of the CostModelLinear for a particular build type (release, debug)
 */
struct CostModelLinearConfig final {
  std::map<CostModelLinearTableScanType, CostFeatureWeights> table_scan_models;
  std::map<OperatorType, CostFeatureWeights> other_operator_models;
};

/**
 * Experimental Cost Model that tries to predict the actual runtime in microseconds of an operator. Experiments have
 * shown it to perform only a little better than the much simpler "CostModelNaive"
 *
 * - Currently only support JoinHash, TableScan, UnionPosition and Product, i.e., the most essential operators for
 *      JoinPlans
 * - Calibrated on a specific machine on a specific hyrise code base - so not expected to yield reliable results
 * - For JoinHash - since it shows erratic performance behaviour - only the runtime of some of the operators phases is
 *      being predicted.
 */
class CostModelLinear : public AbstractCostModel {
 public:
  static CostModelLinearConfig create_debug_build_config();
  static CostModelLinearConfig create_release_build_config();

  /**
   * @return a CostModelLinear calibrated on the current build type (debug, release)
   */
  static CostModelLinearConfig create_current_build_type_config();

  explicit CostModelLinear(const CostModelLinearConfig& config = create_current_build_type_config());

  std::string name() const override;

  Cost get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const override;

  Cost estimate_cost(const AbstractCostFeatureProxy& feature_proxy) const override;

 protected:
   Cost _predict_cost(const CostFeatureWeights& feature_weights, const AbstractCostFeatureProxy& feature_proxy) const;

 private:
  CostModelLinearConfig _config;
};

}  // namespace opossum