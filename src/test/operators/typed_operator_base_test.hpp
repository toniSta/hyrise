#include "base_test.hpp"

#include <boost/hana/for_each.hpp>

#include "all_type_variant.hpp"
#include "constant_mappings.hpp"
#include "storage/encoding_type.hpp"

namespace opossum {

using ParamType = std::tuple<DataType, EncodingType, bool /* nullable */>;

// This is the base class for all tests that should run on every possible combination of data type and encoding.
// A sample table is generate in TableScanBetweenTest. Depending on how other tests will use this, it might make sense
// to have a single instantiation here.
class TypedOperatorBaseTest : public BaseTestWithParam<ParamType> {
 public:
  static std::string format(testing::TestParamInfo<ParamType> info) {
    const auto& [data_type, encoding, nullable] = info.param;

    return data_type_to_string.left.at(data_type) + encoding_type_to_string.left.at(encoding) +
           (nullable ? "" : "Not") + "Nullable";
  };
};

static std::vector<ParamType> create_param_pairs() {
  std::vector<ParamType> pairs;

  hana::for_each(data_type_pairs, [&](auto pair) {
    // TODO iterate over encoding types
    pairs.emplace_back(hana::first(pair), EncodingType::Unencoded, true);
    pairs.emplace_back(hana::first(pair), EncodingType::Unencoded, false);
  });

  return pairs;
}

}  // namespace opossum
