
#pragma once

#include <vector>
#include <optional>
#include <cstdint>
#include "rust/cxx.h"
#include "velox/core/PlanNode.h"
#include "substrait/algebra.pb.h"
#include "cpp_rust_interop/bridge.h"
#include "operators/dpextensions/Nodes.h"

namespace gluten {

std::vector<uint8_t> serializeRel(const ::substrait::Rel& rel);

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPFilterNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::FilterNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel);

} // namespace gluten