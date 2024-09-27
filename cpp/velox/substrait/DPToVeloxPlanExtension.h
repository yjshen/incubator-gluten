
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

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPProjectNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::ProjectNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel);

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPAggregateNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::AggregationNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel);

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPOrderByNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::OrderByNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel);

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPHashJoinNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::HashJoinNode> original_node,
    facebook::velox::core::PlanNodePtr left,
    facebook::velox::core::PlanNodePtr right,
    const ::substrait::Rel& rel);

} // namespace gluten
