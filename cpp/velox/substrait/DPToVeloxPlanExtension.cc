#include "DPToVeloxPlanExtension.h"

namespace gluten {

std::vector<uint8_t> serializeRel(const ::substrait::Rel& rel) {
  size_t size = rel.ByteSizeLong();
  std::vector<uint8_t> buffer(size);
  if (!rel.SerializeToArray(buffer.data(), size)) {
    VELOX_FAIL("Failed to serialize Rel {}", rel.DebugString());
  }
  return buffer;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPFilterNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::FilterNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel) {

  std::cerr << "Try to create DPFilterNode for " << rel.DebugString() << std::endl;

  auto serialized_substrait = serializeRel(rel);
  if (canOffloadToDP(serialized_substrait)) {
    auto dpFilterNode = std::make_shared<DPFilterNode>(
      id,
      original_node,
      source,
      std::move(serialized_substrait)
    );
    return dpFilterNode;
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPProjectNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::ProjectNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel) {

  std::cerr << "Try to create DPProjectNode for " << rel.DebugString() << std::endl;

  auto serialized_substrait = serializeRel(rel);
  if (canOffloadToDP(serialized_substrait)) {
    auto dpProjectNode = std::make_shared<DPProjectNode>(
      id,
      original_node,
      source,
      std::move(serialized_substrait)
    );
    return dpProjectNode;
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPAggregateNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::AggregationNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel) {

  std::cerr << "Try to create DPAggregateNode for " << rel.DebugString() << std::endl;

  auto serialized_substrait = serializeRel(rel);
  if (canOffloadToDP(serialized_substrait)) {
    auto dpAggregateNode = std::make_shared<DPAggregateNode>(
      id,
      original_node,
      source,
      std::move(serialized_substrait)
    );
    return dpAggregateNode;
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPOrderByNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::OrderByNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::Rel& rel) {

  std::cerr << "Try to create DPOrderByNode for " << rel.DebugString() << std::endl;

  auto serialized_substrait = serializeRel(rel);
  if (canOffloadToDP(serialized_substrait)) {
    auto dpOrderByNode = std::make_shared<DPOrderByNode>(
      id,
      original_node,
      source,
      std::move(serialized_substrait)
    );
    return dpOrderByNode;
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPHashJoinNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::HashJoinNode> original_node,
    facebook::velox::core::PlanNodePtr left,
    facebook::velox::core::PlanNodePtr right,
    const ::substrait::Rel& rel) {

  std::cerr << "Try to create DPHashJoinNode for " << rel.DebugString() << std::endl;

  auto serialized_substrait = serializeRel(rel);
  if (canOffloadToDP(serialized_substrait)) {
    auto dpHashJoinNode = std::make_shared<DPHashJoinNode>(
      id,
      original_node,
      left,
      right,
      std::move(serialized_substrait)
    );
    return dpHashJoinNode;
  }
  return std::nullopt;
}

} // namespace gluten
