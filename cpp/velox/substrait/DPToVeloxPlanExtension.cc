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

} // namespace gluten
