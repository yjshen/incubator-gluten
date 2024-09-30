#include "DPToVeloxPlanExtension.h"
#include "substrait/SubstraitParser.h"
#include "velox/compute/VeloxBackend.h"
#include "core/config/GlutenConfig.h"

#include "build/core/proto/substrait/algebra.pb.h"
#include "build/core/proto/substrait/plan.pb.h"
#include "build/core/proto/substrait/type.pb.h"
#include "build/core/proto/substrait/capabilities.pb.h"
#include "build/core/proto/substrait/function.pb.h"
#include "build/core/proto/substrait/parameterized_types.pb.h"
#include "build/core/proto/substrait/type_expressions.pb.h"

namespace gluten {

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPFilterNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::FilterNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::FilterRel& filterRel) {

  auto dpFilterEnabled = VeloxBackend::get()->getBackendConf()->get<bool>(kDPFilterEnabled, false);
  if (!dpFilterEnabled) {
    DLOG(INFO) << "DP filter is not enabled or we cannot get it, return original filter node.";
    return std::nullopt;
  }

  if (filterRel.has_advanced_extension()) {
    auto maybeQflowIR = SubstraitParser::getQFlowIR(filterRel.advanced_extension());
    if (maybeQflowIR) {
      DLOG(INFO) << "Create DPFilterNode with QFlow IR: " << maybeQflowIR.value();
      return std::make_shared<DPFilterNode>(
        id,
        original_node,
        source,
        std::move(maybeQflowIR.value())
      );
    }
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPProjectNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::ProjectNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::ProjectRel& projectRel) {

  auto dpProjectEnabled = VeloxBackend::get()->getBackendConf()->get<bool>(kDPProjectEnabled, false);
  if (!dpProjectEnabled) {
    DLOG(INFO) << "DP project is not enabled or we cannot get it, return original project node.";
    return std::nullopt;
  }

  if (projectRel.has_advanced_extension()) {
    auto maybeQflowIR = SubstraitParser::getQFlowIR(projectRel.advanced_extension());
    if (maybeQflowIR) {
      DLOG(INFO) << "Create DPProjectNode with QFlow IR: " << maybeQflowIR.value();
      return std::make_shared<DPProjectNode>(
        id,
        original_node,
        source,
        std::move(maybeQflowIR.value())
      );
    }
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPAggregateNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::AggregationNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::AggregateRel& aggregateRel) {

  auto dpAggregateEnabled = VeloxBackend::get()->getBackendConf()->get<bool>(kDPAggregateEnabled, false);
  if (!dpAggregateEnabled) {
    DLOG(INFO) << "DP aggregate is not enabled or we cannot get it, return original aggregate node.";
    return std::nullopt;
  }

  if (aggregateRel.has_advanced_extension()) {
    auto maybeQflowIR = SubstraitParser::getQFlowIR(aggregateRel.advanced_extension());
    if (maybeQflowIR) {
      DLOG(INFO) << "Create DPAggregateNode with QFlow IR: " << maybeQflowIR.value();
      return std::make_shared<DPAggregateNode>(
        id,
        original_node,
        source,
        std::move(maybeQflowIR.value())
      );
    }
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPOrderByNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::OrderByNode> original_node,
    facebook::velox::core::PlanNodePtr source,
    const ::substrait::SortRel& sortRel) {

  auto dpOrderByEnabled = VeloxBackend::get()->getBackendConf()->get<bool>(kDPOrderByEnabled, false);
  if (!dpOrderByEnabled) {
    DLOG(INFO) << "DP order by is not enabled or we cannot get it, return original order by node.";
    return std::nullopt;
  }

  if (sortRel.has_advanced_extension()) {
    auto maybeQflowIR = SubstraitParser::getQFlowIR(sortRel.advanced_extension());
    if (maybeQflowIR) {
      DLOG(INFO) << "Create DPOrderByNode with QFlow IR: " << maybeQflowIR.value();
      return std::make_shared<DPOrderByNode>(
        id,
        original_node,
        source,
        std::move(maybeQflowIR.value())
      );
    }
  }
  return std::nullopt;
}

std::optional<facebook::velox::core::PlanNodePtr> tryCreateDPHashJoinNode(
    const facebook::velox::core::PlanNodeId& id,
    std::shared_ptr<const facebook::velox::core::HashJoinNode> original_node,
    facebook::velox::core::PlanNodePtr left,
    facebook::velox::core::PlanNodePtr right,
    const ::substrait::JoinRel& joinRel) {

  auto dpHashJoinEnabled = VeloxBackend::get()->getBackendConf()->get<bool>(kDPHashJoinEnabled, false);
  if (!dpHashJoinEnabled) {
    DLOG(INFO) << "DP hash join is not enabled or we cannot get it, return original hash join node.";
    return std::nullopt;
  }

  if (joinRel.has_advanced_extension()) {
    auto maybeQflowIR = SubstraitParser::getQFlowIR(joinRel.advanced_extension());
    if (maybeQflowIR) {
      DLOG(INFO) << "Create DPHashJoinNode with QFlow IR: " << maybeQflowIR.value();
      return std::make_shared<DPHashJoinNode>(
        id,
        original_node,
        left,
        right,
        std::move(maybeQflowIR.value())
      );
    }
  }
  return std::nullopt;
}

} // namespace gluten
