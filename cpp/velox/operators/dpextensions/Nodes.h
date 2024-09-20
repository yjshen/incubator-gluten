// Velox query plan is a tree of PlanNodes.
// DP nodes are the nodes that are validated by the DPConverter, and could be
// later translated to DP Operators, which would call DP runtime for execution.

#pragma once

#include "velox/core/PlanNode.h"
#include <folly/dynamic.h>
#include <string>
#include <vector>

using PlanNodePtr = std::shared_ptr<const facebook::velox::core::PlanNode>;

namespace gluten {

template<typename BaseNode>
class DPNodeMixin : public facebook::velox::core::PlanNode {
protected:
    std::shared_ptr<const BaseNode> original_node_;
    std::vector<uint8_t> serialized_substrait_;

public:
    DPNodeMixin(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const BaseNode> original_node,
        std::vector<uint8_t> serialized_substrait)
        : facebook::velox::core::PlanNode(id),
          original_node_(std::move(original_node)),
          serialized_substrait_(std::move(serialized_substrait)) {}

    const facebook::velox::RowTypePtr& outputType() const override {
        return original_node_->outputType();
    }

    std::string_view name() const override {
        static std::string dpName = "DP" + std::string(original_node_->name());
        return dpName;
    }

    folly::dynamic serialize() const override {
        return original_node_->serialize();
    }

    const std::vector<uint8_t>& getSerializedSubstrait() const {
        return serialized_substrait_;
    }

    virtual void addDPDetails(std::stringstream& stream) const {
        stream << "DP version of " << original_node_->name();
    }

protected:
    const facebook::velox::core::PlanNode& getOriginalNode() const {
        return *original_node_;
    }

    facebook::velox::core::PlanNode& getOriginalNode() {
        return const_cast<facebook::velox::core::PlanNode&>(*original_node_);
    }

    void addDetails(std::stringstream& stream) const override {
        addDPDetails(stream);
    }
};

class DPProjectNode : public DPNodeMixin<facebook::velox::core::ProjectNode> {
public:
    DPProjectNode(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const facebook::velox::core::ProjectNode> original_node,
        PlanNodePtr source,
        std::vector<uint8_t> serialized_substrait)
        : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(source)} {}

    const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
      return sources_;
    }

private:
    const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

 class DPFilterNode : public DPNodeMixin<facebook::velox::core::FilterNode> {
 public:
     DPFilterNode(
         const facebook::velox::core::PlanNodeId& id,
         std::shared_ptr<const facebook::velox::core::FilterNode> original_node,
         PlanNodePtr source,
         std::vector<uint8_t> serialized_substrait)
         : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(source)} {}

     const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
         return sources_;
     }

 private:
     const std::vector<facebook::velox::core::PlanNodePtr> sources_;
 };

class DPAggregateNode : public DPNodeMixin<facebook::velox::core::AggregationNode> {
public:
    DPAggregateNode(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const facebook::velox::core::AggregationNode> original_node,
        PlanNodePtr source,
        std::vector<uint8_t> serialized_substrait)
        : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(source)} {}

    const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
        return sources_;
    }

    // Checks if the aggregation can spill to disk based on the query configuration
    // @param queryConfig The query configuration
    // @return true if aggregation spilling is enabled, false otherwise
    bool canSpill(const facebook::velox::core::QueryConfig& queryConfig) const {
        return queryConfig.aggregationSpillEnabled();
    }

    // Checks if this is a partial aggregation step
    // @return true if this is a partial aggregation, false otherwise
    bool isPartial() const {
        return original_node_->step() == facebook::velox::core::AggregationNode::Step::kPartial;
    }

    // Checks if the input data is already pre-grouped
    // @return true if the input is pre-grouped, false otherwise
    bool isPreGrouped() const {
        return original_node_->isPreGrouped();
    }

private:
    const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class DPOrderByNode : public DPNodeMixin<facebook::velox::core::OrderByNode> {
public:
    DPOrderByNode(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const facebook::velox::core::OrderByNode> original_node,
        PlanNodePtr source,
        std::vector<uint8_t> serialized_substrait)
        : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(source)} {}

    const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
        return sources_;
    }

    // Checks if the order by operation can spill to disk based on the query configuration
    // @param queryConfig The query configuration
    // @return true if order by spilling is enabled, false otherwise
    bool canSpill(const facebook::velox::core::QueryConfig& queryConfig) const {
        return queryConfig.orderBySpillEnabled();
    }

private:
    const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class DPHashJoinNode : public DPNodeMixin<facebook::velox::core::HashJoinNode> {
public:
    DPHashJoinNode(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const facebook::velox::core::HashJoinNode> original_node,
        PlanNodePtr left,
        PlanNodePtr right,
        std::vector<uint8_t> serialized_substrait)
        : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(left), std::move(right)} {}

    const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
        return sources_;
    }

    // Checks if the hash join can spill to disk based on the query configuration
    // @param queryConfig The query configuration
    // @return true if the original node can spill, false otherwise
    bool canSpill(const facebook::velox::core::QueryConfig& queryConfig) const {
        return original_node_->canSpill(queryConfig);
    }

private:
    const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class DPMergeJoinNode : public DPNodeMixin<facebook::velox::core::MergeJoinNode> {
public:
    DPMergeJoinNode(
        const facebook::velox::core::PlanNodeId& id,
        std::shared_ptr<const facebook::velox::core::MergeJoinNode> original_node,
        PlanNodePtr left,
        PlanNodePtr right,
        std::vector<uint8_t> serialized_substrait)
        : DPNodeMixin(id, std::move(original_node), std::move(serialized_substrait)),
            sources_{std::move(left), std::move(right)} {}

    const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
        return sources_;
    }

    facebook::velox::core::JoinType joinType() const {
        return original_node_->joinType();
    }

    // The following methods check for specific join types:
    bool isInnerJoin() const {
        return joinType() == facebook::velox::core::JoinType::kInner;
    }

    bool isLeftJoin() const {
        return joinType() == facebook::velox::core::JoinType::kLeft;
    }

    bool isRightJoin() const {
        return joinType() == facebook::velox::core::JoinType::kRight;
    }

    bool isFullJoin() const {
        return joinType() == facebook::velox::core::JoinType::kFull;
    }

    bool isLeftSemiFilterJoin() const {
        return joinType() == facebook::velox::core::JoinType::kLeftSemiFilter;
    }

    bool isLeftSemiProjectJoin() const {
        return joinType() == facebook::velox::core::JoinType::kLeftSemiProject;
    }

    bool isRightSemiFilterJoin() const {
        return joinType() == facebook::velox::core::JoinType::kRightSemiFilter;
    }

    bool isRightSemiProjectJoin() const {
        return joinType() == facebook::velox::core::JoinType::kRightSemiProject;
    }

    bool isAntiJoin() const {
        return joinType() == facebook::velox::core::JoinType::kAnti;
    }

    bool isPreservingProbeOrder() const {
        return isInnerJoin() || isLeftJoin() || isAntiJoin();
    }

private:
    const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

} //  namespace gluten
