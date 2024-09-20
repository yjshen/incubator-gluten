// Naive conversion from Velox DP PlanNode to DP Operators

#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "rust/cxx.h"
#include "velox/operators/dpextensions/Nodes.h"
#include "velox/operators/dpextensions/Operators.h"

namespace gluten {

class DPProjectOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
    std::unique_ptr<facebook::velox::exec::Operator> toOperator(
        facebook::velox::exec::DriverCtx* ctx,
        int32_t id,
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpProjectNode = std::dynamic_pointer_cast<const DPProjectNode>(node)) {
            return std::make_unique<DPProject>(id, ctx, dpProjectNode);
        }
        return nullptr;
    }
};

class DPFilterOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
    std::unique_ptr<facebook::velox::exec::Operator> toOperator(
        facebook::velox::exec::DriverCtx* ctx,
        int32_t id,
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpFilterNode = std::dynamic_pointer_cast<const DPFilterNode>(node)) {
            return std::make_unique<DPFilter>(id, ctx, dpFilterNode);
        }
        return nullptr;
    }
};

class DPOrderByOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
    std::unique_ptr<facebook::velox::exec::Operator> toOperator(
        facebook::velox::exec::DriverCtx* ctx,
        int32_t id,
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpOrderByNode = std::dynamic_pointer_cast<const DPOrderByNode>(node)) {
            return std::make_unique<DPOrderBy>(id, ctx, dpOrderByNode);
        }
        return nullptr;
    }
};

class DPAggregateOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
    std::unique_ptr<facebook::velox::exec::Operator> toOperator(
        facebook::velox::exec::DriverCtx* ctx,
        int32_t id,
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpAggregateNode = std::dynamic_pointer_cast<const DPAggregateNode>(node)) {
            if (dpAggregateNode->isPreGrouped()) {
                return std::make_unique<DPStreamingAggregate>(id, ctx, dpAggregateNode);
            } else {
                return std::make_unique<DPHashAggregate>(id, ctx, dpAggregateNode);
            }
        }
        return nullptr;
    }
};

using OperatorSupplier = std::function<
    std::unique_ptr<facebook::velox::exec::Operator>(int32_t operatorId, facebook::velox::exec::DriverCtx* ctx)>;


class DPHashJoinOperatorTranslator : public facebook::velox::exec::Operator::PlanNodeTranslator {
public:
    std::unique_ptr<facebook::velox::exec::Operator> toOperator(
        facebook::velox::exec::DriverCtx* ctx,
        int32_t id,
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpHashJoinNode = std::dynamic_pointer_cast<const DPHashJoinNode>(node)) {
            return std::make_unique<DPHashJoinProbe>(id, ctx, dpHashJoinNode);
        }
        return nullptr;
    }

    OperatorSupplier toOperatorSupplier(
        const facebook::velox::core::PlanNodePtr& node) override {
        if (auto dpHashJoinNode = std::dynamic_pointer_cast<const DPHashJoinNode>(node)) {
            return [dpHashJoinNode](int32_t operatorId, facebook::velox::exec::DriverCtx* ctx) {
                return std::make_unique<DPHashJoinBuild>(operatorId, ctx, dpHashJoinNode);
            };
        }
        return nullptr;
    }
};

} // namespace gluten
