/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"
#include "rust/cxx.h"

namespace gluten {

class DPFilterNode final : public facebook::velox::core::PlanNode {
 public:
  DPFilterNode(
      const facebook::velox::core::PlanNodeId& id,
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::core::PlanNodePtr& source,
      std::string filterExpression,
      std::vector<uint8_t> serializedFilter)
      : facebook::velox::core::PlanNode(id),
        outputType_(outputType),
        source_(source),
        filterExpression_(std::move(filterExpression)),
        serializedFilter_(std::move(serializedFilter)) {}

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources() const override {
    return sources_;
  }

  const std::string& filterExpression() const {
    return filterExpression_;
  }

  std::string_view name() const override {
    return "DPFilter";
  }

  folly::dynamic serialize() const override {
    VELOX_UNSUPPORTED("DPFilter plan node is not serializable");
  }

 private:
  void addDetails(std::stringstream& stream) const override {
    stream << "filterExpression: " << filterExpression_;
  }

  const facebook::velox::RowTypePtr outputType_;
  const facebook::velox::core::PlanNodePtr source_;
  std::vector<facebook::velox::core::PlanNodePtr> sources_{source_};
  std::string filterExpression_;
  std::vector<uint8_t> serializedFilter_;
};

class DPFilter : public facebook::velox::exec::Operator {
 public:
  DPFilter(
      int32_t operatorId,
      facebook::velox::exec::DriverCtx* driverCtx,
      std::shared_ptr<const DPFilterNode> dpFilterNode)
      : facebook::velox::exec::Operator(
            driverCtx,
            dpFilterNode->outputType(),
            operatorId,
            dpFilterNode->id(),
            dpFilterNode->name().data()),
        filterExpression_(dpFilterNode->filterExpression()) {
    // Initialize filter logic here
  }

  bool needsInput() const override {
    return !input_;
  }

  void addInput(facebook::velox::RowVectorPtr input) override {
    input_ = std::move(input);
  }

  facebook::velox::RowVectorPtr getOutput() override {
    if (!input_) {
      return nullptr;
    }

    // Apply filter logic here
    // This is a placeholder implementation
    auto result = applyFilter(input_);
    input_ = nullptr;
    return result;
  }

  facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
    return facebook::velox::exec::BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_;
  }

 private:
  facebook::velox::RowVectorPtr applyFilter(const facebook::velox::RowVectorPtr& input) {
    // Implement actual filtering logic here
    // This is just a placeholder that returns the input unchanged
    // Placeholder for Falcon filter implementation
    std::cerr << "Placeholder for Falcon filter: Input row vector size = " << input->size() << std::endl;
    LOG(WARNING) << "Placeholder for Falcon filter: Input row vector size = " << input->size();
    return input;
  }

  std::string filterExpression_;
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

} // namespace gluten
