// Velox operators are analogy to Physical Operators in Spark, and are the basic
// building blocks of a Velox executable pipeline that could be later driven by [[velox::exec::Driver]]
// to be actually executing over the data.
//
// A DP operator is the operator supported by DP runtime, wraps the pointer for a DFG, and delegates the
// actual execution to the DP runtime.

#pragma once

#include "Nodes.h"
#include "IO.h"
#include "velox/exec/Operator.h"
#include "sparkle.h"
#include "velox/type/Type.h"
#include <cstdint>
#include <iostream>

namespace gluten {

class DPBaseOperator : public facebook::velox::exec::Operator {
protected:
    bool initialized_ = false;
    SparkleHandle_t sparkle_handle_ = nullptr;
    std::vector<std::string> qflows_;
    std::string name_;
    std::shared_ptr<const facebook::velox::core::PlanNode> planNode_;

public:
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        const std::string& qflow)
        : Operator(driverCtx, planNode->outputType(), operatorId, planNode->id(), planNode->name().data()),
          qflows_({qflow}),
          name_(planNode->name().data()),
          planNode_(planNode) {}

    // Constructor with operatorType
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        std::string operatorType,
        const std::string& qflow)
        : Operator(driverCtx, planNode->outputType(), operatorId, planNode->id(), operatorType),
          qflows_({qflow}),
          name_(operatorType),
          planNode_(planNode) {}


    // Constructor with spillConfig
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        const std::string& qflow,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              planNode->name().data(),
              spillConfig),
          qflows_({qflow}),
          name_(planNode->name().data()),
          planNode_(planNode) {}

    // Constructor with spillConfig and operator type
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        std::string operatorType,
        const std::string& qflow,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              operatorType,
              spillConfig),
          qflows_({qflow}),
          name_(operatorType),
          planNode_(planNode) {}

    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        bool noOutput,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        std::string operatorType,
        const std::string& qflow,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              operatorType,
              spillConfig),
          qflows_({qflow}),
          name_(operatorType),
          planNode_(planNode) {}

    /**
     * @brief Initializes the operator by compile the QFlow IR and launch the sparkle backend.
     * This method should be called before any processing begins.
     */
    virtual void initialize() {
        if (!initialized_) {
            sparkle_handle_ = sparkle_init();
            auto sparkle_status = sparkle_launch(sparkle_handle_, qflows_);
            if (sparkle_status != SparkleSuccess) {
                VELOX_FAIL(
                    "Failed to compile QFlow IR and launch sparkle backend. Status {} ", static_cast<int>(sparkle_status)); 
            }
            initialized_ = true;
        }
    }

    /**
     * @brief Closes the operator and releases resources.
     * This method should be called when the operator is no longer needed.
     */
    void close() override {
        Operator::close();
        sparkle_clean(sparkle_handle_);
    }

    std::string toString() const override {
        std::stringstream out;
        out << operatorType() << "[" << planNodeId() << "] " << operatorId() << " " << name_;
        return out.str();
    }

    /**
     * @brief Indicates that no more input will be added.
     * This method is called when all input has been processed.
     * Child classes can override this method to provide custom behavior.
     */
    void noMoreInput() override {
        Operator::noMoreInput();
        auto status = sparkle_send_done(sparkle_handle_);
        VELOX_CHECK_EQ(status, SparkleSuccess, "Failed to send done to sparkle backend");
    }

    /**
     * Overwrite the name of the operator.
     * 
     * @param newName the new name of the operator.
     */
    void setName(std::string& newName) {
        name_ = newName;
    }

    std::vector<std::string> qflows() const {
        return qflows_;
    }

    const std::shared_ptr<const facebook::velox::core::PlanNode> getPlanNode() const {
        return planNode_;
    }

    void setQflows(const std::vector<std::string>& qflows) {
        qflows_ = qflows;
    }

    virtual std::unique_ptr<DPBaseOperator> clone() const = 0;

protected:
    const uint8_t* toRawPointer(const facebook::velox::RowVectorPtr& input) {
        return reinterpret_cast<const uint8_t*>(input.get());
    }

    facebook::velox::RowVectorPtr fromRawPointer(const uint8_t* outputPtr) {
        return std::static_pointer_cast<facebook::velox::RowVector>(
            std::shared_ptr<facebook::velox::RowVector>(
                reinterpret_cast<facebook::velox::RowVector*>(const_cast<uint8_t*>(outputPtr))
            )
        );
    }

    std::string fusedOperatorType() const {
        return operatorType() + "*";
    }
};

class DPProject : public DPBaseOperator {
public:
    DPProject(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPProjectNode> dpProjectNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpProjectNode,
              dpProjectNode->getQflow()) {}

    DPProject(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPProjectNode> dpProjectNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpProjectNode,
              operatorType,
              dpProjectNode->getQflow()) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPProject>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPProjectNode>(planNode_),
            fusedOperatorType()
        );
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

        initialize();
        auto result = evaluate(input_);
        input_ = nullptr;
        return result;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
        return noMoreInput_;
    }

    facebook::velox::RowVectorPtr evaluate(const facebook::velox::RowVectorPtr& input) {
        // auto inputPtr = toRawPointer(input);
        // const uint8_t* outputPtr = evaluateDFG(dfg_instance_id_, inputPtr);
        // return fromRawPointer(outputPtr);
        return nullptr;
    }
};

class ResizeOperator : public facebook::velox::exec::Operator {
public:
    ResizeOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        facebook::velox::RowTypePtr outputType)
        : Operator(
              driverCtx, outputType, operatorId, /*planNode->id()*/"", "resize") {

        const auto& queryConfig = driverCtx->queryConfig();
        maxOutputBatchSize_ = queryConfig.preferredOutputBatchRows();
   }

    void addInput(facebook::velox::RowVectorPtr input) override {
        in_ = std::move(input);
        cursor_ = 0;
    }

    bool needsInput() const override {
        return !noMoreInput_ && !in_;
    }

    bool isFinished() override {
        return !in_ && noMoreInput_;
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if(!in_) {
            return nullptr;
        }
        int32_t remainingLength = in_->size() - cursor_;
        GLUTEN_CHECK(remainingLength >= 0, "Invalid state");
        if (remainingLength == 0) {
          in_ = nullptr;
          return nullptr;
        }
        int32_t sliceLength = std::min(maxOutputBatchSize_, remainingLength);
        auto out = std::dynamic_pointer_cast<facebook::velox::RowVector>(in_->slice(cursor_, sliceLength));
        cursor_ += sliceLength;
        GLUTEN_CHECK(out != nullptr, "Invalid state");
        return out;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
            return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

private:
    int32_t maxOutputBatchSize_;
    facebook::velox::RowVectorPtr in_;
    int32_t cursor_ = 0;
};

class DPFilter : public DPBaseOperator {
public:
    DPFilter(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPFilterNode> dpFilterNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpFilterNode,
              dpFilterNode->getQflow()) {}

    DPFilter(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPFilterNode> dpFilterNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpFilterNode,
              operatorType,
              dpFilterNode->getQflow()) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPFilter>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPFilterNode>(planNode_),
            fusedOperatorType()
        );
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

        initialize();
        auto result = evaluate(input_);
        input_ = nullptr;
        return result;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
        return noMoreInput_;
    }

    facebook::velox::RowVectorPtr evaluate(const facebook::velox::RowVectorPtr& input) {
        // auto inputPtr = toRawPointer(input);
        // const uint8_t* outputPtr = evaluateDFG(dfg_instance_id_, inputPtr);
        // return fromRawPointer(outputPtr);
        return nullptr;
    }
};

class DPOrderBy : public DPBaseOperator {
public:
    DPOrderBy(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPOrderByNode> dpOrderByNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpOrderByNode,
              dpOrderByNode->getQflow(),
              dpOrderByNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}

    DPOrderBy(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPOrderByNode> dpOrderByNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpOrderByNode,
              operatorType,
              dpOrderByNode->getQflow(),
              dpOrderByNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPOrderBy>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPOrderByNode>(planNode_),
            fusedOperatorType()
        );
    }

    bool needsInput() const override {
        return !finished_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        // auto inputPtr = toRawPointer(input);
        // evaluateDFGBuild(dfg_instance_id_, inputPtr);
    }


    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        // if (dfgResultHasNext(dfg_instance_id_)) {
        //     const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
        //     return fromRawPointer(outputPtr);
        // } else {
        //     finished_ = true;
        //     return nullptr;
        // }
        return nullptr;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
        return finished_;
    }

    void reclaim(uint64_t targetBytes, facebook::velox::memory::MemoryReclaimer::Stats& stats)
        override {
            // auto freed = dfgSpill(dfg_instance_id_);
            // TODO: update stats
            return;
        }

 private:
    bool finished_ = false;
};

class DPHashAggregate : public DPBaseOperator {
public:
    DPHashAggregate(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPAggregateNode> dpAggregateNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpAggregateNode,
              dpAggregateNode->isPartial() ? "DPPartialHashAggregate" : "DPHashAggregate",
              dpAggregateNode->getQflow(),
              dpAggregateNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt),
                isPartialOutput(dpAggregateNode->isPartial()) {}

    DPHashAggregate(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPAggregateNode> dpAggregateNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpAggregateNode,
              operatorType,
              dpAggregateNode->getQflow(),
              dpAggregateNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt),
              isPartialOutput(dpAggregateNode->isPartial()) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPHashAggregate>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPAggregateNode>(planNode_),
            fusedOperatorType()
        );
    }

    bool needsInput() const override {
        return !noMoreInput_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        // auto inputPtr = toRawPointer(input);
        // if (isPartialOutput) {
        //     evaluateDFGPartialBuild(dfg_instance_id_, inputPtr);
        // } else {
        //     evaluateDFGBuild(dfg_instance_id_, inputPtr);
        // }
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        // dfgNoMoreInput(dfg_instance_id_);
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        // if (dfgResultHasNext(dfg_instance_id_)) {
        //     const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
        //     return fromRawPointer(outputPtr);   
        // } else {
        //     finished_ = true;
        //     return nullptr;
        // }
        return nullptr;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }
    
    bool isFinished() override {
        return finished_;
    }

    void reclaim(uint64_t targetBytes, facebook::velox::memory::MemoryReclaimer::Stats& stats)
        override {
            // auto freed = dfgSpill(dfg_instance_id_);
            // TODO: update stats
            return;
        }

private:
    bool isPartialOutput;
    // TODO: with limited memory, we could output partial results
    bool finished_ = false;
};

class DPStreamingAggregate : public DPBaseOperator {
public:
    DPStreamingAggregate(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPAggregateNode> dpAggregateNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpAggregateNode,
              dpAggregateNode->isPartial() ? "DPPartialStreamingAggregate" : "DPStreamingAggregate",
              dpAggregateNode->getQflow(),
              std::nullopt),
               isPartialOutput(dpAggregateNode->isPartial()) {}

    DPStreamingAggregate(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPAggregateNode> dpAggregateNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpAggregateNode,
              operatorType,
              dpAggregateNode->getQflow(),
              std::nullopt),
              isPartialOutput(dpAggregateNode->isPartial()) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPStreamingAggregate>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPAggregateNode>(planNode_),
            fusedOperatorType()
        );
    }

    bool needsInput() const override {
        return true;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        // auto inputPtr = toRawPointer(input);
        // if (isPartialOutput) {
        //     evaluateDFGPartialBuild(dfg_instance_id_, inputPtr);
        // } else {
        //     evaluateDFGBuild(dfg_instance_id_, inputPtr);
        // }
    }

    bool isFinished() override {
        return noMoreInput_ && finished_;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        // dfgNoMoreInput(dfg_instance_id_);
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        // if (dfgResultHasNext(dfg_instance_id_)) {
        //     const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
        //     return fromRawPointer(outputPtr);   
        // } else {
        //     finished_ = true;
        //     return nullptr;
        // }
        return nullptr;
    }
    

private:
    bool isPartialOutput;
    bool finished_ = false;
};

class DPHashJoinBuild : public DPBaseOperator {
public:
    DPHashJoinBuild(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPHashJoinNode> dpHashJoinNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              true,
              dpHashJoinNode,
              "DPHashJoinBuild",
              dpHashJoinNode->getQflow(),
              // TODO: avoid spill for build side for now for simplicity
              // TODO: add spill config later
              std::nullopt) {}

    DPHashJoinBuild(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPHashJoinNode> dpHashJoinNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpHashJoinNode,
              operatorType,
              dpHashJoinNode->getQflow(),
              // TODO: avoid spill for build side for now for simplicity
              // TODO: add spill config later
              std::nullopt) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPHashJoinBuild>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPHashJoinNode>(planNode_),
            fusedOperatorType()
        );
    }

    bool needsInput() const override {
        return !buildFinished_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        // auto inputPtr = toRawPointer(input);
        // evaluateDFGBuild(dfg_instance_id_, inputPtr);
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        // dfgNoMoreInput(dfg_instance_id_);
        buildFinished_ = true;
    }

    bool isFinished() override {
        return buildFinished_;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    facebook::velox::RowVectorPtr getOutput() override {
        return nullptr;
    }

private:
    bool buildFinished_ = false;
};

class DPHashJoinProbe : public DPBaseOperator {
public:
    DPHashJoinProbe(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPHashJoinNode> dpHashJoinNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpHashJoinNode,
              "DPHashJoinProbe",
              dpHashJoinNode->getQflow(),
              dpHashJoinNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}

    DPHashJoinProbe(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPHashJoinNode> dpHashJoinNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpHashJoinNode,
              operatorType,
              dpHashJoinNode->getQflow(),
              dpHashJoinNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPHashJoinProbe>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPHashJoinNode>(planNode_),
            fusedOperatorType()
        );
    }

    bool needsInput() const override {
        return !input_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        input_ = std::move(input);
    }

    bool isFinished() override {
        return noMoreInput_;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (!input_) {
            return nullptr;
        }

        initialize();
        auto result = evaluate(input_);
        input_ = nullptr;
        return result;
    }

    facebook::velox::RowVectorPtr evaluate(const facebook::velox::RowVectorPtr& input) {
        // auto inputPtr = toRawPointer(input);
        // const uint8_t* outputPtr = evaluateDFG(dfg_instance_id_, inputPtr);
        // return fromRawPointer(outputPtr);
        return nullptr;
    }

private:
    bool finished_ = false;    
};

class DPMergeSource : public DPBaseOperator {
public:
    DPMergeSource(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPMergeJoinNode> dpMergeJoinNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpMergeJoinNode,
              "DPMergeSource",
              dpMergeJoinNode->getQflow(),
              std::nullopt) {}

    DPMergeSource(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPMergeJoinNode> dpMergeJoinNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpMergeJoinNode,
              operatorType,
              dpMergeJoinNode->getQflow(),
              std::nullopt) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPMergeSource>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPMergeJoinNode>(planNode_),
            fusedOperatorType()
        );
    }

    void initialize() override {
        if (!initialized_) {
            initialized_ = true;
            // dfg_instance_id_ = compileDFGMergeSource(qflow_);
        }
    }

    bool needsInput() const override {
        return !state_.rlock()->atEnd;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        auto state = state_.wlock();
        if (!state->atEnd) {
            state->data = std::move(input);
            notify(consumerPromise_);
        }
    }

    facebook::velox::RowVectorPtr getOutput() override {
        facebook::velox::RowVectorPtr result;
        facebook::velox::ContinueFuture future;
        auto reason = next(&future, &result);
        if (reason == facebook::velox::exec::BlockingReason::kNotBlocked) {
            return result;
        }
        return nullptr;
    }

    void noMoreInput() {
        auto state = state_.wlock();
        state->atEnd = true;
        notify(consumerPromise_);
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        auto state = state_.rlock();
        if (state->data == nullptr && !state->atEnd) {
            producerPromise_ = facebook::velox::ContinuePromise("DPMergeSource::isBlocked");
            *future = producerPromise_->getSemiFuture();
            return facebook::velox::exec::BlockingReason::kWaitForProducer;
        }
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
        return state_.rlock()->atEnd && state_.rlock()->data == nullptr;
    }

    /**
     * @brief Retrieves the next row vector from the DFG merge source.
     *
     * This method is responsible for fetching the next row vector from the DFG merge source.
     * It checks the current state of the operator to determine if it should wait for more input
     * or if it can proceed with fetching the next row vector.
     *
     * @param future The future object used to handle blocking scenarios.
     * @param data The pointer to store the resulting row vector.
     * 
     * @return The blocking reason indicating if the operation is blocked or not.
     */
    facebook::velox::exec::BlockingReason next(
        facebook::velox::ContinueFuture* future,
        facebook::velox::RowVectorPtr* data) {
        auto state = state_.wlock();
        if (state->data != nullptr) {
            *data = std::move(state->data);
            notify(producerPromise_);
            return facebook::velox::exec::BlockingReason::kNotBlocked;
        }

        if (state->atEnd) {
            *data = nullptr;
            return facebook::velox::exec::BlockingReason::kNotBlocked;
        }

        // const uint8_t* outputPtr = dfgMergeSourceNext(dfg_instance_id_);
        // if (outputPtr != nullptr) {
        //     *data = fromRawPointer(outputPtr);
        //     return facebook::velox::exec::BlockingReason::kNotBlocked;
        // }

        consumerPromise_ = facebook::velox::ContinuePromise("DPMergeSource::next");
        *future = consumerPromise_->getSemiFuture();
        return facebook::velox::exec::BlockingReason::kWaitForProducer;
    }

    /**
     * @brief Enqueues a row vector into the DFG merge source.
     *
     * This method is responsible for enqueuing a row vector into the DFG merge source.
     * It checks the current state of the operator to determine if it should wait for more input
     * or if it can proceed with enqueuing the row vector.
     *
     * @param data The row vector to be enqueued.
     * @param future The future object used to handle blocking scenarios.
     * 
     * @return The blocking reason indicating if the operation is blocked or not.
     */
    facebook::velox::exec::BlockingReason enqueue(
        facebook::velox::RowVectorPtr data,
        facebook::velox::ContinueFuture* future) {
        auto state = state_.wlock();
        if (state->atEnd) {
            notify(consumerPromise_);
            return facebook::velox::exec::BlockingReason::kNotBlocked;
        }

        if (data == nullptr) {
            state->atEnd = true;
            // dfgMergeSourceNoMoreInput(dfg_instance_id_);
            notify(consumerPromise_);
            return facebook::velox::exec::BlockingReason::kNotBlocked;
        }

        // auto inputPtr = toRawPointer(data);
        // dfgMergeSourceEnqueue(dfg_instance_id_, inputPtr);

        // if (dfgMergeSourceIsFull(dfg_instance_id_)) {
        //     producerPromise_ = facebook::velox::ContinuePromise("DPMergeSource::enqueue");
        //     *future = producerPromise_->getSemiFuture();
        //     return facebook::velox::exec::BlockingReason::kWaitForConsumer;
        // }

        notify(consumerPromise_);
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    void close() override {
        // dfgMergeSourceClose(dfg_instance_id_);
        state_.wlock()->data = nullptr;
        state_.wlock()->atEnd = true;
        notify(producerPromise_);
        notify(consumerPromise_);
        DPBaseOperator::close();
    }

private:
    struct State {
        bool atEnd = false;
        /**
         * @brief The current data row vector being processed.
         *
         * This pointer holds the reference to the current row vector that the operator
         * is processing. It is used to pass data between different stages of the data
         * flow within the operator.
         */
        facebook::velox::RowVectorPtr data;
    };

    folly::Synchronized<State> state_;

    std::optional<facebook::velox::ContinuePromise> consumerPromise_;
    std::optional<facebook::velox::ContinuePromise> producerPromise_;

    facebook::velox::exec::BlockingReason waitForConsumer(facebook::velox::ContinueFuture* future) {
        producerPromise_ = facebook::velox::ContinuePromise("DPMergeSource::waitForConsumer");
        *future = producerPromise_->getSemiFuture();
        return facebook::velox::exec::BlockingReason::kWaitForConsumer;
    }

    void notify(std::optional<facebook::velox::ContinuePromise>& promise) {
        if (promise) {
            promise->setValue();
            promise.reset();
        }
    }
    
};

class DPMergeJoin : public DPBaseOperator {
public:
    DPMergeJoin(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPMergeJoinNode> dpMergeJoinNode)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpMergeJoinNode,
              "DPMergeJoin",
              dpMergeJoinNode->getQflow(),
              std::nullopt),
          dpMergeJoinNode_(std::move(dpMergeJoinNode)) {}

    DPMergeJoin(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        std::shared_ptr<const DPMergeJoinNode> dpMergeJoinNode,
        std::string operatorType)
        : DPBaseOperator(
              operatorId,
              driverCtx,
              dpMergeJoinNode,
              operatorType,
              dpMergeJoinNode->getQflow(),
              std::nullopt) {}

    std::unique_ptr<DPBaseOperator> clone() const override {
        return std::make_unique<DPMergeJoin>(
            -1,
            operatorCtx_->driverCtx(),
            std::static_pointer_cast<const DPMergeJoinNode>(planNode_),
            fusedOperatorType()
        );
    }

    void initialize() override {
        if (!initialized_) {
            initialized_ = true;
            // dfg_instance_id_ = compileDFGMergeJoin(qflow_);
        }
    }

    bool needsInput() const override {
        // return dfgMergeJoinNeedsInput(dfg_instance_id_);
        return true;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        // auto inputPtr = toRawPointer(input);
        // dfgMergeJoinAddInput(dfg_instance_id_, inputPtr);
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        // if (dfgMergeJoinIsBlocked(dfg_instance_id_)) {
        //     *future = std::move(futureRightSideInput_);
        //     return facebook::velox::exec::BlockingReason::kWaitForMergeJoinRightSide;
        // }
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    facebook::velox::RowVectorPtr getOutput() override {
        // const uint8_t* outputPtr = dfgMergeJoinGetOutput(dfg_instance_id_);
        // if (outputPtr == nullptr) {
        //     return nullptr;
        // }
        // return fromRawPointer(outputPtr);
        return nullptr;
    }

    bool isFinished() override {
        // return dfgMergeJoinIsFinished(dfg_instance_id_);
        return true;
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        // dfgMergeJoinNoMoreInput(dfg_instance_id_);
    }

    void close() override {
        // dfgMergeJoinClose(dfg_instance_id_);
        DPBaseOperator::close();
    }

private:
    std::shared_ptr<const DPMergeJoinNode> dpMergeJoinNode_;
    facebook::velox::ContinueFuture futureRightSideInput_{facebook::velox::ContinueFuture::makeEmpty()};
};


} // namespace gluten
