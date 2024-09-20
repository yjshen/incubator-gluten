// Velox operators are analogy to Physical Operators in Spark, and are the basic
// building blocks of a Velox executable pipeline that could be later driven by [[velox::exec::Driver]]
// to be actually executing over the data.
//
// A DP operator is the operator supported by DP runtime, wraps the pointer for a DFG, and delegates the
// actual execution to the DP runtime.

#pragma once

#include "Nodes.h"
#include "velox/exec/Operator.h"
#include "cpp_rust_interop/bridge.h"
#include <cstdint>

namespace gluten {

class DPBaseOperator : public facebook::velox::exec::Operator {
protected:
    bool initialized_ = false;
    uint64_t dfg_instance_id_ = 0;
    const std::vector<uint8_t>& serialized_substrait_;

public:
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        const std::vector<uint8_t>& serialized_substrait)
        : Operator(driverCtx, planNode->outputType(), operatorId, planNode->id(), planNode->name().data()),
          serialized_substrait_(serialized_substrait) {}


    // Constructor with spillConfig
    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        const std::vector<uint8_t>& serialized_substrait,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              planNode->name().data(),
              spillConfig),
          serialized_substrait_(serialized_substrait) {}

    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        std::string operatorType,
        const std::vector<uint8_t>& serialized_substrait,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              operatorType,
              spillConfig),
          serialized_substrait_(serialized_substrait) {}

    DPBaseOperator(
        int32_t operatorId,
        facebook::velox::exec::DriverCtx* driverCtx,
        bool noOutput,
        const std::shared_ptr<const facebook::velox::core::PlanNode>& planNode,
        std::string operatorType,
        const std::vector<uint8_t>& serialized_substrait,
        std::optional<facebook::velox::common::SpillConfig> spillConfig)
        : Operator(
              driverCtx,
              planNode->outputType(),
              operatorId,
              planNode->id(),
              operatorType,
              spillConfig),
          serialized_substrait_(serialized_substrait) {}

    virtual void initialize() {
        if (!initialized_) {
            // Call Rust code to compile substrait to DFG instance
            dfg_instance_id_ = compileSubstraitToDFG(serialized_substrait_);
            initialized_ = true;
        }
    }

    void close() override {
        Operator::close();
        dfgClose(dfg_instance_id_);
    }

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
              dpProjectNode->getSerializedSubstrait()) {}

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
        auto inputPtr = toRawPointer(input);
        const uint8_t* outputPtr = evaluateDFG(dfg_instance_id_, inputPtr);
        return fromRawPointer(outputPtr);
    }
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
              dpFilterNode->getSerializedSubstrait()) {}

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

        // Dummy implementation of filter to just return the first row
        if (input->size() == 0) {
            return nullptr;
        }
        return std::static_pointer_cast<facebook::velox::RowVector>(input->slice(0, 1));
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
              dpOrderByNode->getSerializedSubstrait(),
              dpOrderByNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}


    bool needsInput() const override {
        return !finished_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        auto inputPtr = toRawPointer(input);
        evaluateDFGBuild(dfg_instance_id_, inputPtr);
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        dfgNoMoreInput(dfg_instance_id_);
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        if (dfgResultHasNext(dfg_instance_id_)) {
            const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
            return fromRawPointer(outputPtr);
        } else {
            finished_ = true;
            return nullptr;
        }
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    bool isFinished() override {
        return finished_;
    }

    void reclaim(uint64_t targetBytes, facebook::velox::memory::MemoryReclaimer::Stats& stats)
        override {
            auto freed = dfgSpill(dfg_instance_id_);
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
              dpAggregateNode->isPartial() ? "PartialHashAggregate" : "HashAggregate",
              dpAggregateNode->getSerializedSubstrait(),
              dpAggregateNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt),
                isPartialOutput(dpAggregateNode->isPartial()) {}

    bool needsInput() const override {
        return !noMoreInput_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        auto inputPtr = toRawPointer(input);
        if (isPartialOutput) {
            evaluateDFGPartialBuild(dfg_instance_id_, inputPtr);
        } else {
            evaluateDFGBuild(dfg_instance_id_, inputPtr);
        }
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        dfgNoMoreInput(dfg_instance_id_);
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        if (dfgResultHasNext(dfg_instance_id_)) {
            const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
            return fromRawPointer(outputPtr);   
        } else {
            finished_ = true;
            return nullptr;
        }
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }
    
    bool isFinished() override {
        return finished_;
    }

    void reclaim(uint64_t targetBytes, facebook::velox::memory::MemoryReclaimer::Stats& stats)
        override {
            auto freed = dfgSpill(dfg_instance_id_);
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
              dpAggregateNode->isPartial() ? "PartialStreamingAggregate" : "StreamingAggregate",
              dpAggregateNode->getSerializedSubstrait(),
              std::nullopt),
               isPartialOutput(dpAggregateNode->isPartial()) {}


    bool needsInput() const override {
        return true;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        auto inputPtr = toRawPointer(input);
        if (isPartialOutput) {
            evaluateDFGPartialBuild(dfg_instance_id_, inputPtr);
        } else {
            evaluateDFGBuild(dfg_instance_id_, inputPtr);
        }
    }

    bool isFinished() override {
        return noMoreInput_ && finished_;
    }

    facebook::velox::exec::BlockingReason isBlocked(facebook::velox::ContinueFuture* future) override {
        return facebook::velox::exec::BlockingReason::kNotBlocked;
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        dfgNoMoreInput(dfg_instance_id_);
    }

    facebook::velox::RowVectorPtr getOutput() override {
        if (finished_ || !noMoreInput_) {
            return nullptr;
        }

        initialize();
        if (dfgResultHasNext(dfg_instance_id_)) {
            const uint8_t* outputPtr = dfgResultNext(dfg_instance_id_);
            return fromRawPointer(outputPtr);   
        } else {
            finished_ = true;
            return nullptr;
        }
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
              "HashJoinBuild",
              dpHashJoinNode->getSerializedSubstrait(),
              // TODO: avoid spill for build side for now for simplicity
              // TODO: add spill config later
              std::nullopt) {}

    bool needsInput() const override {
        return !buildFinished_;
    }

    void addInput(facebook::velox::RowVectorPtr input) override {
        initialize();
        auto inputPtr = toRawPointer(input);
        evaluateDFGBuild(dfg_instance_id_, inputPtr);
    }

    void noMoreInput() override {
        Operator::noMoreInput();
        dfgNoMoreInput(dfg_instance_id_);
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
              "HashJoinProbe",
              dpHashJoinNode->getSerializedSubstrait(),
              dpHashJoinNode->canSpill(driverCtx->queryConfig())
               ? driverCtx->makeSpillConfig(operatorId)
               : std::nullopt) {}

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
        auto inputPtr = toRawPointer(input);
        const uint8_t* outputPtr = evaluateDFG(dfg_instance_id_, inputPtr);
        return fromRawPointer(outputPtr);
    }

private:
    bool finished_ = false;    
};


} // namespace gluten
