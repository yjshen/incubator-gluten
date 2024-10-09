/*
 * Adds a Resize Operator in between DP and Velox operator when output of DP operator is consumed
 * by velox operator
 */

#pragma once

#include <vector>
#include "config/GlutenConfig.h"
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/operators/dpextensions/Operators.h"

namespace gluten {

class DPBatchResizerAdapter {
 public:
  bool adapt(const facebook::velox::exec::DriverFactory& driverFactory, facebook::velox::exec::Driver& driver) {
    const auto& queryConfig = driver.driverCtx()->queryConfig();
    auto enabled = queryConfig.get<bool>(kDPBatchResizerEnabled, true);
    if(!enabled) {
        DLOG(INFO) << "dp batch resize feature is not enabled" << std::endl;
        return true;
    }
    printOperatorInfo(driver.operators(), "List of operators before adding resize: ");
    addResizeOp(driverFactory, driver);
    printOperatorInfo(driver.operators(), "List of operators before adding resize: ");
    return true;
  }

 private:
    void addResizeOp(
      const facebook::velox::exec::DriverFactory& driverFactory,
      facebook::velox::exec::Driver& driver) {
        auto operators = driver.operators();
        std::deque<facebook::velox::exec::Operator*> operatorList;
        operatorList.push_front(operators[operators.size() - 1]);
        for (int32_t i = operators.size() - 2; i >= 0; i--) {
           auto* op = operators[i];
           if (auto* dpOp = dynamic_cast<DPBaseOperator*>(op)) {
               auto* nextOp = operators[i + 1];
               if (auto* dpOpNext = dynamic_cast<DPBaseOperator*>(nextOp); dpOpNext == nullptr) {
                 auto* resizeOp = new ResizeOperator(-1, driver.driverCtx(),
                  dpOp->getPlanNode()->outputType());
                 operatorList.push_front(resizeOp);
               }
           }
           operatorList.push_front(op);
        }
        // Convert deque to vector of std::unique_ptr<Operator>
        std::vector<std::unique_ptr<facebook::velox::exec::Operator>> uniqueOperatorList;
        uniqueOperatorList.reserve(operatorList.size());  // Reserve space in the vector
        for (auto* op : operatorList) {
           uniqueOperatorList.push_back(std::unique_ptr<facebook::velox::exec::Operator>(op));  // Wrap raw pointer in unique_ptr
        }
        // Now call the replaceOperators function
        driverFactory.replaceOperators(driver, 0, operators.size(), std::move(uniqueOperatorList));
    }

    void printOperatorInfo(const std::vector<facebook::velox::exec::Operator*>& ops, const std::string& message) const {
    DLOG(INFO) << message << std::endl;
        for (int32_t i = 0; i < ops.size(); ++i) {
          DLOG(INFO) << "  ID " << ops[i]->operatorId()
                    << ", Type " << ops[i]->toString() << std::endl;
        }
    }
};

// Register the adapter
void registerDPBatchResizerAdapter() {
  facebook::velox::exec::DriverAdapter dpAdapter{
      "DPBatchResizer",
      {},
      [](const facebook::velox::exec::DriverFactory& factory, facebook::velox::exec::Driver& driver) {
        auto adapter = DPBatchResizerAdapter();
        return adapter.adapt(factory, driver);
      }};
  facebook::velox::exec::DriverFactory::registerAdapter(dpAdapter);
}

} // namespace gluten
