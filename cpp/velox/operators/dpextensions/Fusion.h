// Consolidate adjacent DP operators into a single one to reduce DP <-> Velox data exchange.

#pragma once

#include <vector>
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/operators/dpextensions/Operators.h"

namespace gluten {

class DPOperatorConsolidationAdapter {
 public:
  bool adapt(const facebook::velox::exec::DriverFactory& driverFactory, facebook::velox::exec::Driver& driver) {
    auto operators = driver.operators();
    std::vector<facebook::velox::exec::Operator*> dpOperators;

    printOperatorInfo(operators, "DP Operator Consolidation started:");

    for (int32_t i = 0; i < operators.size(); ++i) {
      auto* op = operators[i];
      if (auto* dpOp = dynamic_cast<DPBaseOperator*>(op)) {
        dpOperators.push_back(dpOp);
      } else {
        if (!dpOperators.empty() && dpOperators.size() > 1) {
          consolidateOperators(driverFactory, driver, i - dpOperators.size(), i);
          dpOperators.clear();
        }
      }
    }

    if (!dpOperators.empty() && dpOperators.size() > 1) {
      consolidateOperators(driverFactory, driver, operators.size() - dpOperators.size(), operators.size());
    }

    // Print debug information after consolidation
    printOperatorInfo(driver.operators(), "DP Operator Consolidation finished:");

    return true;
  }

 private:
  void consolidateOperators(
      const facebook::velox::exec::DriverFactory& driverFactory,
      facebook::velox::exec::Driver& driver,
      int32_t start,
      int32_t end) {
    if (start >= end) {
      return;
    }

    auto operators = driver.operators();
    auto* lastOperator = dynamic_cast<DPBaseOperator*>(operators[end - 1]);
    VELOX_CHECK(lastOperator != nullptr, "Expected a DPBaseOperator");

    // Create a new name that reflects the merged operations
    std::string newName = "(";
    for (int32_t i = start; i < end - 1; ++i) {
      if (i != start) {
        newName += "->";
      }
      newName += operators[i]->operatorType();
    }
    newName += ")";

    // Create a clone of the last operator and set its name to annotate the consolidation.
    auto lastOperatorPtr = lastOperator->clone();
    lastOperatorPtr->setName(newName);

    // Replace the range of operators with just the last operator
    std::vector<std::unique_ptr<facebook::velox::exec::Operator>> replaceWith;
    replaceWith.push_back(std::move(lastOperatorPtr));

    DLOG(INFO) << "Replacing operators from position " << start << " to " << end << std::endl;
    auto replaced = driverFactory.replaceOperators(driver, start, end, std::move(replaceWith));

    printOperatorInfo(driver.operators(), "Consolidation Result:");
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
void registerDPOperatorConsolidationAdapter() {
  facebook::velox::exec::DriverAdapter dpAdapter{
      "DPOperatorConsolidation",
      {},
      [](const facebook::velox::exec::DriverFactory& factory, facebook::velox::exec::Driver& driver) {
        auto adapter = DPOperatorConsolidationAdapter();
        return adapter.adapt(factory, driver);
      }};
  facebook::velox::exec::DriverFactory::registerAdapter(dpAdapter);
}

} // namespace gluten
