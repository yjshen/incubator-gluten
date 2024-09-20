#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include "rust/cxx.h"
#include "dp_executor/src/lib.rs.h"

uint32_t cpp_function(const std::string& s);

bool canOffloadToDP(const std::vector<uint8_t>& serializedRel);

uint64_t compileSubstraitToDFG(const std::vector<uint8_t>& serializedRel);

uint8_t const *evaluateDFG(uint64_t dfgInstanceId, uint8_t const *inputPtr);

void evaluateDFGBuild(uint64_t dfgInstanceId, uint8_t const *inputPtr);

void evaluateDFGPartialBuild(uint64_t dfgInstanceId, uint8_t const *inputPtr);

void dfgNoMoreInput(uint64_t dfgInstanceId);

bool dfgResultHasNext(uint64_t dfgInstanceId);

uint8_t const *dfgResultNext(uint64_t dfgInstanceId);

void dfgClose(uint64_t dfgInstanceId);

uint64_t dfgMemoryUsage(uint64_t dfgInstanceId);

uint64_t dfgSpill(uint64_t dfgInstanceId);
