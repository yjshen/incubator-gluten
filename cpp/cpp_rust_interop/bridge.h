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

// MergeSource functions
int64_t compileDFGMergeSource(const std::vector<uint8_t>& serializedRel);

const uint8_t* dfgMergeSourceNext(int64_t dfgInstanceId);

void dfgMergeSourceEnqueue(int64_t dfgInstanceId, const uint8_t* inputPtr);

bool dfgMergeSourceIsFull(int64_t dfgInstanceId);

void dfgMergeSourceNoMoreInput(int64_t dfgInstanceId);

void dfgMergeSourceClose(int64_t dfgInstanceId);

// MergeJoin functions
int64_t compileDFGMergeJoin(const std::vector<uint8_t>& serializedRel);

bool dfgMergeJoinNeedsInput(int64_t dfgInstanceId);

void dfgMergeJoinAddInput(int64_t dfgInstanceId, const uint8_t* inputPtr);

bool dfgMergeJoinIsBlocked(int64_t dfgInstanceId);

const uint8_t* dfgMergeJoinGetOutput(int64_t dfgInstanceId);

bool dfgMergeJoinIsFinished(int64_t dfgInstanceId);

void dfgMergeJoinNoMoreInput(int64_t dfgInstanceId);

void dfgMergeJoinClose(int64_t dfgInstanceId);
