#include "bridge.h"
#include <vector>
#include <iostream>

uint32_t cpp_function(const std::string& s) {
    return s.length();
}

void dummy_calling_rust() {
  std::vector<uint8_t> bytes = {1, 2, 3, 4, 5};

  // Call Rust function directly with rust::Slice
  bool can_offload = can_offload_to_dp(rust::Slice<const uint8_t>(bytes.data(), bytes.size()));

  // Use the result
  if (can_offload) {
    std::cout << "Can offload to DP" << std::endl;
  } else {
    std::cout << "Cannot offload to DP" << std::endl;
  }
}

bool canOffloadToDP(const std::vector<uint8_t>& serializedRel) {
  return can_offload_to_dp(rust::Slice<const uint8_t>(serializedRel.data(), serializedRel.size()));
}

uint64_t compileSubstraitToDFG(const std::vector<uint8_t>& serializedRel) {
  return compile_substrait_to_dfg(rust::Slice<const uint8_t>(serializedRel.data(), serializedRel.size()));
}

uint8_t const *evaluateDFG(uint64_t dfgInstanceId, uint8_t const *inputPtr) {
  return evaluate(dfgInstanceId, inputPtr);
}

void evaluateDFGBuild(uint64_t dfgInstanceId, uint8_t const *inputPtr) {
  evaluate_build(dfgInstanceId, inputPtr);
}

void evaluateDFGPartialBuild(uint64_t dfgInstanceId, uint8_t const *inputPtr) {
  evaluate_partial_build(dfgInstanceId, inputPtr);
}

void dfgNoMoreInput(uint64_t dfgInstanceId) {
  no_more_input(dfgInstanceId);
}

bool dfgResultHasNext(uint64_t dfgInstanceId) {
  return result_has_next(dfgInstanceId);
}

uint8_t const *dfgResultNext(uint64_t dfgInstanceId) {
  return result_next(dfgInstanceId);
}

void dfgClose(uint64_t dfgInstanceId) {
  dfg_close(dfgInstanceId);
}

uint64_t dfgMemoryUsage(uint64_t dfgInstanceId) {
  return memory_usage(dfgInstanceId);
}

uint64_t dfgSpill(uint64_t dfgInstanceId) {
  return spill(dfgInstanceId);
}

// MergeSource functions
int64_t compileDFGMergeSource(const std::vector<uint8_t>& serializedRel) {
  return compile_dfg_merge_source(rust::Slice<const uint8_t>(serializedRel.data(), serializedRel.size()));
}

const uint8_t* dfgMergeSourceNext(int64_t dfgInstanceId) {
  return dfg_merge_source_next(dfgInstanceId);
}

void dfgMergeSourceEnqueue(int64_t dfgInstanceId, const uint8_t* inputPtr) {
  dfg_merge_source_enqueue(dfgInstanceId, inputPtr);
}

bool dfgMergeSourceIsFull(int64_t dfgInstanceId) {
  return dfg_merge_source_is_full(dfgInstanceId);
}

void dfgMergeSourceNoMoreInput(int64_t dfgInstanceId) {
  dfg_merge_source_no_more_input(dfgInstanceId);
}

void dfgMergeSourceClose(int64_t dfgInstanceId) {
  dfg_merge_source_close(dfgInstanceId);
}

// MergeJoin functions
int64_t compileDFGMergeJoin(const std::vector<uint8_t>& serializedRel) {
  return compile_dfg_merge_join(rust::Slice<const uint8_t>(serializedRel.data(), serializedRel.size()));
}

bool dfgMergeJoinNeedsInput(int64_t dfgInstanceId) {
  return dfg_merge_join_needs_input(dfgInstanceId);
}

void dfgMergeJoinAddInput(int64_t dfgInstanceId, const uint8_t* inputPtr) {
  dfg_merge_join_add_input(dfgInstanceId, inputPtr);
}

bool dfgMergeJoinIsBlocked(int64_t dfgInstanceId) {
  return dfg_merge_join_is_blocked(dfgInstanceId);
}

const uint8_t* dfgMergeJoinGetOutput(int64_t dfgInstanceId) {
  return dfg_merge_join_get_output(dfgInstanceId);
}

bool dfgMergeJoinIsFinished(int64_t dfgInstanceId) {
  return dfg_merge_join_is_finished(dfgInstanceId);
}

void dfgMergeJoinNoMoreInput(int64_t dfgInstanceId) {
  dfg_merge_join_no_more_input(dfgInstanceId);
}

void dfgMergeJoinClose(int64_t dfgInstanceId) {
  dfg_merge_join_close(dfgInstanceId);
}
