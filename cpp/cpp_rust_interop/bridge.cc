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

/// @brief Converts one substrait plan into another by replacing all nodes in
/// the input plan that can be executed by sparkle backend with a sparkle node
/// @param input Input plan to convert
/// @param output Converted plan
/// @return 0, i.e., 'SparkleSuccess' if successfully converted, non-zero
/// otherwise
SparkleStatus_t sparkle_plan(std::string &input, std::string &output) {
  return SparkleInvalidSubstraitPlan;
}

/// @brief Initializes the sparkle backend to compile a plan and run it. User
/// must call this API first and all other Sparkle API(s) take the handle
/// retuned by this function.
/// @return A handle to the sparkle backend. Returns 'nullptr' if init fails
SparkleHandle_t sparkle_init() {
  return nullptr;
}

/// @brief Destroys the given sparkle handle. If the backend is still running,
/// it will be cancelled
/// @param handle Sparkle handle to destroy
void sparkle_clean(SparkleHandle_t handle) {
  // do nothing
}

/// @brief Compiles the given qflow plan and launches the backend
/// @param handle Handle to the sparkle backend
/// @param ir QFlow IR to compile and launch
/// @return 0, i.e., 'SparkleSuccess' if successfully launched, non-zero
/// otherwise
SparkleStatus_t sparkle_launch(SparkleHandle_t handle, std::vector<std::string> &irs) {
  return SparkleSuccess;
}

/// @brief Sends the metadata of input batch of rows to sparkle backend.
/// Metadata must be sent to the backend before the data for each column
/// @param handle Handle to sparkle backend
/// @param row_count Number of rows in the batch
/// @param buf_count Number of columns in the batch
/// @return 0, i.e., 'SparkleSuccess' if successfully sent, non-zero otherwise
SparkleStatus_t sparkle_send_metadata(SparkleHandle_t handle,
                                      uint64_t row_count, uint64_t buf_count) {
  return SparkleSuccess;
}

/// @brief Send a column in the input batch of rows to sparkle backend
/// @param handle Handle to the sparkle backend
/// @param type Type of the column
/// @param nulls Pointer to the null bitmap
/// @param nulls_len Length of the null bitmap in bytes
/// @param values Pointer to the values
/// @param values_len Length of the values buffer in bytes
/// @param string_data_count TBD
/// @param string_data TBD
/// @param string_data_len TBD
/// @return 0, i.e., 'SparkleSuccess' if successfully sent, non-zero otherwise
SparkleStatus_t sparkle_send_data(SparkleHandle_t handle, uint8_t type,
                                  uint8_t *nulls, uint64_t nulls_len,
                                  uint8_t *values, uint64_t values_len,
                                  std::vector<uint8_t *> &string_data,
                                  std::vector<uint64_t> &string_data_len) {
  return SparkleSuccess;
}

/// @brief Closes the sparkle backend input port to notify EOF
/// @param handle Handle to the sparkle backend
/// @return 0, i.e., 'SparkleSuccess' if successfully closed, non-zero otherwise
SparkleStatus_t sparkle_send_done(SparkleHandle_t handle) {
  return SparkleSuccess;
}

/// @brief Reads the metadata of output batch of rows from sparkle backend.
/// Metadata must be received from the backend before the data for each column
/// @param handle Handle to sparkle backend
/// @param row_count Number of rows in the batch
/// @param buf_count Number of columns in the batch
/// @return 0, i.e., 'SparkleSuccess' if successfully received, non-zero
/// otherwise
SparkleStatus_t sparkle_recv_metadata(SparkleHandle_t handle,
                                        uint64_t &row_count, uint64_t &buf_count) {
  return SparkleSuccess;
}

/**
 * @brief Retrieves information about variable-length data for a specific column.
 *
 * This function fetches metadata about variable-length data (such as strings or binary)
 * for a specified column from Sparkle. It returns the number of elements and their
 * individual lengths.
 *
 * @param handle Handle to the sparkle backend
 * @param column_index The index of the column for which to retrieve information.
 * @param type The data type of the column (as defined in Velox's type system).
 * @param[out] lengths A vector to be filled with the lengths of each variable-length element.
 *
 * @return SparkleStatus_t indicating the success or failure of the operation.
 *
 * @note The size of the `lengths` vector should be pre-allocated to match the number of rows
 *       obtained from sparkle_recv_metadata. This function fills the vector without resizing it.
 */
SparkleStatus_t sparkle_recv_variable_length_info(SparkleHandle_t handle,
                                                  uint32_t column_index,
                                                  uint8_t type,
                                                  std::vector<uint64_t>& lengths) {
  return SparkleSuccess;
}

/// @brief Receives a column in output input batch of rows from sparkle backend
/// @param handle Handle to the sparkle backend
/// @param type Type of the column
/// @param nulls Pointer to the null bitmap
/// @param nulls_len Length of the null bitmap in bytes
/// @param values Pointer to the values
/// @param values_len Length of the values buffer in bytes
/// @param string_data Pointers to string data buffer
/// @param string_data_len Pointers to string data buffer lengths
/// @return 0, i.e., 'SparkleSuccess' if successfully received, non-zero
/// otherwise
SparkleStatus_t sparkle_recv_data(SparkleHandle_t handle, uint8_t &type,
                                  uint8_t *&nulls, uint64_t &nulls_len,
                                  uint8_t *&values, uint64_t &values_len,
                                  std::vector<uint8_t *> &string_data,
                                  std::vector<uint64_t> &string_data_len) {
  return SparkleSuccess;
}
