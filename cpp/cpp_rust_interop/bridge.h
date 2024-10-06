#pragma once

#include <cstdint>
#include <vector>
#include <string>

typedef enum {
  SparkleSuccess = 0,
  SparkleInvalidSubstraitPlan,
  SparkleQFlowIR,
  SparkleNotConnected,
  SparkleConnectError,
  SparkleSendError,
  SparkleAcceptError,
  SparkleReceiveError,
  SparkleReceivedEof,
  SparkleUnimplemented,
} SparkleStatus_t;

typedef void *SparkleHandle_t;

/// @brief Converts one substrait plan into another by replacing all nodes in
/// the input plan that can be executed by sparkle backend with a sparkle node
/// @param input Input plan to convert
/// @param output Converted plan
/// @return 0, i.e., 'SparkleSuccess' if successfully converted, non-zero
/// otherwise
SparkleStatus_t sparkle_plan(std::string &input, std::string &output);

/// @brief Initializes the sparkle backend to compile a plan and run it. User
/// must call this API first and all other Sparkle API(s) take the handle
/// retuned by this function.
/// @return A handle to the sparkle backend. Returns 'nullptr' if init fails
SparkleHandle_t sparkle_init();

/// @brief Destroys the given sparkle handle. If the backend is still running,
/// it will be cancelled
/// @param handle Sparkle handle to destroy
void sparkle_clean(SparkleHandle_t handle);

/// @brief Compiles the given qflow plan and launches the backend
/// @param handle Handle to the sparkle backend
/// @param ir QFlow IR to compile and launch
/// @return 0, i.e., 'SparkleSuccess' if successfully launched, non-zero
/// otherwise
SparkleStatus_t sparkle_launch(SparkleHandle_t handle, std::vector<std::string> &irs);

/// @brief Sends the metadata of input batch of rows to sparkle backend.
/// Metadata must be sent to the backend before the data for each column
/// @param handle Handle to sparkle backend
/// @param row_count Number of rows in the batch
/// @param buf_count Number of columns in the batch
/// @return 0, i.e., 'SparkleSuccess' if successfully sent, non-zero otherwise
SparkleStatus_t sparkle_send_metadata(SparkleHandle_t handle,
                                      uint64_t row_count, uint64_t buf_count);

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
SparkleStatus_t sparkle_send_data(SparkleHandle_t handle, int32_t type,
                                  uint8_t *nulls, uint64_t nulls_len,
                                  uint8_t *values, uint64_t values_len,
                                  std::vector<uint8_t *> &string_data,
                                  std::vector<uint64_t> &string_data_len);

/// @brief Closes the sparkle backend input port to notify EOF
/// @param handle Handle to the sparkle backend
/// @return 0, i.e., 'SparkleSuccess' if successfully closed, non-zero otherwise
SparkleStatus_t sparkle_send_done(SparkleHandle_t handle);

/// @brief Reads the metadata of output batch of rows from sparkle backend.
/// Metadata must be received from the backend before the data for each column
/// @param handle Handle to sparkle backend
/// @param row_count Number of rows in the batch
/// @param buf_count Number of columns in the batch
/// @return 0, i.e., 'SparkleSuccess' if successfully received, non-zero
/// otherwise
SparkleStatus_t sparkle_recv_metadata(SparkleHandle_t handle,
                                      uint64_t &row_count, uint64_t &buf_count);

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
 * @param nulls_len Length of the null bitmap in bytes
 * @param values_len Length of the values buffer in bytes
 * @param[out] lengths A vector to be filled with the lengths of each variable-length element.
 *
 * @return SparkleStatus_t indicating the success or failure of the operation.
 *
 * @note The size of the `lengths` vector should be pre-allocated to match the number of rows
 *       obtained from sparkle_recv_metadata. This function fills the vector without resizing it. 
 */
SparkleStatus_t sparkle_recv_data_lengths(SparkleHandle_t handle,
                                          uint32_t column_index,
                                          uint8_t &type,
                                          uint64_t &nulls_len,
                                          uint64_t &values_len,
                                          std::vector<uint64_t>& lengths);

/// @brief Receives a column in output input batch of rows from sparkle backend
/// @param handle Handle to the sparkle backend
/// @param column_index The index of the column for which to retrieve data
/// @param nulls Pointer to the null bitmap
/// @param values Pointer to the values
/// @param string_data Pointers to string data buffer
/// @return 0, i.e., 'SparkleSuccess' if successfully received, non-zero
/// otherwise
SparkleStatus_t sparkle_recv_data(SparkleHandle_t handle,
                                  uint32_t column_index,
                                  uint8_t *nulls,
                                  uint8_t *values,
                                  std::vector<uint8_t *> &string_data);
