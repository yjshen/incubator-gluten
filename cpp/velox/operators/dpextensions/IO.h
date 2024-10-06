// Data in and out cross Velox and DP runtime

#pragma once

#include "velox/common/base/BitUtil.h"
#include "velox/type/Type.h"
#include "velox/vector/TypeAliases.h"
#include "sparkle.h"
#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include <fmt/format.h>

template <>
struct fmt::formatter<SparkleStatus_t> : formatter<int> {
    auto format(SparkleStatus_t status, format_context& ctx) const {
        return formatter<int>::format(static_cast<int>(status), ctx);
    }
};

using vector_size_t = int32_t;

namespace gluten {

/** Extract data from a Velox RowVector and send it to Sparkle.
 *
 * This class is responsible for extracting data from a Velox RowVector and sending it to Sparkle as raw pointers.
 */
class VeloxDataExtractor {
public:
    VeloxDataExtractor(SparkleHandle_t handle, const facebook::velox::RowVectorPtr& input, facebook::velox::RowTypePtr& inputType)
        : handle_(handle), input_(input), inputType_(inputType) {}

    SparkleStatus_t extractData() {
        auto metadata_status = sparkle_send_metadata(handle_, input_->size(), input_->childrenSize());
        if (metadata_status != SparkleSuccess) {
            return metadata_status;
        }

        for (int i = 0; i < input_->childrenSize(); i++) {
            auto child = input_->childAt(i);
            auto childType = inputType_->childAt(i);
            
            auto status = extractColumn(child, childType);
            if (status != SparkleSuccess) {
                return status;
            }
        }

        return SparkleSuccess;
    }

private:
    SparkleHandle_t handle_;
    facebook::velox::RowVectorPtr input_;
    facebook::velox::RowTypePtr inputType_;

    // Result pointers
    uint8_t* nulls_ = nullptr;
    uint64_t nulls_len_ = 0;
    uint8_t* values_ = nullptr;
    uint64_t values_len_ = 0;
    std::vector<uint8_t*> string_data_;
    std::vector<uint64_t> string_data_len_;

    SparkleStatus_t extractColumn(const facebook::velox::VectorPtr& child, const facebook::velox::TypePtr& childType) {
        resetResultPointers();

        switch (childType->kind()) {
            case facebook::velox::TypeKind::BOOLEAN:
                extractBoolean(child);
                break;
            case facebook::velox::TypeKind::TINYINT:
            case facebook::velox::TypeKind::SMALLINT:
            case facebook::velox::TypeKind::INTEGER:
            case facebook::velox::TypeKind::BIGINT:
            case facebook::velox::TypeKind::REAL:
            case facebook::velox::TypeKind::DOUBLE:
                extractFixedWidth(child, childType);
                break;
            case facebook::velox::TypeKind::VARCHAR:
            case facebook::velox::TypeKind::VARBINARY:
                extractVariableWidth(child, childType);
                break;
            case facebook::velox::TypeKind::ARRAY:
                extractArray(child, childType);
                break;
            case facebook::velox::TypeKind::MAP:
                extractMap(child, childType);
                break;
            case facebook::velox::TypeKind::ROW:
                extractRow(child, childType);
                break;
            default:
                return SparkleUnimplemented;
        }

        uint8_t type = static_cast<uint8_t>(childType->kind());
        return sparkle_send_data(handle_, type, nulls_, nulls_len_, values_, values_len_, string_data_, string_data_len_);
    }

    void resetResultPointers() {
        nulls_ = nullptr;
        nulls_len_ = 0;
        values_ = nullptr;
        values_len_ = 0;
        string_data_.clear();
        string_data_len_.clear();
    }

    void extractBoolean(const facebook::velox::VectorPtr& vector) {
        handleEncodings<bool>(vector);
    }

    void extractFixedWidth(const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        switch (type->kind()) {
            case facebook::velox::TypeKind::TINYINT:
                handleEncodings<int8_t>(vector);
                break;
            case facebook::velox::TypeKind::SMALLINT:
                handleEncodings<int16_t>(vector);
                break;
            case facebook::velox::TypeKind::INTEGER:
                handleEncodings<int32_t>(vector);
                break;
            case facebook::velox::TypeKind::BIGINT:
                handleEncodings<int64_t>(vector);
                break;
            case facebook::velox::TypeKind::REAL:
                handleEncodings<float>(vector);
                break;
            case facebook::velox::TypeKind::DOUBLE:
                handleEncodings<double>(vector);
                break;
            default:
                // This should never happen due to the switch in extractColumn
                break;
        }
    }

    void extractVariableWidth(const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        handleEncodings<facebook::velox::StringView>(vector);
    }

    void extractArray(const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        // Implementation for array extraction
    }

    void extractMap(const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        // Implementation for map extraction
    }

    void extractRow(const facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        // Implementation for row extraction
    }

    template<typename T>
    void handleEncodings(const facebook::velox::VectorPtr& vector) {
        switch (vector->encoding()) {
            case facebook::velox::VectorEncoding::Simple::FLAT:
                extractFlat<T>(vector);
                break;
            case facebook::velox::VectorEncoding::Simple::CONSTANT:
                extractConstant<T>(vector);
                break;
            case facebook::velox::VectorEncoding::Simple::DICTIONARY:
                extractDictionary<T>(vector);
                break;
            // Add cases for other encodings as needed
            default:
                // Handle unsupported encoding
                break;
        }
    }

    template<typename T>
    void extractFlat(const facebook::velox::VectorPtr& vector) {
        // Implementation for flat vector extraction
    }

    template<typename T>
    void extractConstant(const facebook::velox::VectorPtr& vector) {
        // Implementation for constant vector extraction
    }

    template<typename T>
    void extractDictionary(const facebook::velox::VectorPtr& vector) {
        // Implementation for dictionary vector extraction
    }
};

/**
 * @brief Receives data from Sparkle and constructs Velox RowVectors.
 *
 * This class is responsible for receiving data from Sparkle and constructing
 * corresponding Velox RowVectors. It handles both fixed-width and variable-length
 * data types, and manages the creation and population of vectors efficiently.
 */
class SparkleDataReceiver {
public:
    /**
     * @brief Constructs a SparkleDataReceiver.
     *
     * @param handle Handle to the Sparkle instance.
     * @param outputType The RowType describing the structure of the output.
     * @param pool Memory pool for allocations.
     */
    SparkleDataReceiver(SparkleHandle_t handle, 
                        const facebook::velox::RowTypePtr& outputType, 
                        facebook::velox::memory::MemoryPool* pool)
        : handle_(handle), outputType_(outputType), pool_(pool) {}

    /**
     * @brief Receives data from Sparkle and constructs a RowVector.
     *
     * This method orchestrates the entire process of receiving data from Sparkle,
     * including metadata retrieval, vector creation, and data population.
     *
     * @return A RowVector containing the received data.
     */
    facebook::velox::RowVectorPtr receiveData() {
        uint64_t rowCount, bufCount;
        auto status = sparkle_recv_metadata(handle_, rowCount, bufCount);
        VELOX_CHECK_EQ(status, SparkleSuccess, "Failed to receive metadata");

        auto result = createRowVector(rowCount);

        for (int i = 0; i < outputType_->size(); ++i) {
            auto childType = outputType_->childAt(i);
            auto child = result->childAt(i);
            receiveColumn(i, child, childType);
        }

        return result;
    }

private:
    SparkleHandle_t handle_;
    facebook::velox::RowTypePtr outputType_;
    facebook::velox::memory::MemoryPool* pool_;

    /**
     * @brief Creates a RowVector with the specified size.
     *
     * This method creates a RowVector and its child vectors based on the outputType.
     * For variable-length types, it retrieves length information from Sparkle.
     *
     * @param size The number of rows in the vector.
     * @return A RowVector with appropriately sized and typed child vectors.
     */
    facebook::velox::RowVectorPtr createRowVector(vector_size_t size) {
        std::vector<facebook::velox::VectorPtr> children;
        for (int i = 0; i < outputType_->size(); ++i) {
            children.push_back(createVector(outputType_->childAt(i), size, i));
        }
        return std::make_shared<facebook::velox::RowVector>(
            pool_, outputType_, nullptr, size, std::move(children));
    }

    /**
     * @brief Creates a vector of the specified type and size.
     *
     * This method creates either a fixed-width or variable-length vector based on the type.
     * For variable-length types, it retrieves length information from Sparkle.
     *
     * @param type The type of the vector.
     * @param size The number of elements in the vector.
     * @param columnIndex The index of the column in the RowVector.
     * @return A vector of the specified type and size.
     */
    facebook::velox::VectorPtr createVector(const facebook::velox::TypePtr& type, vector_size_t size, int columnIndex) {
        switch (type->kind()) {
            case facebook::velox::TypeKind::BOOLEAN:
            case facebook::velox::TypeKind::TINYINT:
            case facebook::velox::TypeKind::SMALLINT:
            case facebook::velox::TypeKind::INTEGER:
            case facebook::velox::TypeKind::BIGINT:
            case facebook::velox::TypeKind::REAL:
            case facebook::velox::TypeKind::DOUBLE:
            case facebook::velox::TypeKind::TIMESTAMP:
                return facebook::velox::BaseVector::create(type, size, pool_);
            case facebook::velox::TypeKind::VARCHAR:
            case facebook::velox::TypeKind::VARBINARY:
                return createVariableLengthVector(type, size, columnIndex);
            case facebook::velox::TypeKind::ARRAY:
            case facebook::velox::TypeKind::MAP:
            case facebook::velox::TypeKind::ROW:
                // TODO: Implement complex types
                VELOX_NYI("Complex types not yet implemented");
            default:
                VELOX_FAIL("Unsupported type kind: {}", type->toString());
        }
    }

    /**
     * @brief Creates a variable-length vector at the specified column index.
     *
     * @param type The type of the vector.
     * @param size The number of elements in the vector.
     * @param columnIndex The index of the column in the RowVector.
     * @return A vector of the specified type and size.
     */
    facebook::velox::VectorPtr createVariableLengthVector(const facebook::velox::TypePtr& type, vector_size_t size, int columnIndex) {
        std::vector<uint64_t> lengths(size);
        auto status = sparkle_recv_variable_length_info(
            handle_, 
            columnIndex,
            static_cast<uint8_t>(type->kind()), 
            lengths);
        VELOX_CHECK_EQ(status, SparkleSuccess, "Failed to receive variable length info");

        // TODO: return correct vector based on type
        return nullptr;
    }

    /**
     * @brief Receives data for a single column.
     *
     * This method delegates to the appropriate receive method based on whether
     * the column is fixed-width or variable-length.
     *
     * @param columnIndex The index of the column.
     * @param vector The vector to fill with data.
     * @param type The type of the column.
     */
    void receiveColumn(int columnIndex, facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        switch (type->kind()) {
            case facebook::velox::TypeKind::BOOLEAN:
            case facebook::velox::TypeKind::TINYINT:
            case facebook::velox::TypeKind::SMALLINT:
            case facebook::velox::TypeKind::INTEGER:
            case facebook::velox::TypeKind::BIGINT:
            case facebook::velox::TypeKind::REAL:
            case facebook::velox::TypeKind::DOUBLE:
            case facebook::velox::TypeKind::TIMESTAMP:
                receiveFixedWidthColumn(columnIndex, vector, type);
                break;
            case facebook::velox::TypeKind::VARCHAR:
            case facebook::velox::TypeKind::VARBINARY:
                receiveVariableLengthColumn(columnIndex, vector, type);
                break;
            case facebook::velox::TypeKind::ARRAY:
            case facebook::velox::TypeKind::MAP:
            case facebook::velox::TypeKind::ROW:
                // TODO: Implement complex types
                VELOX_NYI("Complex types not yet implemented");
            default:
                VELOX_FAIL("Unsupported type kind: {}", type->toString());
        }
    }

    /**
     * @brief Receives data for a fixed-width column.
     *
     * This method receives nulls and values for a fixed-width column from Sparkle
     * and populates the provided vector.
     *
     * @param columnIndex The index of the column.
     * @param vector The vector to fill with data.
     * @param type The type of the column.
     */
    void receiveFixedWidthColumn(int columnIndex, facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        // TODO
    }

    /**
     * @brief Receives data for a variable-length column.
     *
     * This method receives variable-length data from Sparkle and populates
     * the provided vector. It assumes the vector has already been created
     * with the correct buffer size.
     *
     * @param columnIndex The index of the column.
     * @param vector The vector to fill with data.
     * @param type The type of the column.
     */
    void receiveVariableLengthColumn(int columnIndex, facebook::velox::VectorPtr& vector, const facebook::velox::TypePtr& type) {
        // TODO
    }
};

/** 
 * @brief Push a RowVector to Sparkle.
 *
 * @param handle Handle pointing to a specific DFG instance
 * @param input Input data we get from Velox and want to send to Sparkle
 * @param inputType Type of the input data
 * @return SparkleStatus_t Status of the operation
 */
SparkleStatus_t addInput(
        SparkleHandle_t handle,
        const facebook::velox::RowVectorPtr& input,
        facebook::velox::RowTypePtr& inputType) {
    VeloxDataExtractor extractor(handle, input, inputType);
    return extractor.extractData();
}

/**
 * @brief Creates a RowVector from data received from Sparkle.
 * 
 * @param handle Handle to the Sparkle instance.
 * @param outputType The RowType describing the structure of the output.
 * @param pool Memory pool for allocations.
 * @return A RowVector containing the received data.
 */
facebook::velox::RowVectorPtr getOutput(
    SparkleHandle_t handle,
    const facebook::velox::RowTypePtr& outputType,
    facebook::velox::memory::MemoryPool* pool) {
    SparkleDataReceiver receiver(handle, outputType, pool);
    return receiver.receiveData();
}


} // namespace gluten
