//
//  Instructions.hpp
//  metaldb
//
//  Created by Matthew Paletta on 2022-03-13.
//

#pragma once

#include "engine.h"

#include <sstream>
#include <string>

namespace metaldb::engine {
    using instruction_serialized_type = std::vector<InstSerializedValue>;

    static std::string columnTypeToString(ColumnType type) {
        switch (type) {
        case String:
            return "String";
        case Float:
            return "Float";
        case Integer:
            return "Integer";
        case Unknown:
            return "Unknown";
        }
    }

    namespace detail {
        template<typename T, typename Size>
        void serializeVector(instruction_serialized_type* output, const std::vector<T>& input, const Size& size) {
            WriteBytesStartingAt(*output, size);
            for (const auto& c : input) {
                WriteBytesStartingAt(*output, c);
            }
        }

        template<typename T>
        std::vector<T> deserializeVector(InstSerializedValue** input) {
            using count_type = std::int8_t;
            static_assert(sizeof(count_type) == sizeof(InstSerializedValue), "Unsafe to statically cast values of different sizes.");
            const auto size = (count_type) **input;
            (*input)++;

            std::vector<T> input_values(size);
            for (count_type i = 0; i < size; ++i) {
                input_values.at(i) = (T) **input;
                (*input)++;
            }

            return input_values;
        }
    }

    class ParseRow final : ParseRowInstruction {
    public:
        ParseRow(MethodType method, const std::vector<ColumnType>& columnTypes, SkipHeaderType skipHeader) : _method(method), _columnTypes(columnTypes), _skipHeader(skipHeader) {}

        NumColumnsType numColumns() const {
            return this->_columnTypes.size();
        }

        bool operator==(const ParseRow& other) const {
            return this->_method == other._method && this->_columnTypes == other._columnTypes && this->_skipHeader == other._skipHeader;
        }

        std::string description() const {
            std::stringstream sstream;

            sstream << "ParseRow Method: " << ParseRow::methodToString(this->_method) << " ";
            sstream << "Skip Header: " << this->_skipHeader << " ";
            sstream << "Column Types (" << this->_columnTypes.size() << ") ";
            for (const auto& type : this->_columnTypes) {
                sstream << columnTypeToString(type) << " ";
            }

            return sstream.str();
        }

        static std::string methodToString(Method method) {
            switch (method) {
            case CSV:
                return "CSV";
            }
        }

        static ParseRow deserialize(InstSerializedValue** input) {
            // Assume we don't have the type encoded
            // pull out the method
            auto method = ReadBytesStartingAt<MethodType>(input);
            (*input) += sizeof(MethodType);

            bool skipHeader = ReadBytesStartingAt<SkipHeaderType>(input);
            (*input) += sizeof(SkipHeaderType);

            // Then the vector
            const auto columnTypes = detail::deserializeVector<ColumnType>(input);
            return ParseRow(method, columnTypes, skipHeader);
        }

        instruction_serialized_type serialize() const {
            instruction_serialized_type output;
            WriteBytesStartingAt(output, this->_method);
            WriteBytesStartingAt(output, this->_skipHeader);
            detail::serializeVector(&output, this->_columnTypes, (NumColumnsType) this->_columnTypes.size());
            return output;
        }

    private:
        Method _method;
        std::vector<ColumnType> _columnTypes;
        ParseRowInstruction::SkipHeaderType _skipHeader;
    };

    class Projection final : ProjectionInstruction {
    public:
        Projection(const std::vector<std::size_t>& columnIndexes) : _indexes(columnIndexes) {}

        std::size_t numColumns() const {
            return this->_indexes.size();
        }

        bool operator==(const Projection& other) const {
            return this->_indexes == other._indexes;
        }

        std::string description() const {
            std::stringstream sstream;

            sstream << "Projection (" << this->_indexes.size() << ") ";
            for (const auto& col : this->_indexes) {
                sstream << col << " ";
            }

            return sstream.str();
        }

        static Projection deserialize(InstSerializedValue** input) {
            // Assume we don't have the type encoded

            // Pull out the vector elements
            const auto input_values = detail::deserializeVector<std::size_t>(input);
            return Projection(input_values);
        }

        instruction_serialized_type serialize() const {
            instruction_serialized_type output;
            detail::serializeVector(&output, this->_indexes, (NumColumnsType) this->_indexes.size());
            return output;
        }

    private:
        std::vector<std::size_t> _indexes;
    };

    class Output final {
    public:
        Output() = default;

        std::string description() const {
            std::stringstream sstream;

            sstream << "Output()";

            return sstream.str();
        }

        static Output deserialize(InstSerializedValue** input) {
            // Assume we don't have the type encoded
            return Output();
        }

        instruction_serialized_type serialize() const {
            instruction_serialized_type output;
            return output;
        }
    };


    // Encoder/Decoder
    class Encoder final {
    public:
        Encoder() : _data({0}) {}

        Encoder& encode(const class ParseRow& parseRow) {
            return this->encodeImpl(parseRow, PARSEROW);
        }

        Encoder& encode(const class Projection& projection) {
            return this->encodeImpl(projection, PROJECTION);
        }

        Encoder& encode(const class Output& output) {
            return this->encodeImpl(output, OUTPUT);
        }

        instruction_serialized_type data() const {
            return this->_data;
        }

    private:
        instruction_serialized_type _data;

        Encoder(instruction_serialized_type&& data) : _data(std::move(data)) {}

        template<typename T>
        Encoder& encodeImpl(const T& instruction, InstructionType type) {
            // The first element stores the number of elements
            this->_data.at(0) += 1;

            auto buffer = instruction.serialize();

            // Copy into vector
            this->_data.emplace_back(static_cast<InstSerializedValue>(type));
            std::copy(buffer.begin(), buffer.end(), std::back_inserter(this->_data));
            return *this;
        }
    };

    class Decoder final {
    public:
        Decoder(instruction_serialized_type& data) : _data(data) {}

        template<typename T>
        T decode() {
            auto* startPtr = &this->_data.at(this->index);
            auto* endPtr = startPtr;
            T value = T::deserialize(&endPtr);

            // It must have moved this many spaces.
            this->index += (endPtr - startPtr);
            return value;
        }

        InstructionType decodeType() {
            return (InstructionType) this->_data.at(this->index++);
        }

        bool hasNext() const {
            return this->index < this->_data.size();
        }

        std::size_t numInstructions() const {
            return (std::size_t) this->_data.at(0);
        }

    private:
        std::size_t index = 1;
        instruction_serialized_type& _data;
    };
}
