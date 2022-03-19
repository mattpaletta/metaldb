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
    using instruction_serialized_type = std::vector<instruction_serialized_value_type>;

    static std::string columnTypeToString(ColumnType type) {
        switch (type) {
        case String:
            return "String";
        case Float:
            return "Float";
        case Integer:
            return "Integer";
        }
    }

    namespace detail {
        template<typename T>
        void serializeVector(instruction_serialized_type* output, const std::vector<T>& input) {
            output->emplace_back((instruction_serialized_value_type) input.size());
            for (const auto& c : input) {
                output->emplace_back((instruction_serialized_value_type) c);
            }
        }

        template<typename T>
        std::vector<T> deserializeVector(instruction_serialized_value_type** input) {
            using count_type = std::int8_t;
            static_assert(sizeof(count_type) == sizeof(instruction_serialized_value_type), "Unsafe to statically cast values of different sizes.");
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

    class ParseRow final {
    public:
        ParseRow(Method method, const std::vector<ColumnType>& columnTypes) : _method(method), _columnTypes(columnTypes) {}

        std::size_t numColumns() const {
            return this->_columnTypes.size();
        }

        bool operator==(const ParseRow& other) const {
            return this->_method == other._method && this->_columnTypes == other._columnTypes;
        }

        std::string description() const {
            std::stringstream sstream;

            sstream << "ParseRow Method: " << ParseRow::methodToString(this->_method) << " ";
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

        static ParseRow deserialize(instruction_serialized_value_type** input) {
            // Assume we don't have the type encoded
            // pull out the method
            auto method = (Method) **input;
            (*input)++;

            // Then the vector
            const auto columnTypes = detail::deserializeVector<ColumnType>(input);
            return ParseRow(method, columnTypes);
        }

        instruction_serialized_type serialize() const {
            instruction_serialized_type output;
            output.emplace_back((instruction_serialized_value_type) this->_method);
            detail::serializeVector(&output, this->_columnTypes);
            return output;
        }

    private:
        Method _method;
        std::vector<ColumnType> _columnTypes;
    };

    class Projection final {
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

        static Projection deserialize(instruction_serialized_value_type** input) {
            // Assume we don't have the type encoded

            // Pull out the vector elements
            const auto input_values = detail::deserializeVector<std::size_t>(input);
            return Projection(input_values);
        }

        instruction_serialized_type serialize() const {
            instruction_serialized_type output;
            detail::serializeVector(&output, this->_indexes);
            return output;
        }

    private:
        std::vector<std::size_t> _indexes;
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
            this->_data.emplace_back(static_cast<instruction_serialized_value_type>(type));
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
