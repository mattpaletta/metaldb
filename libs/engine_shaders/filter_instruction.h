#pragma once

#include "constants.h"
#include "instruction_type.h"
#include "stack.h"
#include "string_section.h"

namespace metaldb {
    /**
     * Filters a row and either returns the empty row or the row as it was, based on evaluating a predicate using a very small
     * stack-based virtual machine.
     */
    class FilterInstruction final {
    public:
        METAL_CONSTANT static constexpr auto MAX_VM_STACK_SIZE = 32;

        enum Operation : InstSerializedValue {
            READ_FLOAT_CONSTANT,
            READ_INT_CONSTANT,
            READ_STRING_CONSTANT,
            READ_FLOAT_COLUMN,
            READ_INT_COLUMN,
            READ_STRING_COLUMN,
            CAST_FLOAT_INT,
            CAST_INT_FLOAT,
            GT_FLOAT,
            GT_INT,
            LT_FLOAT,
            LT_INT,
            GTE_FLOAT,
            GTE_INT,
            EQ_FLOAT,
            EQ_INT,
            NE_FLOAT,
            NE_INT
        };

        using NumOperationsType = uint16_t;
        METAL_CONSTANT static constexpr auto NumOperationsOffset = 0;

        using OperationsType = InstSerializedValue;
        METAL_CONSTANT static constexpr auto OperationOffset = sizeof(NumOperationsType) + NumOperationsOffset;

        FilterInstruction(InstSerializedValuePtr instructions) CPP_NOEXCEPT : _instructions(instructions) {}

        CPP_PURE_FUNC NumOperationsType NumOperations() const CPP_NOEXCEPT {
            return ReadBytesStartingAt<NumOperationsType>(&this->_instructions[NumOperationsOffset]);
        }

        CPP_PURE_FUNC OperationsType GetOperation(NumOperationsType op) const CPP_NOEXCEPT {
            const auto index = OperationOffset + (op * sizeof(NumOperationsType));
            return ReadBytesStartingAt<OperationsType>(&this->_instructions[index]);
        }

        /**
         * Returns a pointer 1 past the end of the filter instruction.  This will either be an unknown if we exceed the end of the array or
         * an encoded @b InstructionType .
         */
        CPP_PURE_FUNC InstSerializedValuePtr End() const CPP_NOEXCEPT {
            // Returns 1 past the end of the instruction
            const auto index = OperationOffset + sizeof(OperationsType) * this->NumOperations();
            return &this->_instructions[index];
        }

        CPP_PURE_FUNC TempRow GetRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) const CPP_NOEXCEPT {
            if (this->ShouldIncludeRow(row, constants)) {
                return row;
            } else {
                // If excluded, return the empty row.
                return TempRow();
            }
        }

    private:
        // Taken from C++ standard
        METAL_CONSTANT static constexpr float floatEpsilon = 1.19209e-07f;

        InstSerializedValuePtr _instructions;

        CPP_PURE_FUNC size_t IndexOfValue(size_t i) const CPP_NOEXCEPT {
            return sizeof(this->NumOperations()) + (i * sizeof(InstSerializedValue));
        }

        CPP_PURE_FUNC InstSerializedValue GetValue(size_t i) const CPP_NOEXCEPT {
            const auto index = this->IndexOfValue(i);
            return *((InstSerializedValue METAL_DEVICE *) &this->_instructions[index]);
        }

        template<typename T>
        CPP_PURE_FUNC T GetTypeStartingAtByte(size_t i) const CPP_NOEXCEPT {
            union {
                T a;
                InstSerializedValue bytes[sizeof(T)];
            } thing;

            for (auto n = 0UL; n < sizeof(T); ++n) {
                thing.bytes[n] = this->GetValue(i + n);
            }

            return thing.a;
        }

        CPP_PURE_FUNC types::FloatType GetFloatStartingAtByte(size_t i) const CPP_NOEXCEPT {
            return this->GetTypeStartingAtByte<types::FloatType>(i);
        }

        CPP_PURE_FUNC types::IntegerType GetIntStartingAtByte(size_t i) const CPP_NOEXCEPT {
            return this->GetTypeStartingAtByte<types::IntegerType>(i);
        }

        CPP_PURE_FUNC StringSection GetStringStartingAtByte(size_t i) const CPP_NOEXCEPT {
            const auto length = this->GetIntStartingAtByte(i);
            const auto startStringIndex = this->IndexOfValue(i + 1);
            return StringSection((char METAL_DEVICE *) &this->_instructions[startStringIndex], length);
        }

        CPP_PURE_FUNC bool ShouldIncludeRow(TempRow METAL_THREAD & row, DbConstants METAL_THREAD & constants) const CPP_NOEXCEPT {
            Stack<MAX_VM_STACK_SIZE> stack;

            // TODO: Add support and operations for null.
            size_t operationIndex = 0;
            for (auto i = 0; i < this->NumOperations(); ++i) {
                switch (this->GetOperation(operationIndex++)) {
                case READ_FLOAT_CONSTANT: {
                    auto val = this->GetFloatStartingAtByte(operationIndex);
                    operationIndex += sizeof(val);
                    stack.Push<types::FloatType>(val);
                    break;
                }
                case READ_INT_CONSTANT: {
                    auto val = this->GetIntStartingAtByte(operationIndex);
                    operationIndex += sizeof(val);
                    stack.Push<types::IntegerType>(val);
                    break;
                }
                case READ_STRING_CONSTANT: {
                    // TODO: Do I need new scratch space for all my strings?
                    // Ideally wouldn't copy them from their original location

                    auto val = this->GetStringStartingAtByte(operationIndex);
                    operationIndex += val.Size();
                    for (auto j = 0UL; j < val.Size(); ++j) {
                        // Push each character of the string onto the stack (reverse order)
                        auto ch = val.C_Str()[val.Size() - 1 - j];
                        stack.Push(ch);
                    }
                    // Push the length of the string
                    stack.Push<types::IntegerType>(val.Size());
                    break;
                }
                case READ_FLOAT_COLUMN: {
                    const auto column = this->GetIntStartingAtByte(operationIndex++);
                    const auto val = row.ReadColumnFloat(column);
                    stack.Push<types::FloatType>(val);
                    break;
                }
                case READ_INT_COLUMN: {
                    const auto column = this->GetIntStartingAtByte(operationIndex++);
                    const auto val = row.ReadColumnInt(column);
                    stack.Push<types::IntegerType>(val);
                    break;
                }
                case READ_STRING_COLUMN: {
                    // TODO: Do I need new scratch space for all my strings?
                    // Ideally wouldn't copy them from their original location

                    const auto column = this->GetIntStartingAtByte(operationIndex++);
                    const auto val = row.ReadColumnString(column);
                    operationIndex += val.Size();
                    for (auto j = 0UL; j < val.Size(); ++j) {
                        // Push each character of the string onto the stack (reverse order)
                        auto ch = val.C_Str()[val.Size() - 1 - j];
                        stack.Push(ch);
                    }
                    // Push the length of the string
                    stack.Push<types::IntegerType>(val.Size());
                    break;
                }
                case CAST_FLOAT_INT: {
                    const auto floatVal = stack.Pop<types::FloatType>();
                    const auto intVal = (types::IntegerType) floatVal;
                    stack.Push<types::IntegerType>(intVal);
                    break;
                }
                case CAST_INT_FLOAT: {
                    const auto intVal = stack.Pop<types::IntegerType>();
                    const auto floatVal = (types::FloatType) intVal;
                    stack.Push<types::FloatType>(floatVal);
                    break;
                }
                case GT_FLOAT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
                    const auto comp = valA > valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case GT_INT: {
                    const auto valA = stack.Pop<types::IntegerType>();
                    const auto valB = stack.Pop<types::IntegerType>();
                    const auto comp = valA > valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case LT_FLOAT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
                    const auto comp = valA < valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case LT_INT: {
                    const auto valA = stack.Pop<types::IntegerType>();
                    const auto valB = stack.Pop<types::IntegerType>();
                    const auto comp = valA < valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case GTE_FLOAT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
                    const auto comp = valA >= valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case GTE_INT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
                    const auto comp = valA >= valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case EQ_FLOAT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
#ifdef __METAL__
                    const auto comp = (metal::abs(valA) - metal::abs(valB) <= floatEpsilon) ? 1 : 0;
#else
                    const auto comp = (abs(valA) - abs(valB) <= floatEpsilon) ? 1 : 0;
#endif
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case EQ_INT: {
                    const auto valA = stack.Pop<types::IntegerType>();
                    const auto valB = stack.Pop<types::IntegerType>();
                    const auto comp = valA == valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case NE_FLOAT: {
                    const auto valA = stack.Pop<types::FloatType>();
                    const auto valB = stack.Pop<types::FloatType>();
#ifdef __METAL__
                    const auto comp = (metal::abs(valA) - metal::abs(valB) > floatEpsilon) ? 1 : 0;
#else
                    const auto comp = (abs(valA) - abs(valB) > floatEpsilon) ? 1 : 0;
#endif
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                case NE_INT: {
                    const auto valA = stack.Pop<types::IntegerType>();
                    const auto valB = stack.Pop<types::IntegerType>();
                    const auto comp = valA != valB ? 1 : 0;
                    stack.Push<types::IntegerType>(comp);
                    break;
                }
                }
            }

            if (stack.Size() < sizeof(types::IntegerType)) {
                // The row got us into an invalid state.
                return false;
            }

            // Must end with an int
            return stack.Pop<types::IntegerType>() ? true : false;
        }
    };
}
