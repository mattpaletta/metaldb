#pragma once

#include "constants.h"
#include "raw_table.h"
#include "instruction_type.h"
#include "parse_row_instruction.h"
#include "projection_instruction.h"
#include "output_instruction.h"
#include "method.h"
#include "temp_row.h"
#include "db_constants.h"
#include "vm.h"

#ifndef __METAL__
#include <iostream>
#endif

namespace metaldb {
    // Declare functions for procesing row here to get all the imports
    inline void OutputInstruction::WriteRow(InstructionPtr METAL_THREAD * decodedInstructions, size_t numDecodedInstructions, DbConstants METAL_THREAD & constants) {
        // Get prev
        const auto indexLastInstruction = 2*(numDecodedInstructions-1);
        const auto lastInstructionType = (InstructionPtr) decodedInstructions[indexLastInstruction];

        TempRow row;
        if (lastInstructionType == PARSEROW) {
            auto parseRowInstruction = ((ParseRowInstruction METAL_THREAD *) decodedInstructions[indexLastInstruction+1]);
            row = parseRowInstruction->GetRow(constants);
        } else if (lastInstructionType == PROJECTION) {
            auto projectionInstruction = ((ProjectionInstruction METAL_THREAD *) decodedInstructions[indexLastInstruction+1]);
            row = projectionInstruction->GetRow(decodedInstructions, numDecodedInstructions - 1, constants);
        }

        // Write row into output.
    }

    inline TempRow ProjectionInstruction::GetRow(InstructionPtr METAL_THREAD * decodedInstructions, size_t numDecodedInstructions, DbConstants METAL_THREAD & constants) {
        // Get prev
        const auto indexLastInstruction = 2*(numDecodedInstructions-1);
        const auto lastInstructionType = (InstructionPtr) decodedInstructions[indexLastInstruction];

        TempRow row;
        if (lastInstructionType == PARSEROW) {
            auto parseRowInstruction = ((ParseRowInstruction METAL_THREAD *) decodedInstructions[indexLastInstruction+1]);
            row = parseRowInstruction->GetRow(constants);
        } else if (lastInstructionType == PROJECTION) {
            auto projectionInstruction = ((ProjectionInstruction METAL_THREAD *) decodedInstructions[indexLastInstruction+1]);
            row = projectionInstruction->GetRow(decodedInstructions, numDecodedInstructions - 1, constants);
        }

#ifndef __METAL__
        std::cout << "Doing Projection" << std::endl;
#endif

        // Do the projection
        TempRow::TempRowBuilder builder;
        {
            builder.numColumns = this->numColumns();
#ifndef __METAL__
            std::cout << "Got N columns: " << (int)this->numColumns() << std::endl;
#endif

            // Read column types
            for (int8_t i = 0; i < this->numColumns(); ++i) {
                auto columnToRead = this->getColumnIndex(i);
#ifndef __METAL__
                std::cout << "Reading column: " << (int)columnToRead << std::endl;
#endif
                // Set all column types
                auto columnType = row.ColumnType(columnToRead);
                builder.columnTypes[i] = columnType;

#ifndef __METAL__
                // TODO: Why is the column type not being recorded correctly.
                std::cout << "Column type: " << ((int)columnType) << std::endl;
#endif

                // Set all column sizes, and they might get pruned
                if (columnType == String) {
                    builder.columnSizes[i] = row.ColumnSize(columnToRead);
                } else {
                    builder.columnSizes[i] = 0;
                }

#ifndef __METAL__
                std::cout << "Column Size: " << (int)builder.columnSizes[i] << std::endl;
#endif
            }
        }
        TempRow newRow = builder;
        // Copy the columns we are interested in
        for (size_t i = 0; i < this->numColumns(); ++i) {
            const auto columnToRead = this->getColumnIndex(i);

            // Read from row into newRow
            newRow.append(row.data(row.ColumnStartOffset(columnToRead)), row.ColumnSize(columnToRead));
        }

        return newRow;
    }

    static InstructionType decodeType(METAL_DEVICE int8_t* instruction) {
        return (InstructionType) *instruction;
    }

    // Build up a stack of decoded instructions in the call stack
    static void runExpression(InstructionPtr METAL_THREAD * decodedInstructions, size_t numDecodedInstructions, DbConstants METAL_THREAD & constants) {
        // Start at the end, use the call stack as the VM stack, but backwards.
        if (numDecodedInstructions == 0) {
            // This is just safety
            return;
        }

        // TODO: how to handle sorting & aggregations? - same way, sync after creating each group/sorted.
        // Start by lazily processing each row.
        const auto indexLastInstruction = 2*(numDecodedInstructions-1);
#ifndef __METAL__
        assert((InstructionPtr) decodedInstructions[indexLastInstruction] == metaldb::OUTPUT);
#endif
        {
            auto outputInstruction = ((metaldb::OutputInstruction METAL_THREAD *) decodedInstructions[indexLastInstruction+1]);
            outputInstruction->WriteRow(decodedInstructions, numDecodedInstructions - 1, constants);
        }

        if (numDecodedInstructions == 1) {
            // This is the last one, sort prefix sum, write into output buffer.
        }
    }


    // Push each instruction onto the call stack, stored in `decodedInstructions`
    // Each instruction upon decoding has an 'end' method that returns the address of 1 past the end of the decoded instruction in the buffer.
    // When all instructions are decoded, can evaluate the expression using a VM based on the Metal stack.
    static void decodeInstruction(InstructionPtr METAL_THREAD * decodedInstructions, size_t numDecodedInstructions, int8_t numInstructions, METAL_DEVICE int8_t* instructions, DbConstants METAL_THREAD & constants) {
        if (numInstructions == 0) {
            // When no more instructions to decode
            // Run the instructions
            // Need to keep calling into the stack otherwise we'll loose the decoded instructions on the call stack.
            runExpression(decodedInstructions, numDecodedInstructions, constants);
            return;
        }

        switch (metaldb::decodeType(instructions)) {
        case metaldb::PARSEROW: {
            auto parseRowExpression = metaldb::ParseRowInstruction(&instructions[1]);
            decodedInstructions[(numDecodedInstructions*2)+0] = (InstructionPtr) metaldb::PARSEROW;
            decodedInstructions[(numDecodedInstructions*2)+1] = (InstructionPtr) &parseRowExpression;

            return decodeInstruction(decodedInstructions, numDecodedInstructions+1, numInstructions - 1, parseRowExpression.end(), constants);
        }
        case metaldb::PROJECTION: {
            auto projectionInstruction = metaldb::ProjectionInstruction(&instructions[1]);
            decodedInstructions[(numDecodedInstructions*2)+0] = (InstructionPtr) metaldb::PROJECTION;
            decodedInstructions[(numDecodedInstructions*2)+1] = (InstructionPtr) &projectionInstruction;

            return decodeInstruction(decodedInstructions, numDecodedInstructions+1, numInstructions-1, projectionInstruction.end(), constants);
        }
        case metaldb::OUTPUT:
            auto outputProjection = metaldb::OutputInstruction(&instructions[1]);

            decodedInstructions[(numDecodedInstructions*2)+0] = (InstructionPtr) metaldb::OUTPUT;
            decodedInstructions[(numDecodedInstructions*2)+1] = (InstructionPtr) &outputProjection;

            return decodeInstruction(decodedInstructions, numDecodedInstructions+1, numInstructions-1, outputProjection.end(), constants);
        }
    }

    template<size_t MaxNumInstructions = MAX_VM_STACK_SIZE / 2>
    static void decodeInstructionRoot(int8_t numInstructions, METAL_DEVICE int8_t* instructions, DbConstants METAL_THREAD & constants) {
        // This should only be called once

        InstructionPtr decodedInstructions[MaxNumInstructions * 2];
        size_t numDecodedInstructions = 0;

        decodeInstruction(decodedInstructions, numDecodedInstructions, numInstructions, instructions, constants);
    }
}


inline void runQueryKernelImpl(METAL_DEVICE char* rawData, METAL_DEVICE int8_t* instructions, METAL_DEVICE char* outputBuffer, uint8_t thread_position_in_grid) {
    using namespace metaldb;
    RawTable rawTable(rawData);

    // Decode instructions + dispatch
    const auto numInstructions = instructions[0];
    DbConstants constants{rawTable, outputBuffer, thread_position_in_grid};

    // Dynamically scale the parameter
    if (numInstructions == 1) {
        decodeInstructionRoot<1>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 2) {
        decodeInstructionRoot<2>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 4) {
        decodeInstructionRoot<4>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 8) {
        decodeInstructionRoot<8>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 16) {
        decodeInstructionRoot<16>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 32) {
        decodeInstructionRoot<32>(numInstructions, &instructions[1], constants);
    } else if (numInstructions <= 64) {
        decodeInstructionRoot<64>(numInstructions, &instructions[1], constants);
    } else {
        decodeInstructionRoot(numInstructions, &instructions[1], constants);
    }
}

#ifdef __METAL__
kernel void runQueryKernel(device char* rawData [[ buffer(0) ]], device int8_t* instructions [[ buffer(1) ]], device char* outputBuffer [[ buffer(2) ]], uint id [[ thread_position_in_grid ]]) {
    runQueryKernelImpl(rawData, instructions, outputBuffer, id);
}
#endif

