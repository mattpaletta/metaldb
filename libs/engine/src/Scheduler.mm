#include "Scheduler.hpp"
#include "OutputRowReader.hpp"
#include "OutputRowWriter.hpp"

#include <iostream>
#include <filesystem>
#include <algorithm>
#include <cassert>

auto metaldb::Scheduler::SerializeRawTable(const metaldb::reader::RawTable& rawTable, std::size_t maxChunkSize) noexcept -> std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> {
    using SizeOfHeaderType = RawTable::SizeOfHeaderType;
    using SizeOfDataType = RawTable::SizeOfDataType;
    using NumRowsType = RawTable::NumRowsType;
    using RowIndexType = RawTable::RowIndexType;

    std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> output;
    auto rawDataSerialized = MakeBufferPtr();

    std::size_t lastDataOffset = 0;
    auto prepareChunk = [&](NumRowsType startRow, NumRowsType endRow) {
        const auto numRowsLocal = endRow - startRow;
        if (numRowsLocal == 0 || endRow < startRow /* if overflowed */) {
            return;
        }

        // Write header section

        // size of the header
        SizeOfHeaderType sizeOfHeader = 0;
        {
            // Allocate space for header size
            WriteBytesStartingAt<SizeOfHeaderType>(*rawDataSerialized, 0);
            sizeOfHeader += sizeof(SizeOfHeaderType);
        }


        // Allocate space for buffer size
        SizeOfDataType numBytes = 0;

        // Size of data
        {
            assert(rawDataSerialized->size() == RawTable::SizeOfDataOffset);
            WriteBytesStartingAt<SizeOfDataType>(*rawDataSerialized, 0);
            sizeOfHeader += sizeof(SizeOfDataType);
        }

        // num rows
        {
            const NumRowsType num = numRowsLocal;
            assert(rawDataSerialized->size() == RawTable::NumRowsOffset);
            WriteBytesStartingAt(*rawDataSerialized, num);
            sizeOfHeader += sizeof(NumRowsType);
        }

        // Write the row indexes
        {
            assert(rawDataSerialized->size() == RawTable::RowIndexOffset);
            for (auto row = startRow; row < endRow; ++row) {
                // Last data access stores the highest index on the last iteration, which we offset
                // because those rows are not in this batch.
                RowIndexType index = rawTable.rowIndexes.at(row) - lastDataOffset;
                WriteBytesStartingAt(*rawDataSerialized, index);
            }
            sizeOfHeader += (sizeof(RowIndexType) * numRowsLocal);
        }

        {
            // Write in the data
            for (auto row = startRow; row < endRow; ++row) {
                RowIndexType index = rawTable.rowIndexes.at(row);
                if (row == rawTable.rowIndexes.size() - 1) {
                    // This is the last row, read until the end.
                    for (std::size_t i = index; i < rawTable.data.size(); ++i) {
                        rawDataSerialized->push_back(rawTable.data.at(i));
                    }
                    numBytes += (rawTable.data.size() - index);
                    lastDataOffset = std::max(lastDataOffset, rawTable.data.size() - 1);
                } else {
                    // Read until the next row
                    RowIndexType nextRow = rawTable.rowIndexes.at(row + 1);
                    for (auto i = index; i < nextRow; ++i) {
                        rawDataSerialized->push_back(rawTable.data.at(i));
                    }
                    numBytes += (nextRow - index);
                    lastDataOffset = std::max(lastDataOffset, nextRow - 0UL);
                }
            }
        }

        // Final cleanup
        WriteBytesStartingAt(&rawDataSerialized->at(RawTable::SizeOfDataOffset), numBytes);
        WriteBytesStartingAt(&rawDataSerialized->at(RawTable::SizeOfHeaderOffset), sizeOfHeader);

        // Flush buffer
        output.emplace_back(rawDataSerialized, numRowsLocal);
        rawDataSerialized = MakeBufferPtr();
    };

    // Chunk it into the max size appropriately.
    NumRowsType i = 0;
    auto numRows = rawTable.NumRows();
    if (numRows > maxChunkSize) {
        for (; i < numRows - maxChunkSize; i += maxChunkSize) {
            prepareChunk(i, i + maxChunkSize);
        }
    }
    prepareChunk(i, numRows);

    return output;
}

auto metaldb::Scheduler::schedule(const QueryEngine::QueryPlan& plan) noexcept -> tf::Taskflow {
    tf::Taskflow taskflow;
    auto manager = MetalManager::Create();

    if (!manager) {
        std::cout << "Failed to get MetalManager, aborting early." << std::endl;
        return taskflow;
    }

    for (const auto& stage : plan.stages) {
        auto placeholder = taskflow.emplace([]{}).name("Do Stage");
        auto buffer = MakeBufferPtr();
        Scheduler::registerStage(placeholder, stage, &taskflow, manager, buffer);
    }

    return taskflow;
}

auto metaldb::Scheduler::MakeBufferPtr() noexcept -> IntermediateBufferTypePtr {
    return std::make_shared<IntermediateBufferType>();
}

auto metaldb::Scheduler::MakeOutputBufferPtr() noexcept -> std::shared_ptr<MetalManager::OutputBufferType> {
    return std::make_shared<MetalManager::OutputBufferType>();
}

auto metaldb::Scheduler::registerStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, IntermediateBufferTypePtr outputBuffer) noexcept -> tf::Task {
    // First register all children
    std::vector<IntermediateBufferTypePtr> childOutputBuffers;
    childOutputBuffers.reserve(stage->children.size() + 1);
    for (auto& child : stage->children) {
        auto childBuffer = MakeBufferPtr();
        auto childDoWork = taskflow->placeholder();
        Scheduler::registerStage(childDoWork, child, taskflow, manager, childBuffer);
        childDoWork.precede(taskDoWork);
        childOutputBuffers.push_back(childBuffer);
    }

    Scheduler::registerBaseStage(taskDoWork, stage, taskflow, manager, std::move(childOutputBuffers), outputBuffer);
    return taskDoWork;
}

void metaldb::Scheduler::registerBaseStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, std::vector<IntermediateBufferTypePtr>&& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) noexcept {
    // Create the substeps within the graph for this particular stage.
    auto encoder = std::make_shared<engine::Encoder>();
    auto serializedData = MakeBufferPtr();
    auto encodeWorkTask = [&]{
        Parameters parameters{taskflow, encoder, &taskDoWork, manager, serializedData, childOutputBuffers, outputBuffer};
        return Scheduler::registerBasePartial(stage->partial, parameters);
    }();

    if (taskDoWork.has_work()) {
        taskDoWork.succeed(encodeWorkTask);
        return;
    }

    childOutputBuffers.push_back(serializedData);

    auto maxNumRows = manager->MaxNumRows();
    taskDoWork.work([=](tf::Subflow& subflow) {
        // Doing GPU work.
        std::vector<decltype(MakeOutputBufferPtr())> subtaskOutputBuffers;
        auto mergeSubtasks = subflow.placeholder();

        // Copy data from all child buffers into input, split by threadgroup size.
        // Split into new subflows for each 'group', that can all run in parallel.
        auto currentInputBuffer = MakeBufferPtr();
        OutputRowWriter writer;

        auto submitWork = [&]{

            auto numRows = writer.CurrentNumRows();
            if (numRows == 0) {
                return;
            }

            // Flush the buffer
            writer.write(*currentInputBuffer);

            auto localCurrentInputBuffer = currentInputBuffer;
            auto subtaskNewBuffer = MakeOutputBufferPtr();
            subflow.emplace([=]() {
                manager->run(*localCurrentInputBuffer, encoder->data(), *subtaskNewBuffer, numRows);
            })
            .name("Do Work Chunk")
            .precede(mergeSubtasks);

            // Reset the writer
            writer = OutputRowWriter();
            currentInputBuffer = MakeBufferPtr();
            subtaskOutputBuffers.emplace_back(std::move(subtaskNewBuffer));
        };

        for (auto& childBufferPtr : childOutputBuffers) {
            if (!childBufferPtr) {
                std::cout << "Child buffer pointer not initialized, skipping" << std::endl;
                continue;
            }

            auto reader = OutputRowReader(*childBufferPtr);

            for (std::size_t i = 0; i < reader.NumRows(); ++i) {
                writer.copyRow(reader, i);
                if (writer.CurrentNumRows() == maxNumRows - 1) {
                    // Submit the work and reset the writer.
                    submitWork();
                }
            }
        }

        // Submit remaining work
        submitWork();

        mergeSubtasks.work([=]{
            OutputRowWriter writer;
            for (auto& subtaskBuffer : subtaskOutputBuffers) {
                auto reader = OutputRowReader(*subtaskBuffer);
                for (std::size_t i = 0; i < reader.NumRows(); ++i) {
                    writer.copyRow(reader, i);
                }
            }

            writer.write(*outputBuffer);
        }).name("Merge subtasks");
    })
    .name("Do GPU Work")
    .succeed(encodeWorkTask);
}

auto metaldb::Scheduler::registerBasePartial(const std::shared_ptr<QueryEngine::StagePartial>& partial, Parameters& parameters) noexcept -> tf::Task {
    // Each partial returns the task to encode the instructions.
    tf::Task task;
    if (auto read = std::dynamic_pointer_cast<QueryEngine::ReadPartial>(partial)) {
        task = Scheduler::registerReadPartial(read, parameters);

    } else if (auto projection = std::dynamic_pointer_cast<QueryEngine::ProjectionPartial>(partial)) {
        task = Scheduler::registerProjectionPartial(projection, parameters);

    } else if (auto write = std::dynamic_pointer_cast<QueryEngine::WritePartial>(partial)) {
        task = Scheduler::registerWritePartial(write, parameters);

    } else if (auto output = std::dynamic_pointer_cast<QueryEngine::ShuffleOutputPartial>(partial)) {
        task = Scheduler::registerShufflePartial(output, parameters);
    } else {
        task = parameters.taskflow->emplace([]{ /* empty task */});
    }

    for (auto& childPartial : partial->children) {
        auto childTask = Scheduler::registerBasePartial(childPartial, parameters);
        childTask.precede(task);
    }
    return task;
}

auto metaldb::Scheduler::registerReadPartial(std::shared_ptr<QueryEngine::ReadPartial> read, Parameters& parameters) noexcept -> tf::Task {
    std::cout << "Registering Read partial" << read->id() << " " << read->filepath <<  std::endl;

    auto filename = read->filepath;
    auto method = read->method;
    auto definition = read->definition;

    auto rawTablePtr = reader::RawTable::Placeholder();
    auto readRawTableTask = parameters.taskflow->emplace([=]() {
        std::cout << "Running read command for file: " << filename << std::endl;
        std::filesystem::path path;
        path.append(filename);
        reader::CSVReader reader(path);
        std::cout << "Table Is Valid: " << (reader.IsValid() ? "YES" : "NO") << std::endl;

        reader::CSVReader::CSVOptions options;
        options.containsHeaderLine = true;
        options.stripQuotesFromHeader = true;
        auto rawTable = reader.Read(options);
        *rawTablePtr = std::move(rawTable);
    }).name("Read Raw Table Task: " + filename);

    // This reads in a file (1 chunk) and splits it into serialized sub-chunks each with a max
    // row count of `maxNumRows` (metal/implementation defined).
    // The GPU is guaranteed to always return `OutputRow` buffers, so we can merge them together.
    auto manager = parameters.manager;
    auto encoder = parameters.encoder;
    auto outputBuffer = parameters.outputBuffer;
    auto maxNumRows = manager->MaxNumRows();
    auto maxNumBytes = manager->MaxMemory();
    assert(!parameters.doWorkTask->has_work());
    parameters.doWorkTask->work([=](tf::Subflow& subflow) mutable {
        // Chunk the work out.
        std::vector<decltype(MakeOutputBufferPtr())> subtaskOutputBuffers;
        auto mergeSubtasks = subflow.placeholder();

        auto subtaskBuffers = [&]() {
            auto currentMaxNumRows = maxNumRows;
            while (true) {
                auto subtaskBuffers = Scheduler::SerializeRawTable(*rawTablePtr, currentMaxNumRows);
                // Verify none exceed the max length
                bool allPassed = true;
                for (const auto& [childBufferPtr, numRows] : subtaskBuffers) {
                    if (childBufferPtr->size() >= maxNumBytes) {
                        // Re-serialize
                        const double sizeOfRowApprox = childBufferPtr->size() / (double) numRows;
                        std::size_t approxMaxNumRows = std::floor(maxNumBytes / sizeOfRowApprox);
                        // Never make it larger
                        currentMaxNumRows = std::min(currentMaxNumRows - 1, approxMaxNumRows);
                        allPassed = false;
                        break;
                    }
                }
                if (allPassed)  {
                    return subtaskBuffers;
                }
            }
        }();

        // We can free the rawTable
        rawTablePtr.reset();

        for (auto& [childBufferPtr, numRows] : subtaskBuffers) {
            auto localCurrentInputBuffer = childBufferPtr;
            auto subtaskNewBuffer = MakeOutputBufferPtr();
            auto numRowsLocal = numRows;
            subflow.emplace([=]() {
                auto bufferPtr = localCurrentInputBuffer;
                auto localOutput = subtaskNewBuffer;
                manager->run(*bufferPtr, encoder->data(), *localOutput, numRowsLocal);
            })
            .name("Do Work Chunk")
            .precede(mergeSubtasks);

            // Reset the writer
            subtaskOutputBuffers.emplace_back(std::move(subtaskNewBuffer));
        }

        mergeSubtasks.work([=]() mutable {
            OutputRowWriter writer;
            for (auto& subtaskBuffer : subtaskOutputBuffers) {
                auto reader = OutputRowReader(*subtaskBuffer);
                for (std::size_t i = 0; i < reader.NumRows(); ++i) {
                    writer.copyRow(reader, i);
                }
            }

            writer.write(*outputBuffer);

            std::cout << "Writing output -- Num Columns: " << (int) writer.NumColumns() << " -- Num Bytes: " << (int) writer.NumBytes() << " -- Num Rows: " << (int) writer.CurrentNumRows() << std::endl;
            auto reader = OutputRowReader(*outputBuffer);
            std::cout << "Reading -- Num Columns: " << (int) reader.NumColumns() << " -- Num Bytes: " << (int) reader.NumBytes() << " -- Num Rows: " << (int) reader.NumRows() << std::endl;
        }).name("Merge subtasks");
    })
    .name("Do Parse Row Work: " + filename)
    .succeed(readRawTableTask);

    return parameters.taskflow->emplace([=]() {
        // Encode the commands
        auto columnTypes = [&]{
            std::vector<ColumnType> columnTypes;
            std::transform(definition->columns.begin(), definition->columns.end(), std::back_inserter(columnTypes), [](auto col) {
                // The nullable type is always one more.
                if (col.type == Unknown) {
                    return col.type;
                } else if (col.nullable) {
                    return (metaldb::ColumnType) (col.type + 1);
                } else {
                    return col.type;
                }
            });
            return columnTypes;
        }();
        engine::ParseRow parseRow(method, columnTypes, /* skipHeader */ false);
        encoder->encode(parseRow);
    })
    .name("Encode Parse Row Instruction: " + filename);
}

auto metaldb::Scheduler::registerProjectionPartial(std::shared_ptr<QueryEngine::ProjectionPartial> projection, Parameters& parameters) noexcept -> tf::Task {
    std::cout << "Registering Projection partial" << projection->id() << std::endl;

    auto encoder = parameters.encoder;
    return parameters.taskflow->emplace([=]() {
        engine::Projection projectionInstr(projection->columnIndexes);
        encoder->encode(projectionInstr);
    })
    .name("Encode Projection Task");
}

auto metaldb::Scheduler::registerShufflePartial(std::shared_ptr<QueryEngine::ShuffleOutputPartial> output, Parameters& parameters) noexcept -> tf::Task {
    std::cout << "Registering Output partial" << output->id() << std::endl;

    auto encoder = parameters.encoder;
    return parameters.taskflow->emplace([=]() {
        engine::Output outputInst;
        encoder->encode(outputInst);
    })
    .name("Encode Shuffle Partial");
}

auto metaldb::Scheduler::registerWritePartial(std::shared_ptr<QueryEngine::WritePartial> write, Parameters& parameters) noexcept -> tf::Task {
    std::cout << "Registering Write partial" << write->id() << std::endl;

    auto childOutputBuffers = parameters.childOutputBuffers;
    parameters.doWorkTask->work([=]() {
        // Merge child buffers
        OutputRowWriter writer;
        for (auto& childBuffer : childOutputBuffers) {
            auto reader = OutputRowReader(*childBuffer);
            for (auto i = 0; i < reader.NumRows(); ++i) {
                writer.copyRow(reader, i);
            }
        }

        std::cout << "Writing output -- Num Columns: " << (int) writer.NumColumns() << " -- Num Bytes: " << (int) writer.NumBytes() << " -- Num Rows: " << (int) writer.CurrentNumRows() << std::endl;
    }).name("Do Output Shuffle Work");

    return parameters.taskflow->emplace([=]() {
        // This can just happen after the 'doWorkTask' because it must be the last thing.

    }).name("Write output");
}
