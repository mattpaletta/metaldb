#pragma once

#include <metaldb/reader/csv.hpp>
#include <metaldb/query_engine/query_plan.hpp>
#include <metaldb/query_engine/partials.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "MetalManager.hpp"
#include "OutputRowReader.hpp"
#include "OutputRowWriter.hpp"

#include <taskflow/taskflow.hpp>

#include <iostream>
#include <filesystem>
#include <algorithm>
#include <cassert>

namespace metaldb {
    class Scheduler final {
    public:
        ~Scheduler() = default;

        static tf::Taskflow schedule(const QueryEngine::QueryPlan& plan) {
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

    private:
        Scheduler() = default;

        using IntermediateBufferType = std::vector<char>;
        using IntermediateBufferTypePtr = std::shared_ptr<IntermediateBufferType>;

        static IntermediateBufferTypePtr MakeBufferPtr() {
            return std::make_shared<IntermediateBufferType>();
        }

        static std::shared_ptr<MetalManager::OutputBufferType> MakeOutputBufferPtr() {
            return std::make_shared<MetalManager::OutputBufferType>();
        }

    public:
        // Helper function.
        // Takes in a rawTable, and splits it into a list of pairs with `MaxNumRows` serialized rows, and the number of rows in the chunk.
        static std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> SerializeRawTable(const metaldb::reader::RawTable& rawTable, std::size_t maxNumRows);

        struct Parameters final {
            tf::Taskflow* _Nonnull taskflow;
            std::shared_ptr<engine::Encoder> encoder;
            tf::Task* _Nonnull doWorkTask;
            std::shared_ptr<MetalManager> manager;
            std::shared_ptr<std::vector<char>> serializedData;
            const std::vector<IntermediateBufferTypePtr>& childOutputBuffers;
            IntermediateBufferTypePtr outputBuffer;

            Parameters(tf::Taskflow* _Nonnull taskflow_, std::shared_ptr<engine::Encoder> encoder_, tf::Task* _Nonnull doWorkTask_, std::shared_ptr<MetalManager> manager_, std::shared_ptr<std::vector<char>> serializedData_, const std::vector<IntermediateBufferTypePtr>& childOutputBuffers_, IntermediateBufferTypePtr outputBuffer_) : taskflow(taskflow_), encoder(encoder_), doWorkTask(doWorkTask_), manager(manager_), serializedData(serializedData_), childOutputBuffers(childOutputBuffers_), outputBuffer(outputBuffer_) {}
        };

        static tf::Task registerStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, IntermediateBufferTypePtr outputBuffer) {
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

        static void registerBaseStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, std::vector<IntermediateBufferTypePtr>&& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) {
            // Create the substeps within the graph for this particular stage.
            auto encoder = std::make_shared<engine::Encoder>();
            auto serializedData = MakeBufferPtr();
            auto encodeWorkTask = [&]{
                Parameters parameters{taskflow, encoder, &taskDoWork, manager, serializedData, childOutputBuffers, outputBuffer};
                return Scheduler::registerBasePartial(stage->partial, parameters);
            }();

            if (taskDoWork.has_work()) {
                taskDoWork.succeed(encodeWorkTask);
                return taskDoWork;
            }

            childOutputBuffers.push_back(serializedData);

            auto maxNumRows = manager->MaxNumRows();
            taskDoWork.work([=](tf::Subflow& subflow) {
                // Doing GPU work.
                DebugTask();
                std::vector<decltype(MakeOutputBufferPtr())> subtaskOutputBuffers;
                auto mergeSubtasks = subflow.placeholder();

                // Copy data from all child buffers into input, split by threadgroup size.
                // Split into new subflows for each 'group', that can all run in parallel.
                auto currentInputBuffer = MakeBufferPtr();
                OutputRowWriter writer;

                auto submitWork = [&]{
                    DebugTask();

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
                    DebugTask();

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

            return taskDoWork;
        }

        static tf::Task registerBasePartial(const std::shared_ptr<QueryEngine::StagePartial>& partial, Parameters& parameters) {
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

        static tf::Task registerReadPartial(std::shared_ptr<QueryEngine::ReadPartial> read, Parameters& parameters) {
            std::cout << "Registering Read partial" << read->id() << " " << read->filepath <<  std::endl;

            auto filename = read->filepath;
            auto method = read->method;
            auto definition = read->definition;

            auto rawTablePtr = reader::RawTable::placeholder();
            auto readRawTableTask = parameters.taskflow->emplace([=]() {
                DebugTask();
                std::cout << "Running read command for file: " << filename << std::endl;
                std::filesystem::path path;
                path.append(filename);
                reader::CSVReader reader(path);
                std::cout << "Table Is Valid: " << (reader.isValid() ? "YES" : "NO") << std::endl;

                reader::CSVReader::CSVOptions options;
                options.containsHeaderLine = true;
                options.stripQuotesFromHeader = true;
                auto rawTable = reader.read(options);
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
                DebugTask();
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
                    DebugTask();

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
                DebugTask();
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

        static tf::Task registerProjectionPartial(std::shared_ptr<QueryEngine::ProjectionPartial> projection, Parameters& parameters) {
            std::cout << "Registering Projection partial" << projection->id() << std::endl;

            auto encoder = parameters.encoder;
            return parameters.taskflow->emplace([=]() {
                DebugTask();

                engine::Projection projectionInstr(projection->columnIndexes);
                encoder->encode(projectionInstr);
            })
            .name("Encode Projection Task");
        }

        static tf::Task registerShufflePartial(std::shared_ptr<QueryEngine::ShuffleOutputPartial> output, Parameters& parameters) {
            std::cout << "Registering Output partial" << output->id() << std::endl;

            auto encoder = parameters.encoder;
            return parameters.taskflow->emplace([=]() {
                DebugTask();

                engine::Output outputInst;
                encoder->encode(outputInst);
            })
            .name("Encode Shuffle Partial");
        }

        static tf::Task registerWritePartial(std::shared_ptr<QueryEngine::WritePartial> write, Parameters& parameters) {
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

                DebugTask();
            }).name("Write output");
        }

        static void DebugTask() {
            std::cout << "In Debug Task" << std::endl;
        }
    };
}
