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
                auto buffer = MakeBufferPtr();
                Scheduler::registerStage(stage, &taskflow, manager, buffer);
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

        // Helper function.
        // Takes in a rawTable, and splits it into a list of pairs with `MaxNumRows` serialized rows, and the number of rows in the chunk.
        static std::vector<std::pair<IntermediateBufferTypePtr, std::size_t>> SerializeRawTable(const metaldb::reader::RawTable& rawTable, std::size_t maxNumRows);

        static void registerStage(const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, IntermediateBufferTypePtr outputBuffer) {

            // First register all children
            std::vector<IntermediateBufferTypePtr> childOutputBuffers;
            childOutputBuffers.reserve(stage->children.size() + 1);
            for (auto& child : stage->children) {
                auto childBuffer = MakeBufferPtr();
                Scheduler::registerStage(child, taskflow, manager, childBuffer);
                childOutputBuffers.push_back(childBuffer);
            }

            Scheduler::registerBaseStage(stage, taskflow, manager, std::move(childOutputBuffers), outputBuffer);
        }

        static tf::Task registerBaseStage(const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, std::vector<IntermediateBufferTypePtr>&& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) {
            // Create the substeps within the graph for this particular stage.
            auto encoder = std::make_shared<engine::Encoder>();
            auto serializedData = MakeBufferPtr();
            auto taskDoWork = taskflow->placeholder();
            auto encodeWorkTask = Scheduler::registerBasePartial(stage->partial, taskflow, encoder, taskDoWork, manager, serializedData, childOutputBuffers, outputBuffer);

            if (taskDoWork.has_work()) {
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
                .name("Do Work")
                .succeed(encodeWorkTask);

            return taskDoWork;
        }

        template<typename... Args>
        static tf::Task registerBasePartial(const std::shared_ptr<QueryEngine::StagePartial>& partial, tf::Taskflow* _Nonnull taskflow, Args... args) {
            // Each partial returns the task to encode the instructions.

            auto registerChildPartials = [&](auto& task) {
                for (auto& childPartial : partial->children) {
                    auto childTask = Scheduler::registerBasePartial(childPartial, taskflow, args...);
                    childTask.precede(task);
                }
            };

            if (auto read = std::dynamic_pointer_cast<QueryEngine::ReadPartial>(partial)) {
                auto task = Scheduler::registerReadPartial(read, taskflow, args...);
                registerChildPartials(task);
                return task;
            } else if (auto projection = std::dynamic_pointer_cast<QueryEngine::ProjectionPartial>(partial)) {
                auto task = Scheduler::registerProjectionPartial(projection, taskflow, args...);
                registerChildPartials(task);
                return task;
            } else if (auto write = std::dynamic_pointer_cast<QueryEngine::WritePartial>(partial)) {
                auto task = Scheduler::registerWritePartial(write, taskflow, args...);
                registerChildPartials(task);
                return task;
            }

            return taskflow->emplace([]{
                std::cout << "Empty task" << std::endl;
            });
        }

        static tf::Task registerReadPartial(std::shared_ptr<QueryEngine::ReadPartial> read, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<engine::Encoder> encoder, tf::Task& doWorkTask, std::shared_ptr<MetalManager> manager, std::shared_ptr<std::vector<char>> serializedData, const std::vector<IntermediateBufferTypePtr>& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) {
            std::cout << "Registering Read partial" << read->id() << std::endl;

            auto filename = read->filepath;
            auto method = read->method;
            auto definition = read->definition;

            auto rawTablePtr = reader::RawTable::placeholder();
            auto readRawTableTask = taskflow->emplace([=]() {
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
            })
                .name("Read Raw Table Task: " + filename);

            // This reads in a file (1 chunk) and splits it into serialized sub-chunks each with a max
            // row count of `maxNumRows` (metal/implementation defined).
            // The GPU is guaranteed to always return `OutputRow` buffers, so we can merge them together.
            auto maxNumRows = manager->MaxNumRows();
            doWorkTask.work([=](tf::Subflow& subflow) {
                // Chunk the work out.
                DebugTask();
                std::vector<decltype(MakeOutputBufferPtr())> subtaskOutputBuffers;
                auto mergeSubtasks = subflow.placeholder();

                auto subtaskBuffers = Scheduler::SerializeRawTable(*rawTablePtr, maxNumRows);

                for (auto& [childBufferPtr, numRows] : subtaskBuffers) {
                    auto localCurrentInputBuffer = childBufferPtr;
                    auto subtaskNewBuffer = MakeOutputBufferPtr();
                    auto numRowsLocal = numRows;
                    subflow.emplace([=]() {
                        auto bufferPtr = localCurrentInputBuffer;
                        manager->run(*bufferPtr, encoder->data(), *subtaskNewBuffer, numRowsLocal);
                    })
                    .name("Do Work Chunk")
                    .precede(mergeSubtasks);

                    // Reset the writer
                    subtaskOutputBuffers.emplace_back(std::move(subtaskNewBuffer));
                }

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
            .name("Do Work");

            return taskflow->emplace([=]() {
                // Encode the commands
                DebugTask();
                auto columnTypes = [&]{
                    std::vector<ColumnType> columnTypes;
                    std::transform(definition->columns.begin(), definition->columns.end(), std::back_inserter(columnTypes), [](auto col) {
                        return col.type;
                    });
                    return columnTypes;
                }();
                engine::ParseRow parseRow(method, columnTypes, /* skipHeader */ false);
                encoder->encode(parseRow);
            })
                .name("Encode Parse Row Instruction: " + filename)
                .precede(doWorkTask);
        }

        static tf::Task registerProjectionPartial(std::shared_ptr<QueryEngine::ProjectionPartial> projection, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<engine::Encoder> encoder, tf::Task& doWorkTask, std::shared_ptr<MetalManager> manager, std::shared_ptr<std::vector<char>> serializedData, const std::vector<IntermediateBufferTypePtr>& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) {
            std::cout << "Registering Projection partial" << projection->id() << std::endl;

            return taskflow->emplace([=]() {
                DebugTask();

                engine::Projection projectionInstr(projection->columnIndexes);
                encoder->encode(projectionInstr);
            });
        }

        static tf::Task registerWritePartial(std::shared_ptr<QueryEngine::WritePartial> write, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<engine::Encoder> encoder, tf::Task& doWorkTask, std::shared_ptr<MetalManager> manager, std::shared_ptr<std::vector<char>> serializedData, const std::vector<IntermediateBufferTypePtr>& childOutputBuffers, IntermediateBufferTypePtr outputBuffer) {
            std::cout << "Registering Write partial" << write->id() << std::endl;

            doWorkTask.work([=]() {
                // TODO: How to get access to the output buffer.
            });

            return taskflow->emplace([=]() {
                // This can just happen after the 'doWorkTask' because it must be the last thing.

                DebugTask();
            })
                .name("Write output")
                .succeed(doWorkTask);
        }

        static void DebugTask() {
            std::cout << "In Debug Task" << std::endl;
        }
    };
}
