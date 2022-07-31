#pragma once
#include <metaldb/query_engine/query_plan.hpp>
#include <metaldb/engine/Instructions.hpp>
#include <metaldb/query_engine/partials.hpp>
#include <metaldb/reader/csv.hpp>

#include <taskflow/taskflow.hpp>

#include "MetalManager.hpp"

namespace metaldb {
    class Scheduler final {
    public:
        ~Scheduler() = default;

        static tf::Taskflow schedule(const QueryEngine::QueryPlan& plan);

    private:
        Scheduler() = default;

        using IntermediateBufferType = std::vector<char>;
        using IntermediateBufferTypePtr = std::shared_ptr<IntermediateBufferType>;

        static IntermediateBufferTypePtr MakeBufferPtr();
        static std::shared_ptr<MetalManager::OutputBufferType> MakeOutputBufferPtr();

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

        static tf::Task registerStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, IntermediateBufferTypePtr outputBuffer);

        static void registerBaseStage(tf::Task& taskDoWork, const std::shared_ptr<QueryEngine::Stage>& stage, tf::Taskflow* _Nonnull taskflow, std::shared_ptr<MetalManager> manager, std::vector<IntermediateBufferTypePtr>&& childOutputBuffers, IntermediateBufferTypePtr outputBuffer);

        static tf::Task registerBasePartial(const std::shared_ptr<QueryEngine::StagePartial>& partial, Parameters& parameters);

        static tf::Task registerReadPartial(std::shared_ptr<QueryEngine::ReadPartial> read, Parameters& parameters);

        static tf::Task registerProjectionPartial(std::shared_ptr<QueryEngine::ProjectionPartial> projection, Parameters& parameters);

        static tf::Task registerShufflePartial(std::shared_ptr<QueryEngine::ShuffleOutputPartial> output, Parameters& parameters);

        static tf::Task registerWritePartial(std::shared_ptr<QueryEngine::WritePartial> write, Parameters& parameters);

        static void DebugTask();
    };
}
