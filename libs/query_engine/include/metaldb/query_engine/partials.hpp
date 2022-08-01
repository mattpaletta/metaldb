#pragma once

#include "table_definition.hpp"

#include "engine.h"

#include <string>
#include <memory>
#include <atomic>

namespace metaldb::QueryEngine {
    enum Execution {
        GPU,
        CPU
    };

    class StagePartial {
    public:
        StagePartial() {
            static Counter counter;
            this->_id = counter.i++;
        }

        virtual ~StagePartial() noexcept = default;

        std::shared_ptr<TableDefinition> definition;
        // Children are the stage partial this stage depends on.
        // This will usually be 1, except for a join where multiple feeds into a single one.
        std::vector<std::shared_ptr<StagePartial>> children;

        std::size_t id() const noexcept {
            return this->_id;
        }
        Execution execution = GPU;

    private:
        struct Counter final {
            std::atomic<std::uint64_t> i = 0;
        };
        std::size_t _id;
    };

    struct ReadPartial : public StagePartial {
        ReadPartial(std::string filepath_, metaldb::Method method_) : filepath(std::move(filepath_)), method(method_) {}

        std::string filepath;
        metaldb::Method method;
    };

    struct ProjectionPartial : public StagePartial {
        using ColumnIndexType = uint8_t;
        ProjectionPartial(std::vector<ColumnIndexType> columnIndexes_) : columnIndexes(std::move(columnIndexes_)) {}

        std::vector<ColumnIndexType> columnIndexes;
    };

    struct ShuffleOutputPartial : public StagePartial {
        ShuffleOutputPartial(std::shared_ptr<StagePartial> child) : StagePartial(*child) {
            this->children = {child};
        }
    };

    struct WritePartial : public StagePartial {
        WritePartial(std::string filepath_, metaldb::Method method_, std::vector<std::string> columnNames_ = {}) : filepath(std::move(filepath_)), method(method_), columnNames(std::move(columnNames_)) {
            this->execution = CPU;
        }

        std::string filepath;
        metaldb::Method method;
        std::vector<std::string> columnNames;
    };

    struct Stage {
        Execution execution = GPU;

        std::vector<std::shared_ptr<Stage>> children;
        std::shared_ptr<StagePartial> partial;
    };
}
