#include <metaldb/query_engine/query_engine.hpp>
#include <metaldb/query_engine/partials.hpp>

#include <metaldb/query_engine/AST/filter.hpp>
#include <metaldb/query_engine/AST/read.hpp>
#include <metaldb/query_engine/AST/projection.hpp>
#include <metaldb/query_engine/AST/limit.hpp>
#include <metaldb/query_engine/AST/join.hpp>
#include <metaldb/query_engine/AST/write.hpp>

#include <filesystem>
#include <vector>
#include <string>
#include <memory>
#include <set>
#include <cassert>

namespace {
    using namespace metaldb::QueryEngine;
    auto DispatchAST(const std::shared_ptr<AST::Expr>& expr, const Metadata& metadata) -> std::vector<std::shared_ptr<StagePartial>>;

    auto listDir(const std::string& path) -> std::vector<std::filesystem::path> {
        std::vector<std::filesystem::path> output;
        auto iterator = std::filesystem::recursive_directory_iterator(path);

        for (const std::filesystem::directory_entry& dir_entry : iterator) {
            output.push_back(dir_entry);
        }

        return output;
    }

    auto ProcessReadAST(const std::shared_ptr<AST::Read>& expr, const Metadata& metadata) -> std::vector<std::shared_ptr<StagePartial>> {
        // This has no children, so no recursive call
        std::vector<std::shared_ptr<StagePartial>> partials;
        const auto* tableDef = metadata.getTable(expr->tableName());

        if (tableDef == nullptr) {
            std::cout << "Failed to find table: " << expr->tableName() << std::endl;
            return partials;
        }

        // Read list of files
        auto listOfFiles = listDir(tableDef->filePath);
        std::cout << "Reading " << listOfFiles.size() << " files" << std::endl;
        for (const auto& file : listOfFiles) {
            metaldb::Method method = metaldb::CSV;
            const auto extension = file.extension();
            if (extension == ".csv") {
                method = metaldb::CSV;
            } else {
                std::cout << "Unsure how to handle extension (" << __FILE__ << ", " << __LINE__ << "): " << extension << std::endl;
                continue;
            }

            auto partial = std::make_shared<ReadPartial>(file, method);
            partial->definition = std::make_shared<TableDefinition>(*tableDef);
            partials.emplace_back(partial);
        }
        
        return partials;
    }

    auto ProcessProjectionAST(const std::shared_ptr<AST::Projection>& expr, const Metadata& metadata) -> std::vector<std::shared_ptr<StagePartial>> {
        std::vector<std::shared_ptr<StagePartial>> partials;
        std::vector<std::shared_ptr<StagePartial>> childPartials;
        if (expr->hasChild()) {
            childPartials = DispatchAST(expr->child(), metadata);
        }

        if (childPartials.empty()) {
            std::cout << "Projection got no child partials (" << __FILE__ << ", " << __LINE__ << ")" << std::endl;
            return partials;
        }

        // Make the new table definition
        auto childTableDef = childPartials.at(0)->definition;
        auto tableDef = std::make_shared<TableDefinition>();
        tableDef->name = childTableDef->name;

        // Lookup column indexes
        std::vector<ProjectionPartial::ColumnIndexType> columnIndexes;
        columnIndexes.reserve(expr->numColumns());
        for (const auto& column : expr->columns()) {
            // Store the current table definiton in the AST?
            auto index = childTableDef->getColumnIndex(column);
            if (index) {
                tableDef->columns.push_back(childTableDef->columns.at(*index));
                columnIndexes.push_back(*index);
            } else {
                std::cerr << "Failed to get column name: " << column << std::endl;
                return partials;
            }
        }

        for (auto& p : childPartials) {
            auto partial = std::make_shared<ProjectionPartial>(columnIndexes);
            partial->children.push_back(p);
            partial->definition = tableDef;
            partials.push_back(partial);
        }

        return partials;
    }

    auto ProcessWriteAST(const std::shared_ptr<AST::Write>& expr, const Metadata& metadata) -> std::vector<std::shared_ptr<StagePartial>> {
        auto children = DispatchAST(expr->child(), metadata);
        auto partial = std::make_shared<WritePartial>(expr->filepath(), expr->method());
        partial->columnNames = expr->columns();
        partial->children = children;
        partial->definition = children.at(0)->definition;
        return {partial};
    }

    auto DispatchAST(const std::shared_ptr<AST::Expr>& expr, const Metadata& metadata) -> std::vector<std::shared_ptr<StagePartial>> {
        if (auto read = std::dynamic_pointer_cast<AST::Read>(expr)) {
            return ProcessReadAST(read, metadata);
        }
        if (auto proj = std::dynamic_pointer_cast<AST::Projection>(expr)) {
            return ProcessProjectionAST(proj, metadata);
        }
        if (auto write = std::dynamic_pointer_cast<AST::Write>(expr)) {
            return ProcessWriteAST(write, metadata);
        }

        return {};
     }

    auto CombinePartials(const std::vector<std::shared_ptr<StagePartial>>& partials) -> std::vector<std::shared_ptr<Stage>> {
        std::vector<std::shared_ptr<Stage>> stages;
        for (const auto& p : partials) {
            auto childStages = CombinePartials(p->children);
            auto stage = std::make_shared<Stage>();
            stage->partial = p;
            stage->execution = p->execution;

            if (childStages.size() > 1 || (!childStages.empty() && p->execution != childStages.at(0)->execution)) {
                // If it has 0 or 1 child, combine them in a partial.
                // Move them out of their original location;
                // Split them out if they execute in different places.
                if (childStages.at(0)->execution == GPU && p->execution == CPU) {
                    // Add output instruction
                    for (auto& child : childStages) {
                        // Wrap the child in a shuffle operation.
                        child->partial = std::make_shared<ShuffleOutputPartial>(child->partial);
                        stage->children.push_back(child);
                    }
                } else {
                    stage->children = childStages;
                }

                stage->partial->children = {};
            }

            stages.push_back(stage);
        }

        return stages;
    }
}

auto metaldb::QueryEngine::QueryEngine::compile(const std::shared_ptr<AST::Expr>& expr) const -> QueryPlan {
    auto partials = DispatchAST(expr, this->metadata);
    auto stages = CombinePartials(partials);

    QueryPlan plan;
    plan.stages = stages;
    return plan;
}
