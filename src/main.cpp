#include <metaldb/reader/csv.hpp>
#include <metaldb/engine/Instructions.hpp>
#include <metaldb/engine/Engine.hpp>

#include <iostream>

using namespace metaldb;

int main() {
    std::cout << "Starting MetalDB" << std::endl;

    std::filesystem::path path;
    path.append("../../datasets/iris/iris.csv");
    reader::CSVReader reader(path);
    std::cout << "Table Is Valid: " << (reader.isValid() ? "YES" : "NO") << std::endl;

    reader::CSVReader::CSVOptions options;
    options.containsHeaderLine = true;
    options.stripQuotesFromHeader = true;
    auto rawTable = reader.read(options);

    std::cout << rawTable.debugStr() << std::endl;

    engine::Engine engine;
    using namespace engine;
    ParseRow parseRow(Method::CSV, {ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::Float, ColumnType::String}, /* skipHeader */ false);
    Projection projection({0, 1});
    Output output;
    engine.run(rawTable, parseRow, projection, output);

	return 0;
}
