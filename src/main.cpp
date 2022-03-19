#include <metaldb/reader/csv.hpp>
#include <metaldb/engine/Instructions.hpp>
#include <metaldb/engine/Engine.hpp>

#include <iostream>

using namespace metaldb;

template<typename T>
void printData(const std::vector<T>& data) {
    std::cout << "Got Data of length: " << data.size() << std::endl;
    for (const auto& d : data) {
        std::cout << (int)d << " ";
    }
    std::cout << std::endl;
}

void decode(metaldb::engine::instruction_serialized_type& encoded) {
    using namespace metaldb::engine;
    auto decoder = Decoder(encoded);

    // The first element is the number of instructions
    const auto numInstructions = decoder.numInstructions();
    for (instruction_serialized_value_type i = 0; i < numInstructions; ++i) {
        const auto instructionType = decoder.decodeType();
        switch(instructionType) {
        case PARSEROW: {
            auto val = decoder.decode<class ParseRow>();
            std::cout << val.description() << std::endl;
            break;
        }
        case PROJECTION: {
            auto val = decoder.decode<class Projection>();
            std::cout << val.description() << std::endl;
            break;
        }
        }
    }
}

int testEncode() {
    std::cout << "Testing Encode/Decode" << std::endl;

    using namespace engine;

    Encoder encoder;
    {
        ParseRow parseRow(Method::CSV, {ColumnType::Float, ColumnType::Float, ColumnType::Integer});
        encoder.encode(parseRow);
    }
    {
        Projection projection({0, 1});
        encoder.encode(projection);
    }

    auto data = encoder.data();
    printData(data);

    // Try decode
    decode(data);

    return 0;
}


int main() {
    std::cout << "Starting MetalDB" << std::endl;

    std::filesystem::path path;
    path.append("iris.csv");
    reader::CSVReader reader(path);
    std::cout << "Table Is Valid: " << (reader.isValid() ? "YES" : "NO") << std::endl;

    reader::CSVReader::CSVOptions options;
    options.containsHeaderLine = true;
    options.stripQuotesFromHeader = true;
    auto rawTable = reader.read(options);

    std::cout << rawTable.debugStr() << std::endl;



    engine::Engine engine;
    {
        using namespace engine;
        class ParseRow parseRow(Method::CSV, {ColumnType::Float, ColumnType::Float, ColumnType::Integer});
        class Projection projection({0, 1});
        engine.run(rawTable, parseRow, projection);
    }

	return 0;
}
