#include <metaldb/reader/csv.hpp>
#include <metaldb/engine/Instructions.hpp>
#include <metaldb/engine/Engine.hpp>

#include <iostream>

using namespace metaldb;

auto main() -> int {
    std::cout << "Starting MetalDB" << std::endl;

    engine::Engine engine;
    engine.run();

	return 0;
}
