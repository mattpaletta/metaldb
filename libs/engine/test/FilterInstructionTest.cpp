#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"

class FilterInstructionTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(FilterInstructionTest)

NEW_TEST(FilterInstructionTest, SerializeFilterInstruction) {}
NEW_TEST(FilterInstructionTest, ReadFilterInstruction) {}

CPPTEST_END_CLASS(FilterInstructionTest)
