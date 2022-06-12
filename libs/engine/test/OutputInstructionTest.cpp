#include <cpptest/cpptest.hpp>
#include <metaldb/engine/Instructions.hpp>

#include "RawTableCreator.hpp"

class OutputInstructionTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(OutputInstructionTest)

NEW_TEST(OutputInstructionTest, SerializeOutputInstruction) {}
NEW_TEST(OutputInstructionTest, ReadOutputInstruction) {}

CPPTEST_END_CLASS(OutputInstructionTest)
