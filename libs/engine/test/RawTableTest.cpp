#include <cpptest/cpptest.hpp>
#include "RawTableCreator.hpp"

#include <vector>

class RawTableTest : public cpptest::BaseCppTest {
public:
    void SetUp() override {
        // Run before every test
    }

    void TearDown() override {
        // Run After every test
    }
};

CPPTEST_CLASS(RawTableTest)

NEW_TEST(RawTableTest, TempRowToOutputWriter) {
    CreateMetalRawTable();
}

CPPTEST_END_CLASS(RawTableTest)
