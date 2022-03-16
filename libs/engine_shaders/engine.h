#pragma once

#include "constants.h"

namespace metal {
    class RawTable {
    public:
        RawTable(METAL_DEVICE char* rawData) : _rawData(rawData) {}

        METAL_CONSTANT static constexpr int8_t RAW_DATA_NUM_ROWS_INDEX = 2;

        int8_t GetSizeOfHeader() const {
            return (int8_t) _rawData[0];
        }

        int8_t GetSizeOfData() const {
            return (int8_t) _rawData[1];
        }

        int8_t GetNumRows() const {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX];
        }

        int8_t GetRowIndex(int8_t index) const {
            return (int8_t) _rawData[RAW_DATA_NUM_ROWS_INDEX + 1 + index];
        }

        int8_t GetStartOfData() const {
            return GetSizeOfHeader() + 1;
        }

    private:
        METAL_DEVICE char* _rawData;
    };
}

void runQueryKernelImpl(METAL_DEVICE char* rawData, METAL_DEVICE int8_t* instructions, uint thread_position_in_grid) {
    metal::RawTable rawTable(rawData);

    // TODO: Build out VM & run it through the rows.
}

#ifdef __METAL__
kernel void runQueryKernel(device char* rawData [[ buffer(0) ]], device int8_t* instructions [[ buffer(1) ]], uint id [[ thread_position_in_grid ]]) {
    runQueryKernelImpl(rawData, instructions, id);
}
#endif

