#pragma once

#include "constants.h"
#include "memory.h"

namespace metal {
    namespace strings {
        static METAL_CONSTANT char NULL_CHAR = '\0';

        static int strcmp(const METAL_DEVICE char* str1, const METAL_DEVICE char* str2) {
            const METAL_DEVICE char* currChar1 = str1;
            const METAL_DEVICE char* currChar2 = str2;

            while (currChar1 && currChar2 && *currChar1 != NULL_CHAR && *currChar2 != NULL_CHAR) {
                if (*currChar1 > *currChar2) {
                    return 1;
                } else if (*currChar1 < *currChar2) {
                    return -1;
                }

                currChar1++;
                currChar2++;
            }

            if (currChar1 && currChar2 && *currChar1 == NULL_CHAR && *currChar2 == NULL_CHAR) {
                return 0;
            } else if (currChar1 && *currChar1 == NULL_CHAR) {
                return 1;
            } else {
                return -1;
            }
        }

        static METAL_DEVICE char* strncpy(METAL_DEVICE char* /* restrict */ destination, const METAL_DEVICE char* /* restrict */ source, size_t num) {
            const METAL_DEVICE char* sourcePtr = source;
            METAL_DEVICE char* destinationCpy = destination;

            for (size_t i = 0; i < num; ++i) {
                *destinationCpy = *sourcePtr;
                destinationCpy++;
                sourcePtr++;
            }
            *destinationCpy = NULL_CHAR;

            return destination;
        }

        static METAL_DEVICE char* strcpy(METAL_DEVICE char* /* restrict */ destination, const METAL_DEVICE char* /* restrict */ source) {
            const METAL_DEVICE char* sourcePtr = source;
            METAL_DEVICE char* destinationCpy = destination;

            while (sourcePtr && *sourcePtr != NULL_CHAR) {
                *destinationCpy = *sourcePtr;
                destinationCpy++;
                sourcePtr++;
            }
            *destinationCpy = NULL_CHAR;

            return destination;
        }

        static METAL_DEVICE char* strncat(METAL_DEVICE char* /* restrict */ destination, const METAL_DEVICE char* /* restrict */ source, size_t num) {
            METAL_DEVICE char* endOfDest = destination;

            // Move to the end of destination
            while (endOfDest && *endOfDest++ != NULL_CHAR) {}

            // copy in the string
            return strncpy(endOfDest, source, num);
        }

        static METAL_DEVICE char* strcat(METAL_DEVICE char* /* restrict */ destination, const METAL_DEVICE char* /* restrict */ source) {
            METAL_DEVICE char* endOfDest = destination;

            // Move to the end of destination
            while (endOfDest && *endOfDest++ != NULL_CHAR) {}

            // copy in the string
            return strcpy(endOfDest, source);
        }

        static size_t strlen(const METAL_DEVICE char* str) {
            size_t size = 0;
            const METAL_DEVICE char* strPtr = str;
            while (strPtr && *strPtr != NULL_CHAR) {
                ++size;
            }

            return size;
        }

        static METAL_DEVICE char* const strchr(METAL_DEVICE char* const str, int character) {
            const char charToFind = character;
            METAL_DEVICE char* const strPtr = str;
            while (strPtr && *strPtr != NULL_CHAR) {
                if (*strPtr == charToFind) {
                    return strPtr;
                }
            }

            return nullptr;
        }
    }
}
