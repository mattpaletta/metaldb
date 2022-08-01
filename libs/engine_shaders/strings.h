#pragma once

#include "constants.h"
#include "memory.h"

namespace metal {
    namespace strings {
        constexpr static METAL_CONSTANT char NULL_CHAR = '\0';

        template<typename T = METAL_DEVICE char*, typename V = METAL_DEVICE char*>
        CPP_PURE_FUNC static int strcmp(const T str1, const T str2) {
            const T currChar1 = str1;
            const V currChar2 = str2;

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

        template<typename T = METAL_DEVICE char*, typename V = METAL_DEVICE char*>
        static T strncpy(T /* restrict */ destination, const V/* restrict */ source, size_t num) {
            V sourcePtr = source;
            T destinationCpy = destination;

            for (size_t i = 0; i < num; ++i) {
                *destinationCpy = *sourcePtr;
                destinationCpy++;
                sourcePtr++;
            }
            *destinationCpy = NULL_CHAR;

            return destination;
        }

        template<typename T = METAL_DEVICE char*, typename V = METAL_DEVICE char*>
        static T strcpy(T /* restrict */ destination, const T /* restrict */ source) {
            const V sourcePtr = source;
            T destinationCpy = destination;

            while (sourcePtr && *sourcePtr != NULL_CHAR) {
                *destinationCpy = *sourcePtr;
                destinationCpy++;
                sourcePtr++;
            }
            *destinationCpy = NULL_CHAR;

            return destination;
        }


        template<typename T = METAL_DEVICE char*, typename V = METAL_DEVICE char*>
        static T strncat(T /* restrict */ destination, const V/* restrict */ source, size_t num) {
            T endOfDest = destination;

            // Move to the end of destination
            while (endOfDest && *endOfDest++ != NULL_CHAR) {}

            // copy in the string
            return strncpy(endOfDest, source, num);
        }

        template<typename T = METAL_DEVICE char*, typename V = METAL_DEVICE char*>
        static T strcat(T /* restrict */ destination, const V /* restrict */ source) {
            T endOfDest = destination;

            // Move to the end of destination
            while (endOfDest && *endOfDest++ != NULL_CHAR) {}

            // copy in the string
            return strcpy(endOfDest, source);
        }

        template<typename T = METAL_DEVICE char*>
        CPP_PURE_FUNC static size_t strlen(const T str) {
            size_t size = 0;
            const METAL_DEVICE char* strPtr = str;
            while (strPtr && *strPtr != NULL_CHAR) {
                ++size;
            }

            return size;
        }

        template<typename T = METAL_DEVICE char*>
        CPP_PURE_FUNC static T const strchr(T const str, int character) {
            const char charToFind = character;
            T strPtr = str;
            while (strPtr && *strPtr != NULL_CHAR) {
                if (*strPtr == charToFind) {
                    return strPtr;
                }
                strPtr++;
            }

            return nullptr;
        }

        template<typename T = METAL_DEVICE char*>
        CPP_PURE_FUNC static T const strnchr(T const str, size_t length, int character) {
            const char charToFind = character;
            T strPtr = str;
            for (size_t i = 0; i < length; ++i) {
                if (*strPtr == charToFind) {
                    return strPtr;
                }
                strPtr++;
            }

            return nullptr;
        }

        CPP_CONST_FUNC static int ctoi(char c) {
            if (c == '0') {
                return 0;
            } else if (c == '1') {
                return 1;
            } else if (c == '2') {
                return 2;
            } else if (c == '3') {
                return 3;
            } else if (c == '4') {
                return 4;
            } else if (c == '5') {
                return 5;
            } else if (c == '6') {
                return 6;
            } else if (c == '7') {
                return 7;
            } else if (c == '8') {
                return 8;
            } else if (c == '9') {
                return 9;
            } else if (c == '.') {
                // magic number
                return 101;
            }

            return 102;
        }

        template<typename T = METAL_DEVICE char*>
        CPP_PURE_FUNC static int64_t const stoi(T const str, size_t length) {
            int64_t result = 0;
            for (size_t i = 0; i < length; ++i) {
                if (i > 0) {
                    result *= 10;
                }
                result += ctoi(str[i]);
            }
            return result;
        }

        template<typename T = METAL_DEVICE char*>
        CPP_PURE_FUNC static float const stof(T const str, size_t length) {
            // Find first part
            auto wholePart = strnchr(str, length, '.');
            if (wholePart) {
                // Found a '.'
                auto fractionPoint = wholePart;
                // Check if the decimal was at the end.
                if ((unsigned long)(fractionPoint - str) == length - 1UL) {
                    // It's at the end
                    return stoi(str, length - 1);
                } else {
                    // It's in the middle
                    auto wholePartConv = stoi(str, wholePart - str);
                    auto lengthOfDecimal = length - (wholePart - str) - 1;

                    // Decimal part starts after the '.'
                    auto decimalConv = stoi(fractionPoint + 1, lengthOfDecimal);

                    int multiplier =  1;
                    for (size_t i = 0; i < lengthOfDecimal; ++i) {
                        multiplier = (multiplier << 3) + (multiplier << 1);
                    }
                    auto result = wholePartConv + (decimalConv * 1.f/multiplier);
                    return result;
                }
            } else {
                // No decimal part, just cast to float
                return stoi(str, length);
            }
        }
    }
}
