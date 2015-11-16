// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <limits>
#include <gtest/gtest.h>
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
#include "util/string-parser.h"

#include "common/names.h"

using std::min;
using std::numeric_limits;

namespace impala {

string space[] = {"", "   ", "\t\t\t", "\n\n\n", "\v\v\v", "\f\f\f", "\r\r\r"};
int space_len = 7;

// Tests conversion of s to integer with and without leading/trailing whitespace
template<typename T>
void TestIntValue(const char* s, T exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      T val = StringParser::StringToInt<T>(str.data(), str.length(), &result);
      EXPECT_EQ(exp_val, val) << str;
      EXPECT_EQ(result, exp_result);
    }
  }
}

// Tests conversion of s, given a base, to an integer with and without leading/trailing
// whitespace
template<typename T>
void TestIntValue(
    const char* s, int base, T exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      T val = StringParser::StringToInt<T>(str.data(), str.length(), base, &result);
      EXPECT_EQ(exp_val, val) << str;
      EXPECT_EQ(result, exp_result);
    }
  }
}

void TestBoolValue(const char* s, bool exp_val, StringParser::ParseResult exp_result) {
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      // All combinations of leading and/or trailing whitespace.
      string str = space[i] + s + space[j];
      StringParser::ParseResult result;
      bool val = StringParser::StringToBool(str.data(), str.length(), &result);
      EXPECT_EQ(exp_val, val) << s;
      EXPECT_EQ(result, exp_result);
    }
  }
}

// Compare Impala's float conversion function against strtod.
template<typename T>
void TestFloatValue(const string& s, StringParser::ParseResult exp_result) {
  StringParser::ParseResult result;
  T val = StringParser::StringToFloat<T>(s.data(), s.length(), &result);
  EXPECT_EQ(exp_result, result);

  if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
    T exp_val = strtod(s.c_str(), NULL);
    EXPECT_EQ(exp_val, val);
  }
}

template<typename T>
void TestFloatValueIsNan(const string& s, StringParser::ParseResult exp_result) {
  StringParser::ParseResult result;
  T val = StringParser::StringToFloat<T>(s.data(), s.length(), &result);
  EXPECT_EQ(exp_result, result);

  if (exp_result == StringParser::PARSE_SUCCESS && result == exp_result) {
    EXPECT_TRUE(isnan(val));
  }
}

// Tests conversion of s to double and float with +/- prefixing (and no prefix) and with
// and without leading/trailing whitespace
void TestAllFloatVariants(const string& s, StringParser::ParseResult exp_result) {
  string sign[] = {"", "+", "-"};
  for (int i = 0; i < space_len; ++i) {
    for (int j = 0; j < space_len; ++j) {
      for (int k = 0; k < 3; ++k) {
        // All combinations of leading and/or trailing whitespace and +/- sign.
        string str = space[i] + sign[k] + s + space[j];
        TestFloatValue<float>(str, exp_result);
        TestFloatValue<double>(str, exp_result);
      }
    }
  }
}

template<typename T>
void TestFloatBruteForce() {
  T min_val = numeric_limits<T>::min();
  T max_val = numeric_limits<T>::max();

  // Keep multiplying by 2.
  T cur_val = 1.0;
  while (cur_val < max_val) {
    string s = lexical_cast<string>(cur_val);
    TestFloatValue<T>(s, StringParser::PARSE_SUCCESS);
    cur_val *= 2;
  }

  // Keep dividing by 2.
  cur_val = 1.0;
  while (cur_val > min_val) {
    string s = lexical_cast<string>(cur_val);
    TestFloatValue<T>(s, StringParser::PARSE_SUCCESS);
    cur_val /= 2;
  }
}

TEST(StringToInt, Basic) {
  TestIntValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("123", 123, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("123", 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("12345", 12345, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("12345678", 12345678, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("12345678901234", 12345678901234, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-10", -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-10", -10, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+1", 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("+1", 1, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+0", 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-0", 0, StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, InvalidLeadingTrailing) {
  // Test that trailing garbage is not allowed.
  TestIntValue<int8_t>("123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   123xyz   ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   -12  3xyz ", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("12 3", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-12 3", 0, StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestIntValue<int8_t>("x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   -x123", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   x-123", 0, StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestIntValue<int8_t>("", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToInt, Limit) {
  TestIntValue<int8_t>("127", 127, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-128", -128, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("32767", 32767, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-32768", -32768, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("2147483647", 2147483647, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-2147483648", -2147483648, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("9223372036854775807", numeric_limits<int64_t>::max(),
      StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-9223372036854775808", numeric_limits<int64_t>::min(),
      StringParser::PARSE_SUCCESS);
}

TEST(StringToInt, Overflow) {
  TestIntValue<int8_t>("128", 127, StringParser::PARSE_OVERFLOW);
  TestIntValue<int8_t>("-129", -128, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("32768", 32767, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("-32769", -32768, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("2147483648", 2147483647, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("-2147483649", -2147483648, StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("9223372036854775808", 9223372036854775807LL,
      StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("-9223372036854775809", numeric_limits<int64_t>::min(),
      StringParser::PARSE_OVERFLOW);
}

TEST(StringToInt, Int8_Exhaustive) {
  char buffer[5];
  for (int i = -256; i <= 256; ++i) {
    sprintf(buffer, "%d", i);
    int8_t expected = i;
    if (i > 127) {
      expected = 127;
    } else if (i < -128) {
      expected = -128;
    }
    TestIntValue<int8_t>(buffer, expected,
        i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
  }
}

TEST(StringToIntWithBase, Basic) {
  TestIntValue<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("123", 10, 123, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("123", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("12345", 10, 12345, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("12345678", 10, 12345678, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("12345678901234", 10, 12345678901234, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-10", 10, -10, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("+1", 10, 1, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("+0", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-0", 10, 0, StringParser::PARSE_SUCCESS);

  TestIntValue<int8_t>("a", 16, 10, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("A", 16, 10, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("b", 20, 11, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("B", 20, 11, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("z", 36, 35, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("f0a", 16, 3850, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("7", 8, 7, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("10", 2, 2, StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, NonNumericCharacters) {
  // Alphanumeric digits that are not in base are ok
  TestIntValue<int8_t>("123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-123abc   ", 10, -123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   123abc   ", 10, 123, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   -a123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("   a!123", 10, 0, StringParser::PARSE_SUCCESS);

  // Trailing white space + digits is not ok
  TestIntValue<int8_t>("   -12  3xyz ", 10, 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("12 3", 10, 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("-12 3", 10, 0, StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestIntValue<int8_t>("!123", 0, StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestIntValue<int8_t>("", 0, StringParser::PARSE_FAILURE);
  TestIntValue<int8_t>("   ", 0, StringParser::PARSE_FAILURE);
}

TEST(StringToIntWithBase, Limit) {
  TestIntValue<int8_t>("127", 10, 127, StringParser::PARSE_SUCCESS);
  TestIntValue<int8_t>("-128", 10, -128, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("32767", 10, 32767, StringParser::PARSE_SUCCESS);
  TestIntValue<int16_t>("-32768", 10, -32768, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("2147483647", 10, 2147483647, StringParser::PARSE_SUCCESS);
  TestIntValue<int32_t>("-2147483648", 10, -2147483648, StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("9223372036854775807", 10, numeric_limits<int64_t>::max(),
      StringParser::PARSE_SUCCESS);
  TestIntValue<int64_t>("-9223372036854775808", 10, numeric_limits<int64_t>::min(),
      StringParser::PARSE_SUCCESS);
}

TEST(StringToIntWithBase, Overflow) {
  TestIntValue<int8_t>("128", 10, 127, StringParser::PARSE_OVERFLOW);
  TestIntValue<int8_t>("-129", 10, -128, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("32768", 10, 32767, StringParser::PARSE_OVERFLOW);
  TestIntValue<int16_t>("-32769", 10, -32768, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("2147483648", 10, 2147483647, StringParser::PARSE_OVERFLOW);
  TestIntValue<int32_t>("-2147483649", 10, -2147483648, StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("9223372036854775808", 10, 9223372036854775807LL,
      StringParser::PARSE_OVERFLOW);
  TestIntValue<int64_t>("-9223372036854775809", 10, numeric_limits<int64_t>::min(),
      StringParser::PARSE_OVERFLOW);
}

TEST(StringToIntWithBase, Int8_Exhaustive) {
  char buffer[5];
  for (int i = -256; i <= 256; ++i) {
    sprintf(buffer, "%d", i);
    int8_t expected = i;
    if (i > 127) {
      expected = 127;
    } else if (i < -128) {
      expected = -128;
    }
    TestIntValue<int8_t>(buffer, 10, expected,
        i == expected ? StringParser::PARSE_SUCCESS : StringParser::PARSE_OVERFLOW);
  }
}

TEST(StringToFloat, Basic) {
  TestAllFloatVariants("0", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("123", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.0", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789", StringParser::PARSE_SUCCESS);

  // Scientific notation.
  TestAllFloatVariants("1e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.456E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789E10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789e-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("456.789E-10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.7e-294", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.7E-294", StringParser::PARSE_SUCCESS);

  // Min/max values.
  string float_min = lexical_cast<string>(numeric_limits<float>::min());
  string float_max = lexical_cast<string>(numeric_limits<float>::max());
  TestFloatValue<float>(float_min, StringParser::PARSE_SUCCESS);
  TestFloatValue<float>(float_max, StringParser::PARSE_SUCCESS);
  string double_min = lexical_cast<string>(numeric_limits<double>::min());
  string double_max = lexical_cast<string>(numeric_limits<double>::max());
  TestFloatValue<double>(double_min, StringParser::PARSE_SUCCESS);
  TestFloatValue<double>(double_max, StringParser::PARSE_SUCCESS);

  // Non-finite values
  TestAllFloatVariants("INFinity", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("infinity", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("inf", StringParser::PARSE_SUCCESS);

  TestFloatValueIsNan<float>("nan", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("nan", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<float>("NaN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("NaN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<float>("nana", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("nana", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<float>("naN", StringParser::PARSE_SUCCESS);
  TestFloatValueIsNan<double>("naN", StringParser::PARSE_SUCCESS);

  TestFloatValueIsNan<float>("n aN", StringParser::PARSE_FAILURE);
  TestFloatValueIsNan<float>("nnaN", StringParser::PARSE_FAILURE);


  // Overflow.
  TestFloatValue<float>(float_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<double>(double_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<float>("-" + float_max + "11111", StringParser::PARSE_OVERFLOW);
  TestFloatValue<double>("-" + double_max + "11111", StringParser::PARSE_OVERFLOW);

  // Precision limits
  // Regression test for IMPALA-1622 (make sure we get correct result with many digits
  // after decimal)
  TestAllFloatVariants("1.12345678912345678912", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.01111111111111111111111", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "12345678901234567890.1234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "12345678901234567890.01234567890123456789012", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("1.000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(".000000000000000000001234", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("0.000000000000000000001234e10", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "00000000000000000000.000000000000000000000", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants(
      "00000000000000000000.000000000000000000001", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("12345678901234567890123456", StringParser::PARSE_SUCCESS);
  TestAllFloatVariants("12345678901234567890123456e10", StringParser::PARSE_SUCCESS);

  // Invalid floats.
  TestAllFloatVariants("x456.789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456x.789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.x789e10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789xe10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789a10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789ex10", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789e10x", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("456.789e10   sdfs ", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("1e10   sdfs", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("in", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("in finity", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("na", StringParser::PARSE_FAILURE);
  TestAllFloatVariants("ThisIsANaN", StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, InvalidLeadingTrailing) {
  // Test that trailing garbage is not allowed.
  TestFloatValue<double>("123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("-123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   123xyz   ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   -12  3xyz ", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("12 3", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("-12 3", StringParser::PARSE_FAILURE);

  // Must have at least one leading valid digit.
  TestFloatValue<double>("x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   -x123", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   x-123", StringParser::PARSE_FAILURE);

  // Test empty string and string with only whitespaces.
  TestFloatValue<double>("", StringParser::PARSE_FAILURE);
  TestFloatValue<double>("   ", StringParser::PARSE_FAILURE);
}

TEST(StringToFloat, BruteForce) {
  TestFloatBruteForce<float>();
  TestFloatBruteForce<double>();
}

TEST(StringToBool, Basic) {
  TestBoolValue("true", true, StringParser::PARSE_SUCCESS);
  TestBoolValue("false", false, StringParser::PARSE_SUCCESS);

  TestBoolValue("false xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("true xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("ffffalse xdfsd", false, StringParser::PARSE_FAILURE);
  TestBoolValue("tttfalse xdfsd", false, StringParser::PARSE_FAILURE);
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
