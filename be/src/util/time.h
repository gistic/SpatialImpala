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

#ifndef IMPALA_UTIL_TIME_H
#define IMPALA_UTIL_TIME_H

#include <stdint.h>
#include <time.h>

/// Utilities for collecting timings.
namespace impala {

/// Returns a value representing a point in time with microsecond accuracy that is
/// unaffected by daylight savings or manual adjustments to the system clock. This should
/// not be assumed to be a Unix time. Typically the value corresponds to elapsed time
/// since the system booted. See UnixMillis() below if you need to send a time to a
/// different host.
inline int64_t MonotonicMicros() {  // 63 bits ~= 5K years uptime
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec * 1000000 + now.tv_nsec / 1000;
}

inline int64_t MonotonicMillis() {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

inline int64_t MonotonicSeconds() {
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec;
}

/// Returns the number of milliseconds that have passed since the Unix epoch. This is
/// affected by manual changes to the system clock but is more suitable for use across
/// a cluster. For more accurate timings on the local host use the monotonic functions
/// above.
inline int64_t UnixMillis() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

/// Sleeps the current thread for at least duration_ms milliseconds.
void SleepForMs(const int64_t duration_ms);

}

#endif
