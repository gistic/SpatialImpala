# - Find re2 headers and lib.
# RE2_ROOT hints the location
# This module defines
#  RE2_INCLUDE_DIR, directory containing headers
#  RE2_STATIC_LIB, path to libsnappy.a
#  re2 imported static library

set(RE2_SEARCH_HEADER_PATHS
  ${RE2_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/re2
)

set(RE2_SEARCH_LIB_PATHS
  ${RE2_ROOT}/lib
  $ENV{IMPALA_HOME}/thirdparty/re2/obj
)

find_path(RE2_INCLUDE_DIR re2/re2.h
  PATHS ${RE2_SEARCH_HEADER_PATHS}
        NO_DEFAULT_PATH
  DOC  "Google's re2 regex header path"
)

find_library(RE2_LIBS NAMES re2
  PATHS ${RE2_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "Google's re2 regex library"
)

find_library(RE2_STATIC_LIB NAMES libre2.a
  PATHS ${RE2_SEARCH_LIB_PATHS}
        NO_DEFAULT_PATH
  DOC   "Google's re2 regex static library"
)

message(STATUS ${RE2_INCLUDE_DIR})

if (NOT RE2_INCLUDE_DIR OR NOT RE2_LIBS OR
    NOT RE2_STATIC_LIB)
  set(RE2_FOUND FALSE)
  message(FATAL_ERROR "Re2 includes and libraries NOT found. "
    "Looked for headers in ${RE2_SEARCH_HEADER_PATH}, "
    "and for libs in ${RE2_SEARCH_LIB_PATH}")
else()
    set(RE2_FOUND TRUE)
    add_library(re2 STATIC IMPORTED)
    set_target_properties(re2 PROPERTIES IMPORTED_LOCATION "${RE2_STATIC_LIB}")
endif ()

mark_as_advanced(
  RE2_INCLUDE_DIR
  RE2_LIBS
  RE2_STATIC_LIB
  re2
)
