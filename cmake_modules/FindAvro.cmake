# - Find Avro (headers and libavrocpp_s.a) with AVRO_ROOT hinting a location
# This module defines
#  AVRO_INCLUDE_DIR, directory containing headers
#  AVRO_STATIC_LIB, path to libavrocpp_s.a
#  AVRO_FOUND
#  avro - static library
set(AVRO_SEARCH_HEADER_PATHS
  ${AVRO_ROOT}/include
  $ENV{IMPALA_HOME}/thirdparty/avro-c-$ENV{IMPALA_AVRO_VERSION}/src)

set(AVRO_SEARCH_LIB_PATH
  ${AVRO_ROOT}/lib
  $ENV{IMPALA_HOME}/thirdparty/avro-c-$ENV{IMPALA_AVRO_VERSION}/src)

find_path(AVRO_INCLUDE_DIR NAMES avro/schema.h schema.h PATHS
  ${AVRO_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH)

find_library(AVRO_STATIC_LIB NAMES libavro.a PATHS ${AVRO_SEARCH_LIB_PATH})

if(NOT AVRO_STATIC_LIB)
  message(FATAL_ERROR "Avro includes and libraries NOT found. "
    "Looked for headers in ${AVRO_SEARCH_HEADER_PATHS}, "
    "and for libs in ${AVRO_SEARCH_LIB_PATH}")
  set(AVRO_FOUND FALSE)
else()
  set(AVRO_FOUND TRUE)
  add_library(avro STATIC IMPORTED)
  set_target_properties(avro PROPERTIES IMPORTED_LOCATION "${AVRO_STATIC_LIB}")
endif ()

set(AVRO_FOUND ${AVRO_STATIC_LIB_FOUND})

mark_as_advanced(
  AVRO_INCLUDE_DIR
  AVRO_STATIC_LIB
  AVRO_FOUND
  avro
)
