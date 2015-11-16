# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests for IMPALA-1658

import pytest
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestHiveParquetTimestampConversion(CustomClusterTestSuite):
  '''Hive writes timestamps in parquet files by first converting values from local time
     to UTC. The conversion was not expected (other file formats don't convert) and a
     startup flag was later added to adjust for this (IMPALA-1658). This file tests that
     the conversion and flag behave as expected.
  '''

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def check_sanity(self, expect_converted_result):
    data = self.execute_query_expect_success(self.client, """
        SELECT COUNT(timestamp_col), COUNT(DISTINCT timestamp_col),
               MIN(timestamp_col), MAX(timestamp_col)
        FROM functional_parquet.alltypesagg_hive_13_1""")\
        .get_data()
    assert len(data) > 0
    rows = data.split("\n")
    assert len(rows) == 1
    values = rows[0].split("\t")
    assert len(values) == 4
    assert values[0] == "11000"
    assert values[1] == "10000"
    if expect_converted_result:
      # Doing easy time zone conversion in python seems to require a 3rd party lib,
      # so the only check will be that the value changed in some way.
      assert values[2] != "2010-01-01 00:00:00"
      assert values[3] != "2010-01-10 18:02:05.100000000"
    else:
      assert values[2] == "2010-01-01 00:00:00"
      assert values[3] == "2010-01-10 18:02:05.100000000"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true")
  def test_conversion(self, vector):
    tz_name = time.tzname[time.localtime().tm_isdst]
    self.check_sanity(tz_name not in ("UTC", "GMT"))
    # The value read from the Hive table should be the same as reading a UTC converted
    # value from the Impala table.
    tz_name = time.tzname[time.localtime().tm_isdst]
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg
          i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL AND i.timestamp_col IS NOT NULL)
          OR (h.timestamp_col IS NOT NULL AND i.timestamp_col IS NULL)
          OR h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
        """ % tz_name)\
        .get_data()
    assert len(data) == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=false")
  def test_no_conversion(self, vector):
    self.check_sanity(False)
    # Without conversion all the values will be different.
    tz_name = time.tzname[time.localtime().tm_isdst]
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg
          i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
        """ % tz_name)\
        .get_data()
    expected_row_count = 0 if tz_name in ("UTC", "GMT") else 10000
    assert len(data.split('\n')) == expected_row_count
    # A value should either stay null or stay not null.
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg
          i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL AND i.timestamp_col IS NOT NULL)
          OR (h.timestamp_col IS NOT NULL AND i.timestamp_col IS NULL)
        """)\
        .get_data()
    assert len(data) == 0
