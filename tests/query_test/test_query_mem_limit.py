# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
# Targeted tests to validate per-query memory limit.

import pytest
import sys
import re
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *


class TestQueryMemLimit(ImpalaTestSuite):
  """Test class to do functional validation of per query memory limits.

  A specific query is run on text files, with the memory limit being added as
  an extra dimension. The query is expected to fail/pass depending on the limit
  value.
  """
  # There are a lot of 'unique' comments in lineitem.
  # Almost 80% of the table size.
  QUERIES = ["select count(distinct l_comment) from lineitem",
             "select group_concat(l_linestatus) from lineitem"]
  # TODO: It will be nice if we can get how much memory a query uses
  # dynamically, even if it is a rough approximation.
  # A mem_limit is expressed in bytes, with values <= 0 signifying no cap.
  # These values are either really small, unlimited, or have a really large cap.
  MAXINT_BYTES = str(sys.maxint)
  MAXINT_MB = str(sys.maxint/(1024*1024))
  MAXINT_GB = str(sys.maxint/(1024*1024*1024))
  # We expect the tests with MAXINT_* using valid units [bmg] to succeed.
  PASS_REGEX = re.compile("(%s|%s|%s)[bmg]?$" % (MAXINT_BYTES, MAXINT_MB, MAXINT_GB),
                          re.I)
  MEM_LIMITS = ["-1", "0", "1", "10", "100", "1000", "10000", MAXINT_BYTES,
                MAXINT_BYTES + "b", MAXINT_BYTES + "B",
                MAXINT_MB + "m", MAXINT_MB + "M",
                MAXINT_GB + "g", MAXINT_GB + "G",
                # invalid per-query memory limits
                "-1234", "-3.14", "xyz", "100%", MAXINT_BYTES + "k", "k" + MAXINT_BYTES]

  MEM_LIMITS_CORE = ["-1", "0", "10000", MAXINT_BYTES,
                MAXINT_BYTES + "b", MAXINT_MB + "M", MAXINT_GB + "g"]

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryMemLimit, cls).add_test_dimensions()
    # Only run the query for text
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

    # add mem_limit as a test dimension.
    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_dimension(\
          TestDimension('mem_limit', *TestQueryMemLimit.MEM_LIMITS_CORE))
    else:
      cls.TestMatrix.add_dimension(\
          TestDimension('mem_limit', *TestQueryMemLimit.MEM_LIMITS))

    # Make query a test dimension so we can support more queries.
    cls.TestMatrix.add_dimension(TestDimension('query', *TestQueryMemLimit.QUERIES))
    # This query takes a very long time to finish with a bound on the batch_size.
    # Remove the bound on the batch size.
    cls.TestMatrix.add_constraint(lambda v: v.get_value('exec_option')['batch_size'] == 0)

  @pytest.mark.execute_serially
  def test_mem_limit(self, vector):
    mem_limit = copy(vector.get_value('mem_limit'))
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = mem_limit
    query = vector.get_value('query')
    table_format = vector.get_value('table_format')
    if mem_limit in["0", "-1"] or self.PASS_REGEX.match(mem_limit):
      # should succeed
      self.__exec_query(query, exec_options, True, table_format)
    else:
      # should fail
      self.__exec_query(query, exec_options, False, table_format)

  def __exec_query(self, query, exec_options, should_succeed, table_format):
    try:
      self.execute_query(query, exec_options, table_format=table_format)
      assert should_succeed, "Query was expected to fail"
    except ImpalaBeeswaxException, e:
      assert not should_succeed, "Query should not have failed: %s" % e
