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

import logging
import pytest
from tests.common.test_result_verifier import *
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension

# Unittest class for the test_result_verifier module.
class TestResultVerifier(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResultVerifier, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_result_row_indexing(self, vector):
    res = create_query_result(self.client.execute("select 1 as int_col, 'A' as str_col"))
    assert len(res.rows) == 1
    # Can index columns by case insensitive string (column alias) or column position
    assert res.rows[0]['int_col'] == "1"
    assert res.rows[0][0] == "1"
    assert res.rows[0]['INT_COL'] == "1"

    # String columns results are enclosed in single-quotes
    assert res.rows[0]['str_col'] == "'A'"
    assert res.rows[0][1] == "'A'"

    # Try to index by a column alias and position that does not exist
    try:
      res.rows[0]['does_not_exist']
      assert False, 'Expected error due to column alias not existing'
    except IndexError, e:
      assert "No column with label: does_not_exist" in e.message

    try:
      res.rows[0][2]
      assert False, 'Expected error due to column position not existing'
    except IndexError, e:
      assert 'list index out of range' in e.message
