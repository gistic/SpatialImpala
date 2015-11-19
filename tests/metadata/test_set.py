# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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
# Tests for SET <query option>

import logging
import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_dimensions import *
from tests.common.impala_test_suite import ImpalaTestSuite, SINGLE_NODE_ONLY

class TestSet(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSet, cls).add_test_dimensions()
    # This test only needs to be run once.
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_set(self, vector):
    self.run_test_case('QueryTest/set', vector)
