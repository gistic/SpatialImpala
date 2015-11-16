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
# Verification of impalad metrics after a test run.

import logging
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import TestDimension
from tests.verifiers.metric_verifier import MetricVerifier

class TestValidateMetrics(ImpalaTestSuite):
  """Verify metric values from the debug webpage.

  This test suite must be run after all the tests have been run, and no
  in-flight queries remain.
  TODO: Add a test for local assignments.
  """

  def test_metrics_are_zero(self):
    """Test that all the metric in METRIC_LIST are 0"""
    verifier = MetricVerifier(self.impalad_test_service)
    verifier.verify_metrics_are_zero()

  def test_num_unused_buffers(self):
    """Test that all buffers are unused"""
    verifier = MetricVerifier(self.impalad_test_service)
    verifier.verify_num_unused_buffers()
