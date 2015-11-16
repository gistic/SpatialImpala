# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
# Functional tests running the TPCH workload.
#
import logging
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite,\
    create_single_exec_option_dimension
from tests.common.skip import SkipIfOldAggsJoins

@SkipIfOldAggsJoins.nested_types
class TestTpchNestedQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch_nested'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchNestedQuery, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # The nested tpch data is currently only available in parquet.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_tpch_q1(self, vector):
    self.run_test_case('tpch-q1', vector)

  def test_tpch_q2(self, vector):
    self.run_test_case('tpch-q2', vector)

  def test_tpch_q3(self, vector):
    self.run_test_case('tpch-q3', vector)

  def test_tpch_q4(self, vector):
    self.run_test_case('tpch-q4', vector)

  def test_tpch_q5(self, vector):
    self.run_test_case('tpch-q5', vector)

  def test_tpch_q6(self, vector):
    self.run_test_case('tpch-q6', vector)

  def test_tpch_q7(self, vector):
    self.run_test_case('tpch-q7', vector)

  def test_tpch_q8(self, vector):
    self.run_test_case('tpch-q8', vector)

  def test_tpch_q9(self, vector):
    self.run_test_case('tpch-q9', vector)

  def test_tpch_q10(self, vector):
    self.run_test_case('tpch-q10', vector)

  def test_tpch_q11(self, vector):
    self.run_test_case('tpch-q11', vector)

  def test_tpch_q12(self, vector):
    self.run_test_case('tpch-q12', vector)

  def test_tpch_q13(self, vector):
    self.run_test_case('tpch-q13', vector)

  def test_tpch_q14(self, vector):
    self.run_test_case('tpch-q14', vector)

  def test_tpch_q15(self, vector):
    self.run_test_case('tpch-q15', vector)

  def test_tpch_q16(self, vector):
    self.run_test_case('tpch-q16', vector)

  def test_tpch_q17(self, vector):
    self.run_test_case('tpch-q17', vector)

  def test_tpch_q18(self, vector):
    self.run_test_case('tpch-q18', vector)

  def test_tpch_q19(self, vector):
    self.run_test_case('tpch-q19', vector)

  def test_tpch_q20(self, vector):
    self.run_test_case('tpch-q20', vector)

  def test_tpch_q21(self, vector):
    self.run_test_case('tpch-q21', vector)

  def test_tpch_q22(self, vector):
    self.run_test_case('tpch-q22', vector)
