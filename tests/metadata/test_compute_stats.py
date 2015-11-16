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

from subprocess import check_call

from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfS3, SkipIfIsilon, SkipIf
from tests.util.filesystem_utils import WAREHOUSE

# Tests the COMPUTE STATS command for gathering table and column stats.
# TODO: Merge this test file with test_col_stats.py
@SkipIfS3.insert # S3: missing coverage: compute stats
@SkipIf.not_default_fs # Isilon: Missing coverage: compute stats
class TestComputeStats(ImpalaTestSuite):
  TEST_DB_NAME = "compute_stats_db"
  TEST_ALIASING_DB_NAME = "parquet"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestComputeStats, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Do not run these tests using all dimensions because the expected results
    # are different for different file formats.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    # cleanup and create a fresh test database
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database {0} location '{1}/{0}.db'"
        .format(self.TEST_DB_NAME, WAREHOUSE))
    # cleanup and create a fresh test database whose name is a keyword
    self.cleanup_db(self.TEST_ALIASING_DB_NAME)
    self.execute_query("create database `{0}` location '{1}/{0}.db'"
        .format(self.TEST_ALIASING_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)
    self.cleanup_db(self.TEST_ALIASING_DB_NAME)

  @pytest.mark.execute_serially
  def test_compute_stats(self, vector):
    self.run_test_case('QueryTest/compute-stats', vector)
    # Test compute stats on decimal columns separately so we can vary between CDH4/5
    self.run_test_case('QueryTest/compute-stats-decimal', vector)
    # To cut down on test execution time, only run the compute stats test against many
    # partitions if performing an exhaustive test run.
    if self.exploration_strategy() != 'exhaustive': return
    self.run_test_case('QueryTest/compute-stats-many-partitions', vector)

  @pytest.mark.execute_serially
  def test_compute_stats_incremental(self, vector):
    self.run_test_case('QueryTest/compute-stats-incremental', vector)

  @pytest.mark.execute_serially
  @SkipIfS3.hive
  @SkipIfS3.insert
  @SkipIfIsilon.hive
  def test_compute_stats_impala_2201(self, vector):
    """IMPALA-2201: Tests that the results of compute incremental stats are properly
    persisted when the data was loaded from Hive with hive.stats.autogather=true.
    """

    # Unless something drastic changes in Hive and/or Impala, this test should
    # always succeed.
    if self.exploration_strategy() != 'exhaustive': pytest.skip()

    # Create a table and load data into a single partition with Hive with
    # stats autogathering.
    table_name = "autogather_test"
    create_load_data_stmts = """
      set hive.stats.autogather=true;
      create table {0}.{1} (c int) partitioned by (p1 int, p2 string);
      insert overwrite table {0}.{1} partition (p1=1, p2="pval")
      select id from functional.alltypestiny;
    """.format(self.TEST_DB_NAME, table_name)
    check_call(["hive", "-e", create_load_data_stmts])

    # Make the table visible in Impala.
    self.execute_query("invalidate metadata %s.%s" % (self.TEST_DB_NAME, table_name))

    # Check that the row count was populated during the insert. We expect 8 rows
    # because functional.alltypestiny has 8 rows, but Hive's auto stats gathering
    # is known to be flaky and sometimes sets the row count to 0. So we check that
    # the row count is not -1 instead of checking for 8 directly.
    show_result = \
      self.execute_query("show table stats %s.%s" % (self.TEST_DB_NAME, table_name))
    assert(len(show_result.data) == 2)
    assert("1\tpval\t-1" not in show_result.data[0])

    # Compute incremental stats on the single test partition.
    self.execute_query("compute incremental stats %s.%s partition (p1=1, p2='pval')"
      % (self.TEST_DB_NAME, table_name))

    # Invalidate metadata to force reloading the stats from the Hive Metastore.
    self.execute_query("invalidate metadata %s.%s" % (self.TEST_DB_NAME, table_name))

    # Check that the row count is still 8.
    show_result = \
      self.execute_query("show table stats %s.%s" % (self.TEST_DB_NAME, table_name))
    assert(len(show_result.data) == 2)
    assert("1\tpval\t8" in show_result.data[0])


@SkipIfS3.insert # S3: missing coverage: compute stats
@SkipIf.not_default_fs # Isilon: Missing coverage: compute stats
class TestCorruptTableStats(TestComputeStats):

  @classmethod
  def add_test_dimensions(cls):
    super(TestComputeStats, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
      disable_codegen_options=[False], exec_single_node_option=[100]))
    # Do not run these tests using all dimensions because the expected results
    # are different for different file formats.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  @pytest.mark.execute_serially
  def test_corrupted_stats(self, vector):
    """IMPALA-1983: Test that in the presence of corrupt table statistics a warning is
    issued and the small query optimization is disabled."""
    if self.exploration_strategy() != 'exhaustive': pytest.skip("Only run in exhaustive")
    self.run_test_case('QueryTest/corrupt_stats', vector)
