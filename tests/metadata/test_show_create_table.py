# Copyright (c) 2013 Cloudera, Inc. All rights reserved.
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
import re
import shlex
from subprocess import call
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.util.filesystem_utils import WAREHOUSE

# The purpose of the show create table tests are to ensure that the "SHOW CREATE TABLE"
# output can actually be used to recreate the table. A test consists of a table
# definition. The table is created, then the output of "SHOW CREATE TABLE" is used to
# test if the table can be recreated. This test class does not support --update-results.
class TestShowCreateTable(ImpalaTestSuite):
  TEST_DB_NAME = "show_create_table_test_db"
  VALID_SECTION_NAMES = ["CREATE_TABLE", "QUERY", "RESULTS"]
  # Properties to filter before comparing results
  FILTER_TBL_PROPERTIES = ["transient_lastDdlTime", "numFiles", "numPartitions",\
                           "numRows", "rawDataSize", "totalSize", "COLUMN_STATS_ACCURATE",
                           "STATS_GENERATED_VIA_STATS_TASK"]

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestShowCreateTable, cls).add_test_dimensions()
    # don't use any exec options, running exactly once is fine
    cls.TestMatrix.clear_dimension('exec_option')
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    """ cleanup and create a fresh test database """
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database %s location '%s/%s.db'" %
                       (self.TEST_DB_NAME, WAREHOUSE, self.TEST_DB_NAME))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  def test_show_create_table(self, vector):
    self.__run_show_create_table_test_case('QueryTest/show-create-table', vector)

  def __run_show_create_table_test_case(self, test_file_name, vector):
    """
    Runs a show-create-table test file, containing the following sections:

    ---- CREATE_TABLE
    contains a table creation statement to create table TABLE_NAME
    ---- RESULTS
    contains the expected result of SHOW CREATE TABLE table_name

    OR

    ---- QUERY
    a show create table query
    ---- RESULTS
    contains the expected output of the SHOW CREATE TABLE query
    """
    sections = self.load_query_test_file(self.get_workload(), test_file_name,\
                                         self.VALID_SECTION_NAMES)
    for test_section in sections:
      test_case = ShowCreateTableTestCase(test_section, test_file_name, self.TEST_DB_NAME)

      if not test_case.existing_table:
        # create table in Impala
        self.__exec(test_case.create_table_sql)
      # execute "SHOW CREATE TABLE ..."
      result = self.__exec(test_case.show_create_table_sql)
      create_table_result = self.__normalize(result.data[0])

      if not test_case.existing_table:
        # drop the table
        self.__exec(test_case.drop_table_sql)

      # check the result matches the expected result
      expected_result = self.__normalize(self.__replace_uri(\
          test_case.expected_result,\
          self.__get_location_uri(create_table_result)))
      self.__compare_result(expected_result, create_table_result)

      if test_case.existing_table:
        continue

      # recreate the table with the result from above
      self.__exec(create_table_result)
      try:
        # we should get the same result from "show create table ..."
        result = self.__exec(test_case.show_create_table_sql)
        new_create_table_result = self.__normalize(result.data[0])
        assert create_table_result == new_create_table_result
      finally:
        # drop the table
        self.__exec(test_case.drop_table_sql)

  def __exec(self, sql_str):
    return self.execute_query_expect_success(self.client, sql_str)

  def __get_location_uri(self, sql_str):
    m = re.search("LOCATION '([^\']+)'", sql_str)
    if m is not None:
      return m.group(1)

  def __compare_result(self, expected_sql, actual_sql):
    """ Extract all properties """
    expected_tbl_props = self.__get_properties_map(expected_sql, "TBLPROPERTIES")
    actual_tbl_props = self.__get_properties_map(actual_sql, "TBLPROPERTIES")
    assert expected_tbl_props == actual_tbl_props

    expected_serde_props = self.__get_properties_map(expected_sql, "SERDEPROPERTIES")
    actual_serde_props = self.__get_properties_map(actual_sql, "SERDEPROPERTIES")
    assert expected_serde_props == actual_serde_props

    expected_sql_filtered = self.__remove_properties_maps(expected_sql)
    actual_sql_filtered = self.__remove_properties_maps(actual_sql)
    assert expected_sql_filtered == actual_sql_filtered

  def __normalize(self, s):
    """ Normalize the string to remove extra whitespaces and remove keys
    from tblproperties and serdeproperties that we don't want
    """
    s = ' '.join(s.split())
    for k in self.FILTER_TBL_PROPERTIES:
      kv_regex = "'%s'\s*=\s*'[^\']+'\s*,?" % (k)
      s = re.sub(kv_regex, "", s)
    # If we removed the last property, there will be a dangling comma that is not valid
    # e.g. 'k1'='v1', ) -> 'k1'='v1')
    s = re.sub(",\s*\)", ")", s)
    # Need to remove any whitespace after left parens and before right parens
    s = re.sub("\(\s+", "(", s)
    s = re.sub("\s+\)", ")", s)
    # If the only properties were removed, the properties sections may be empty, which
    # is not valid
    s = re.sub("TBLPROPERTIES\s*\(\s*\)", "", s)
    s = re.sub("SERDEPROPERTIES\s*\(\s*\)", "", s)
    return s

  def __properties_map_regex(self, name):
    return "%s \(([^)]+)\)" % name

  def __remove_properties_maps(self, s):
    """ Removes the tblproperties and serdeproperties from the string """
    return re.sub(self.__properties_map_regex("WITH SERDEPROPERTIES"), "",\
           re.sub(self.__properties_map_regex("TBLPROPERTIES"), "", s)).strip()

  def __get_properties_map(self, s, properties_map_name):
    """ Extracts a dict of key-value pairs from the sql string s. The properties_map_name
    is the name of the properties map, e.g. 'tblproperties' or 'serdeproperties'
    """
    map_match = re.search(self.__properties_map_regex(properties_map_name), s)
    if map_match is None:
      return dict()
    kv_regex = "'([^\']+)'\s*=\s*'([^\']+)'"
    kv_results = dict(re.findall(kv_regex, map_match.group(1)))
    for filtered_key in self.FILTER_TBL_PROPERTIES:
      if filtered_key in kv_results:
        del kv_results[filtered_key]
    return kv_results

  def __replace_uri(self, s, uri):
    return s if uri is None else s.replace("$$location_uri$$", uri)

# Represents one show-create-table test case. Performs validation of the test sections
# and provides SQL to execute for each section.
class ShowCreateTableTestCase(object):
  def __init__(self, test_section, test_file_name, test_db_name):
    if 'QUERY' in test_section:
      self.existing_table = True
      self.show_create_table_sql = test_section['QUERY'].strip()
    elif 'CREATE_TABLE' in test_section:
      self.existing_table = False
      self.create_table_sql = QueryTestSectionReader.build_query(\
          test_section['CREATE_TABLE'])
      name = self.__get_table_name(self.create_table_sql)
      assert name.find(".") == -1, 'Error in test file %s. Found unexpected table '\
            'name %s that is qualified with a database' % (test_file_name, name)
      self.table_name = test_db_name + '.' + name
      self.create_table_sql =\
          self.create_table_sql.replace(name, self.table_name, 1)
      self.show_create_table_sql = 'show create table %s' % (self.table_name)
      self.drop_table_sql = "drop table %s" % (self.table_name)
    else:
      assert 0, 'Error in test file %s. Test cases require a '\
          'CREATE_TABLE section.\n%s' %\
          (test_file_name, pprint.pformat(test_section))
    self.expected_result = test_section['RESULTS']

  def __get_table_name(self, create_table_sql):
    lexer = shlex.shlex(create_table_sql)
    tokens = list(lexer)
    # sanity check the create table statement
    if len(tokens) < 3 or tokens[0].lower() != "create":
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)
    if tokens[1].lower() != "table" and \
      (tokens[1].lower() != "external" or tokens[2].lower() != "table"):
      assert 0, 'Error in test. Invalid CREATE TABLE statement: %s' % (create_table_sql)

    if tokens[1].lower() == "external":
      # expect "create external table table_name ..."
      return tokens[3]
    else:
      # expect a create table table_name ...
      return tokens[2]
