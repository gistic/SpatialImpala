# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Unit tests for the test file parser
#
import logging
import pytest
from tests.util.test_file_parser import *
from tests.common.base_test_suite import BaseTestSuite

test_text = """
# Text before in the header (before the first ====) should be ignored
# so put this here to test it out.
====
---- QUERY
# comment
SELECT blah from Foo
s
---- RESULTS
'Hi'
---- TYPES
string
====
---- QUERY
SELECT 2
---- RESULTS
'Hello'
---- TYPES
string
#====
# SHOULD PARSE COMMENTED OUT TEST PROPERLY
#---- QUERY: TEST_WORKLOAD_Q2
#SELECT int_col from Bar
#---- RESULTS
#231
#---- TYPES
#int
====
---- QUERY: TEST_WORKLOAD_Q2
SELECT int_col from Bar
---- RESULTS
231
---- TYPES
int
====
"""

VALID_SECTIONS = ['QUERY', 'RESULTS', 'TYPES']

class TestTestFileParser(BaseTestSuite):
  def test_valid_parse(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS)
    assert len(results) == 3
    print results[0]
    expected_results = {'QUERY': '# comment\nSELECT blah from Foo\ns',
                        'TYPES': 'string', 'RESULTS': "'Hi'"}
    assert results[0] == expected_results

  def test_invalid_section(self):
    # Restrict valid sections to exclude one of the section names.
    valid_sections = ['QUERY', 'RESULTS']
    results = parse_test_file_text(test_text, valid_sections, skip_unknown_sections=True)
    assert len(results) == 3
    expected_results = {'QUERY': '# comment\nSELECT blah from Foo\ns',
                        'RESULTS': "'Hi'"}
    assert results[0] == expected_results

    # In this case, instead of ignoring the invalid section we should get an error
    try:
      results = parse_test_file_text(test_text, valid_sections,
                                     skip_unknown_sections=False)
      assert 0, 'Expected error due to invalid section'
    except RuntimeError as re:
      assert re.message == "Unknown subsection: TYPES"

  def test_parse_query_name(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS, False)
    assert len(results) == 3
    expected_results = {'QUERY': 'SELECT int_col from Bar',
                        'TYPES': 'int', 'RESULTS': '231',
                        'QUERY_NAME': 'TEST_WORKLOAD_Q2'}
    assert results[2] == expected_results

  def test_parse_commented_out_test_as_comment(self):
    results = parse_test_file_text(test_text, VALID_SECTIONS)
    assert len(results) == 3
    expected_results = {'QUERY': 'SELECT 2', 'RESULTS': "'Hello'",
                        'TYPES': "string\n#====\n"\
                        "# SHOULD PARSE COMMENTED OUT TEST PROPERLY\n"\
                        "#---- QUERY: TEST_WORKLOAD_Q2\n"\
                        "#SELECT int_col from Bar\n"\
                        "#---- RESULTS\n#231\n#---- TYPES\n#int"}
    print expected_results
    print results[1]
    assert results[1] == expected_results
