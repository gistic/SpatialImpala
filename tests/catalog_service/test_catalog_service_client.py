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
# Tests to validate the Catalog Service client APIs.

import logging
import pytest
from tests.common.test_vector import *
from tests.common.test_dimensions import *
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster

from CatalogService import CatalogService
from CatalogService.CatalogService import TGetFunctionsRequest, TGetFunctionsResponse
from ErrorCodes.ttypes import TErrorCode
from Status.ttypes import TStatus
from thrift.transport.TSocket import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from tests.util.filesystem_utils import WAREHOUSE
from tests.util.thrift_util import create_transport

LOG = logging.getLogger('test_catalog_service_client')

class TestCatalogServiceClient(ImpalaTestSuite):
  TEST_DB = 'catalog_service_client_test_db'

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCatalogServiceClient, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db(self.TEST_DB)
    self.client.execute("create database %s location '%s/%s.db'" %
                        (self.TEST_DB, WAREHOUSE, self.TEST_DB))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB)

  def test_get_functions(self, vector):
    impala_cluster = ImpalaCluster()
    catalogd = impala_cluster.catalogd.service
    trans_type = 'buffered'
    if pytest.config.option.use_kerberos:
      trans_type = 'kerberos'
    transport = create_transport(host=catalogd.hostname, port=catalogd.service_port,
                                 service='impala', transport_type=trans_type)
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    catalog_client = CatalogService.Client(protocol)

    request = TGetFunctionsRequest()
    request.db_name = self.TEST_DB
    response = catalog_client.GetFunctions(request)
    assert response.status.status_code == TErrorCode.OK
    assert len(response.functions) == 0

    # Add a function and make sure it shows up.
    self.client.execute("create function %s.fn() RETURNS int "\
        "LOCATION '%s/libTestUdfs.so' SYMBOL='Fn'" % (self.TEST_DB, WAREHOUSE))

    response = catalog_client.GetFunctions(request)
    LOG.debug(response)
    assert len(response.functions) == 1
    assert len(response.functions[0].arg_types) == 0
    assert response.functions[0].name.db_name == self.TEST_DB
    assert response.functions[0].name.function_name == 'fn'
    assert response.functions[0].aggregate_fn is None
    assert response.functions[0].scalar_fn is not None
    assert '/test-warehouse/libTestUdfs.so' in response.functions[0].hdfs_location

    # Add another scalar function with overloaded parameters ensure it shows up.
    self.client.execute("create function %s.fn(int) RETURNS double "\
        "LOCATION '%s/libTestUdfs.so' SYMBOL='Fn'" % (self.TEST_DB, WAREHOUSE))
    response = catalog_client.GetFunctions(request)
    LOG.debug(response)
    assert response.status.status_code == TErrorCode.OK
    assert len(response.functions) == 2

    functions = [fn for fn in response.functions]

    # Sort by number of arg in the function (ascending)
    functions.sort(key=lambda fn: len(fn.arg_types))
    assert len(functions[0].arg_types) == 0
    assert len(functions[1].arg_types) == 1
    assert functions[0].signature == 'fn()'
    assert functions[1].signature == 'fn(INT)'

    # Verify aggregate functions can also be retrieved
    self.client.execute("create aggregate function %s.agg_fn(int, string) RETURNS int "\
        "LOCATION '%s/libTestUdas.so' UPDATE_FN='TwoArgUpdate'" %
        (self.TEST_DB, WAREHOUSE))
    response = catalog_client.GetFunctions(request)
    LOG.debug(response)
    assert response.status.status_code == TErrorCode.OK
    assert len(response.functions) == 3
    functions = [fn for fn in response.functions if fn.aggregate_fn is not None]
    # Should be only 1 aggregate function
    assert len(functions) == 1

    # Negative test cases for database name
    request.db_name = self.TEST_DB + "_does_not_exist"
    response = catalog_client.GetFunctions(request)
    LOG.debug(response)
    assert response.status.status_code == TErrorCode.GENERAL
    assert 'Database does not exist: ' in str(response.status)

    request = TGetFunctionsRequest()
    response = catalog_client.GetFunctions(request)
    LOG.debug(response)
    assert response.status.status_code == TErrorCode.GENERAL
    assert 'Database name must be set' in str(response.status)
