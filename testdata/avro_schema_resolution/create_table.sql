USE functional_avro_snap;

DROP TABLE IF EXISTS schema_resolution_test;

-- Specify the Avro schema in SERDEPROPERTIES instead of TBLPROPERTIES to validate
-- IMPALA-538. Also, give the table a different column definition (col1, col2) than what
-- is defined in the Avro schema for testing mismatched table/deserializer schemas.
CREATE EXTERNAL TABLE schema_resolution_test (col1 string, col2 string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='{
"name": "a",
"type": "record",
"fields": [
  {"name":"boolean1", "type":"boolean", "default": true},
  {"name":"int1",     "type":"int",     "default": 1},
  {"name":"long1",    "type":"long",    "default": 1},
  {"name":"float1",   "type":"float",   "default": 1.0},
  {"name":"double1",  "type":"double",  "default": 1.0},
  {"name":"string1",  "type":"string",  "default": "default string"},
  {"name":"string2",  "type": ["string", "null"],  "default": ""},
  {"name":"string3",  "type": ["null", "string"],  "default": null}
]}')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/avro_schema_resolution_test/';

LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/avro_schema_resolution/records1.avro' OVERWRITE INTO TABLE schema_resolution_test;
LOAD DATA LOCAL INPATH '${env:IMPALA_HOME}/testdata/avro_schema_resolution/records2.avro' INTO TABLE schema_resolution_test;

-- The following tables are used to test Impala's handling of HIVE-6308 which causes
-- COMPUTE STATS and Hive's ANALYZE TABLE to fail for Avro tables with mismatched
-- column definitions and Avro-schema columns. In such cases, COMPUTE STATS is expected to
-- fail in analysis and not after actually computing stats (IMPALA-867).
-- TODO: The creation of these tables should migrate into our regular data loading
-- framework. There are issues with beeline for these CREATE TABLE stmts (NPEs).

-- No explicit column definitions for an Avro table.
DROP TABLE IF EXISTS alltypes_no_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_no_coldef
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='hdfs://${hiveconf:hive.metastore.warehouse.dir}/avro_schemas/functional/alltypes.json');

-- Column definition list has one more column than the Avro schema.
DROP TABLE IF EXISTS alltypes_extra_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_extra_coldef (
id int,
bool_col boolean,
tinyint_col tinyint,
smallint_col smallint,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp,
extra_col string)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='hdfs://${hiveconf:hive.metastore.warehouse.dir}/avro_schemas/functional/alltypes.json');

-- Column definition list is missing 'tinyint_col' and 'timestamp_col' from the Avro schema.
DROP TABLE IF EXISTS alltypes_missing_coldef;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_missing_coldef (
id int,
bool_col boolean,
smallint_col smallint,
int_col int,
bigint_col bigint,
float_col float,
double_col double,
date_string_col string,
string_col string)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='hdfs://${hiveconf:hive.metastore.warehouse.dir}/avro_schemas/functional/alltypes.json');

-- Matching number of columns and column names, but mismatched type (bigint_col is a string).
DROP TABLE IF EXISTS alltypes_type_mismatch;
CREATE EXTERNAL TABLE IF NOT EXISTS alltypes_type_mismatch (
id int,
bool_col boolean,
tinyint_col tinyint,
smallint_col smallint,
int_col int,
bigint_col string,
float_col float,
double_col double,
date_string_col string,
string_col string,
timestamp_col timestamp)
PARTITIONED BY (year int, month int)
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/test-warehouse/alltypes_avro_snap'
TBLPROPERTIES ('avro.schema.url'='hdfs://${hiveconf:hive.metastore.warehouse.dir}/avro_schemas/functional/alltypes.json');
