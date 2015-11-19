// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.PatternMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class provides a storage API for caching CatalogObjects: databases, tables,
 * and functions and the relevant metadata to go along with them. Although this class is
 * thread safe, it does not guarantee consistency with the MetaStore. It is important
 * to keep in mind that there may be external (potentially conflicting) concurrent
 * metastore updates occurring at any time.
 * The CatalogObject storage hierarchy is:
 * Catalog -> Db -> Table
 *               -> Function
 * Each level has its own synchronization, so the cache of Dbs is synchronized and each
 * Db has a cache of tables which is synchronized independently.
 *
 * The catalog is populated with the impala builtins on startup. Builtins and user
 * functions are treated identically by the catalog. The builtins go in a specific
 * database that the user cannot modify.
 * Builtins are populated on startup in initBuiltins().
 */
public abstract class Catalog {
  private static final Logger LOG = Logger.getLogger(Catalog.class);

  // Initial catalog version.
  public final static long INITIAL_CATALOG_VERSION = 0L;
  public static final String DEFAULT_DB = "default";
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;

  public static final String BUILTINS_DB = "_impala_builtins";

  protected final MetaStoreClientPool metaStoreClientPool_ = new MetaStoreClientPool(0);

  // Cache of authorization policy metadata. Populated from data retried from the
  // Sentry Service, if configured.
  protected AuthorizationPolicy authPolicy_ = new AuthorizationPolicy();

  // Thread safe cache of database metadata. Uses an AtomicReference so reset()
  // operations can atomically swap dbCache_ references.
  // TODO: Update this to use a CatalogObjectCache?
  protected AtomicReference<ConcurrentHashMap<String, Db>> dbCache_ =
      new AtomicReference<ConcurrentHashMap<String, Db>>(
          new ConcurrentHashMap<String, Db>());

  // DB that contains all builtins
  private static Db builtinsDb_;

  // Cache of data sources.
  protected final CatalogObjectCache<DataSource> dataSources_;

  // Cache of known HDFS cache pools. Allows for checking the existence
  // of pools without hitting HDFS.
  protected final CatalogObjectCache<HdfsCachePool> hdfsCachePools_ =
      new CatalogObjectCache<HdfsCachePool>(false);

  /**
   * Creates a new instance of a Catalog. If initMetastoreClientPool is true, will
   * also add META_STORE_CLIENT_POOL_SIZE clients to metastoreClientPool_.
   */
  public Catalog(boolean initMetastoreClientPool) {
    if (initMetastoreClientPool) {
      metaStoreClientPool_.addClients(META_STORE_CLIENT_POOL_SIZE);
    }
    dataSources_ = new CatalogObjectCache<DataSource>();
    builtinsDb_ = new BuiltinsDb(BUILTINS_DB, this);
    addDb(builtinsDb_);
  }

  public Db getBuiltinsDb() { return builtinsDb_; }

  /**
   * Adds a new database to the catalog, replacing any existing database with the same
   * name. Returns the previous database with this name, or null if there was no
   * previous database.
   */
  public Db addDb(Db db) {
    return dbCache_.get().put(db.getName().toLowerCase(), db);
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  public Db getDb(String dbName) {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbCache_.get().get(dbName.toLowerCase());
  }

  /**
   * Removes a database from the metadata cache. Returns the value removed or null
   * if not database was removed as part of this operation. Used by DROP DATABASE
   * statements.
   */
  public Db removeDb(String dbName) {
    return dbCache_.get().remove(dbName.toLowerCase());
  }

  /**
   * Returns a list of databases that match dbPattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches everything).
   */
  public List<String> getDbNames(String dbPattern) {
    return filterStringsByPattern(dbCache_.get().keySet(), dbPattern);
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   */
  public Table getTable(String dbName, String tableName) throws
      CatalogException {
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db.getTable(tableName);
  }

  /**
   * Removes a table from the catalog and returns the table that was removed, or null
   * if the table/database does not exist.
   */
  public Table removeTable(TTableName tableName) {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(tableName.getDb_name());
    if (db == null) return null;
    return db.removeTable(tableName.getTable_name());
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern. See filterStringsByPattern for details of the pattern match semantics.
   *
   * dbName must not be null, but tablePattern may be null (and thus matches
   * everything).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return filterStringsByPattern(db.getAllTableNames(), tablePattern);
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if either the table or the database do not exist.
   */
  public boolean containsTable(String dbName, String tableName) {
    Db db = getDb(dbName);
    return (db == null) ? false : db.containsTable(tableName);
  }

  /**
   * Adds a data source to the in-memory map of data sources. It is not
   * persisted to the metastore.
   * @return true if this item was added or false if the existing value was preserved.
   */
  public boolean addDataSource(DataSource dataSource) {
    return dataSources_.add(dataSource);
  }

  /**
   * Removes a data source from the in-memory map of data sources.
   * @return the item that was removed if it existed in the cache, null otherwise.
   */
  public DataSource removeDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.remove(dataSourceName.toLowerCase());
  }

  /**
   * Gets the specified data source.
   */
  public DataSource getDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.get(dataSourceName.toLowerCase());
  }

  /**
   * Gets a list of all data sources.
   */
  public List<DataSource> getDataSources() {
    return dataSources_.getValues();
  }

  /**
   * Returns a list of data sources names that match pattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * pattern may be null (and thus matches everything).
   */
  public List<String> getDataSourceNames(String pattern) {
    return filterStringsByPattern(dataSources_.keySet(), pattern);
  }

  /**
   * Returns a list of data sources that match pattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * pattern may be null (and thus matches everything).
   */
  public List<DataSource> getDataSources(String pattern) {
    List<String> names = filterStringsByPattern(dataSources_.keySet(), pattern);
    List<DataSource> dataSources = Lists.newArrayListWithCapacity(names.size());
    for (String name: names) {
      dataSources.add(dataSources_.get(name));
    }
    return dataSources;
  }

  /**
   * Adds a function to the catalog.
   * Returns true if the function was successfully added.
   * Returns false if the function already exists.
   * TODO: allow adding a function to a global scope. We probably want this to resolve
   * after the local scope.
   * e.g. if we had fn() and db.fn(). If the current database is 'db', fn() would
   * resolve first to db.fn().
   */
  public boolean addFunction(Function fn) {
    Db db = getDb(fn.dbName());
    if (db == null) return false;
    return db.addFunction(fn);
  }

  /**
   * Returns the function that best matches 'desc' that is registered with the
   * catalog using 'mode' to check for matching. If desc matches multiple functions
   * in the catalog, it will return the function with the strictest matching mode.
   * If multiple functions match at the same matching mode, ties are broken by comparing
   * argument types in lexical order. Argument types are ordered by argument precision
   * (e.g. double is preferred over float) and then by alphabetical order of argument
   * type name, to guarantee deterministic results.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.getFunction(desc, mode);
  }

  public static Function getBuiltin(Function desc, Function.CompareMode mode) {
    return builtinsDb_.getFunction(desc, mode);
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   */
  public Function removeFunction(Function desc) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.removeFunction(desc);
  }

  /**
   * Returns true if there is a function with this function name. Parameters
   * are ignored.
   */
  public boolean containsFunction(FunctionName name) {
    Db db = getDb(name.getDb());
    if (db == null) return false;
    return db.containsFunction(name.getFunction());
  }

  /**
   * Adds a new HdfsCachePool to the catalog.
   */
  public boolean addHdfsCachePool(HdfsCachePool cachePool) {
    return hdfsCachePools_.add(cachePool);
  }

  /**
   * Gets a HdfsCachePool given a cache pool name. Returns null if the cache
   * pool does not exist.
   */
  public HdfsCachePool getHdfsCachePool(String poolName) {
    return hdfsCachePools_.get(poolName);
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() { metaStoreClientPool_.close(); }


  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() { return metaStoreClientPool_.getClient(); }

  /**
   * Implement Hive's pattern-matching semantics for SHOW statements. The only
   * metacharacters are '*' which matches any string of characters, and '|'
   * which denotes choice.  Doing the work here saves loading tables or
   * databases from the metastore (which Hive would do if we passed the call
   * through to the metastore client).
   *
   * If matchPattern is null, all strings are considered to match. If it is the
   * empty string, no strings match.
   */
  private List<String> filterStringsByPattern(Iterable<String> candidates,
      String matchPattern) {
    List<String> filtered = Lists.newArrayList();
    if (matchPattern == null) {
      filtered = Lists.newArrayList(candidates);
    } else {
      PatternMatcher matcher = PatternMatcher.createHivePatternMatcher(matchPattern);
      for (String candidate: candidates) {
        if (matcher.matches(candidate)) filtered.add(candidate);
      }
    }
    Collections.sort(filtered, String.CASE_INSENSITIVE_ORDER);
    return filtered;
  }

  /**
   * Returns the HdfsPartition object for the given dbName/tableName and partition spec.
   * This will trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws PartitionNotFoundException - If the partition does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public HdfsPartition getHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
    String partitionNotFoundMsg =
        "Partition not found: " + Joiner.on(", ").join(partitionSpec);
    Table table = getTable(dbName, tableName);
    // This is not an Hdfs table, throw an error.
    if (!(table instanceof HdfsTable)) {
      throw new PartitionNotFoundException(partitionNotFoundMsg);
    }
    // Get the HdfsPartition object for the given partition spec.
    HdfsPartition partition =
        ((HdfsTable) table).getPartitionFromThriftPartitionSpec(partitionSpec);
    if (partition == null) throw new PartitionNotFoundException(partitionNotFoundMsg);
    return partition;
  }

  /**
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public boolean containsHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws CatalogException {
    try {
      return getHdfsPartition(dbName, tableName, partitionSpec) != null;
    } catch (PartitionNotFoundException e) {
      return false;
    }
  }

  /**
   * Gets the thrift representation of a catalog object, given the "object
   * description". The object description is just a TCatalogObject with only the
   * catalog object type and object name set.
   * If the object is not found, a CatalogException is thrown.
   */
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    TCatalogObject result = new TCatalogObject();
    switch (objectDesc.getType()) {
      case DATABASE: {
        Db db = getDb(objectDesc.getDb().getDb_name());
        if (db == null) {
          throw new CatalogException(
              "Database not found: " + objectDesc.getDb().getDb_name());
        }
        result.setType(db.getCatalogObjectType());
        result.setCatalog_version(db.getCatalogVersion());
        result.setDb(db.toThrift());
        break;
      }
      case TABLE:
      case VIEW: {
        Table table = getTable(objectDesc.getTable().getDb_name(),
            objectDesc.getTable().getTbl_name());
        if (table == null) {
          throw new CatalogException("Table not found: " +
              objectDesc.getTable().getTbl_name());
        }
        result.setType(table.getCatalogObjectType());
        result.setCatalog_version(table.getCatalogVersion());
        result.setTable(table.toThrift());
        break;
      }
      case FUNCTION: {
        TFunction tfn = objectDesc.getFn();
        Function desc = Function.fromThrift(tfn);
        Function fn = getFunction(desc, Function.CompareMode.IS_INDISTINGUISHABLE);
        if (fn == null) {
          throw new CatalogException("Function not found: " + tfn);
        }
        result.setType(fn.getCatalogObjectType());
        result.setCatalog_version(fn.getCatalogVersion());
        result.setFn(fn.toThrift());
        break;
      }
      case DATA_SOURCE: {
        String dataSrcName = objectDesc.getData_source().getName();
        DataSource dataSrc = getDataSource(dataSrcName);
        if (dataSrc == null) {
          throw new CatalogException("Data source not found: " + dataSrcName);
        }
        result.setType(dataSrc.getCatalogObjectType());
        result.setCatalog_version(dataSrc.getCatalogVersion());
        result.setData_source(dataSrc.toThrift());
        break;
      }
      case HDFS_CACHE_POOL: {
        HdfsCachePool pool = getHdfsCachePool(objectDesc.getCache_pool().getPool_name());
        if (pool == null) {
          throw new CatalogException(
              "Hdfs cache pool not found: " + objectDesc.getCache_pool().getPool_name());
        }
        result.setType(pool.getCatalogObjectType());
        result.setCatalog_version(pool.getCatalogVersion());
        result.setCache_pool(pool.toThrift());
        break;
      }
      case ROLE:
        Role role = authPolicy_.getRole(objectDesc.getRole().getRole_name());
        if (role == null) {
          throw new CatalogException("Role not found: " +
              objectDesc.getRole().getRole_name());
        }
        result.setType(role.getCatalogObjectType());
        result.setCatalog_version(role.getCatalogVersion());
        result.setRole(role.toThrift());
        break;
      case PRIVILEGE:
        Role tmpRole = authPolicy_.getRole(objectDesc.getPrivilege().getRole_id());
        if (tmpRole == null) {
          throw new CatalogException("No role associated with ID: " +
              objectDesc.getPrivilege().getRole_id());
        }
        for (RolePrivilege p: tmpRole.getPrivileges()) {
          if (p.getName().equalsIgnoreCase(
              objectDesc.getPrivilege().getPrivilege_name())) {
            result.setType(p.getCatalogObjectType());
            result.setCatalog_version(p.getCatalogVersion());
            result.setPrivilege(p.toThrift());
            return result;
          }
        }
        throw new CatalogException(String.format("Role '%s' does not contain " +
            "privilege: '%s'", tmpRole.getName(),
            objectDesc.getPrivilege().getPrivilege_name()));
      default: throw new IllegalStateException(
          "Unexpected TCatalogObject type: " + objectDesc.getType());
    }
    return result;
  }
}
