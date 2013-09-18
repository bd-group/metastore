/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DiskManager.DeviceInfo;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Datacenter;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SFileRef;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MUser;
import org.apache.thrift.TException;

/**
 *
 * DummyRawStoreForJdoConnection.
 *
 * An implementation of RawStore that verifies the DummyJdoConnectionUrlHook has already been
 * applied when this class's setConf method is called, by checking that the value of the
 * METASTORECONNECTURLKEY ConfVar has been updated.
 *
 * All non-void methods return default values.
 */
public class DummyRawStoreForJdoConnection implements RawStore {

  @Override
  public Configuration getConf() {

    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    String expected = DummyJdoConnectionUrlHook.newUrl;
    String actual = arg0.get(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname);

    Assert.assertEquals("The expected URL used by JDO to connect to the metastore: " + expected +
        " did not match the actual value when the Raw Store was initialized: " + actual,
        expected, actual);
  }

  @Override
  public void shutdown() {


  }

  @Override
  public boolean openTransaction() {

    return false;
  }

  @Override
  public boolean commitTransaction() {

    return false;
  }

  @Override
  public void rollbackTransaction() {


  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {


  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {

    return null;
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {

    return false;
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {

    return false;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {

    return null;
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {

    return null;
  }

  @Override
  public boolean createType(Type type) {

    return false;
  }

  @Override
  public Type getType(String typeName) {

    return null;
  }

  @Override
  public boolean dropType(String typeName) {

    return false;
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {


  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException {

    return false;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {

    return null;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {

    return false;
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException {

    return false;
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException {

    return null;
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException,
      MetaException {


  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {

    return null;
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {

    return null;
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {

    return null;
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {

    return null;
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws MetaException {

    return null;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
      short max_parts) throws MetaException {

    return null;
  }


  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {

    return false;
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {

    return null;
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {

    return false;
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {

    return null;
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max)
      throws MetaException {

    return null;
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {


  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
      short maxParts) throws MetaException, NoSuchObjectException {

    return null;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {

    return null;
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
      PartitionEventType evtType) throws MetaException, UnknownTableException,
      InvalidPartitionException, UnknownPartitionException {

    return null;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {

    return false;
  }

  @Override
  public boolean addRole(String rowName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return false;
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType)
      throws MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {

    return null;
  }

  @Override
  public List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {

    return null;
  }

  @Override
  public List<MTablePrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName) {

    return null;
  }

  @Override
  public List<MPartitionPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String partName) {

    return null;
  }

  @Override
  public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String columnName) {

    return null;
  }

  @Override
  public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String partName,
      String columnName) {

    return null;
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {

    return null;
  }

  @Override
  public List<String> listRoleNames() {

    return null;
  }

  @Override
  public List<MRoleMap> listRoles(String principalName, PrincipalType principalType) {

    return null;
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return null;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return null;
  }

  @Override
  public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, NoSuchObjectException {

    return null;
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {

    return null;
  }

  @Override
  public long cleanupEvents() {

    return 0;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName, String colName)
      throws MetaException, NoSuchObjectException {
    return null;
  }


  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName,
                                              String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }


  public boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVals, String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException,
    InvalidInputException {
    return false;

  }

  @Override
  public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVal, String colName) throws MetaException,
    NoSuchObjectException, InvalidInputException, InvalidObjectException  {
    return null;
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,List<String> partVals)
    throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }

  @Override
  public Node findNode(String ip) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void createOrUpdateDevice(DeviceInfo di, Node node) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void createNode(Node node) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean updateNode(Node node) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long countNode() throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void createFile(SFile file) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public SFile getSFile(long fid) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean delNode(String node_name) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Table getTableByID(long id) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getTableOID(String dbName, String tableName) throws MetaException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public SFile updateSFile(SFile newfile) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean createFileLocation(SFileLocation location) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Node getNode(String node_name) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean delSFile(long fid) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<SFileLocation> getSFileLocations(long fid) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public SFileLocation updateSFileLocation(SFileLocation newsfl) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override
  public List<Node> getAllNodes() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition getPartition(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updatePartition(Partition newPart) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alterPartition(String db_name, String tbl_name, String partName,
      List<String> part_vals, Partition new_part) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<String> partNames,
      List<List<String>> part_vals_list, List<Partition> new_parts) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<SFile> findUnderReplicatedFiles() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SFile> findOverReplicatedFiles() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SFile> findLingeringFiles(long node_nr) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition getPartition(String db_name, String tbl_name, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SFileLocation> getSFileLocations(String devid, long curts, long timeout)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Subpartition getSubpartition(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateSubpartition(Subpartition newPart) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void createPartitionIndex(Index index, Partition part) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void createPartitionIndex(Index index, Subpartition part) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean dropPartitionIndex(Index index, Partition part) throws InvalidObjectException,
      NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dropPartitionIndex(Index index, Subpartition part) throws InvalidObjectException,
      NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void createPartitionIndexStores(Index index, Partition part, List<SFile> store,
      List<Long> originFid) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void createPartitionIndexStores(Index index, Subpartition part, List<SFile> store,
      List<Long> originFid) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean dropPartitionIndexStores(Index index, Partition part, List<SFile> store)
      throws InvalidObjectException, NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dropPartitionIndexStores(Index index, Subpartition part, List<SFile> store)
      throws InvalidObjectException, NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, String part_name)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Datacenter getDatacenter(String name) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void createDatacenter(Datacenter dc) throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean add_datawarehouse_sql(int dwNum, String sql) throws InvalidObjectException,
      MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<SFileRef> getPartitionIndexFiles(Index index, Partition part)
      throws InvalidObjectException, NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SFileRef> getSubpartitionIndexFiles(Index index, Subpartition subpart)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Subpartition> getSubpartitions(String dbname, String tbl_name, Partition part)
      throws InvalidObjectException, NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setThisDC(String thisDC) {
    // TODO Auto-generated method stub

  }

  @Override
  public List<BusiTypeColumn> getAllBusiTypeCols() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition getParentPartition(String dbName, String tableName, String subpart_name)
      throws NoSuchObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Datacenter> getAllDatacenters() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean updateDatacenter(Datacenter dc) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dropDatacenter(String dc_name) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<BusiTypeDatacenter> get_all_busi_type_datacenters() throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void append_busi_type_datacenter(BusiTypeDatacenter busiTypeDatacenter)
      throws InvalidObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<SFileLocation> getSFileLocations(int status) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SFile getSFile(String devid, String location) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SFileLocation getSFileLocation(String devid, String location) throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean delSFileLocation(String devid, String location) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<Busitype> showBusitypes() throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int createBusitype(Busitype busitype) throws InvalidObjectException, MetaException,
      TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void findFiles(List<SFile> underReplicated, List<SFile> overReplicated,
      List<SFile> lingering) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public void findVoidFiles(List<SFile> voidFiles) throws MetaException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean addEquipRoom(EquipRoom er) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean modifyEquipRoom(EquipRoom er) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deleteEquipRoom(EquipRoom er) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<EquipRoom> listEquipRoom() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean addGeoLocation(GeoLocation gl) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean modifyGeoLocation(GeoLocation gl) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deleteGeoLocation(GeoLocation gl) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<GeoLocation> listGeoLocation() throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean addUser(String userName, String passwd, String ownerName)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean removeUser(String userName) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> listUsersNames() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean authentication(String userName, String passwd) throws MetaException,
      NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public MUser getMUser(String user) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Device getDevice(String devid) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean delDevice(String devid) throws MetaException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean modifyUser(User user) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return false;
  }

}


