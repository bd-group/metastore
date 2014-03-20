package org.apache.hadoop.hive.metastore.newms;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreateOperation;
import org.apache.hadoop.hive.metastore.api.CreatePolicy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FOFailReason;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.FindNodePolicy;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MSOperation;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SFileRef;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.newms.DiskManager.DMProfile;
import org.apache.hadoop.hive.metastore.newms.DiskManager.DMRequest;
import org.apache.hadoop.hive.metastore.newms.DiskManager.FileLocatingPolicy;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;
import com.google.common.collect.Lists;

/*
 * 没缓存的对象，但是rpc里要得到的
 * BusiTypeColumn，Device,ColumnStatistics,Type,role,User,HiveObjectPrivilege
 */

public class ThriftRPC implements org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface{

	private NewMSConf conf;
	private RawStore rs;
	private IMetaStoreClient client;
	private static final Log LOG= NewMS.LOG;
	private DiskManager dm;
	private final long startTimeMillis;
	public static Long file_creation_lock = 0L;
  public static Long file_reopen_lock = 0L;
  
	public ThriftRPC(NewMSConf conf)
	{
		this.conf = conf;
		rs = new RawStoreImp(conf);
		startTimeMillis = System.currentTimeMillis();
		try {
			client = MsgProcessing.createMetaStoreClient();
			dm = new DiskManager(new HiveConf(DiskManager.class), LOG);
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	/**
   * return the alive timemillis since last set up
   * the {@code startTimeMillis} is inited in the constructor method {@link #ThriftRPC(NewMSConf)}
   *
   * @author mzy
   * @return {@code System.currentTimeMillis - startTimeMillis}
   */
  @Override
  public long aliveSince() throws TException {
    return System.currentTimeMillis() - startTimeMillis;
  }

  @Override
  public long getCounter(String arg0) throws TException {
    if (isNullOrEmpty(arg0)) {
      return -1;
    }
    return 0;
  }

	@Override
	public Map<String, Long> getCounters() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCpuProfile(int profileDurationInSec) throws TException {
		return "";
	}

	@Override
	public String getName() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getOption(String arg0) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getOptions() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public fb_status getStatus() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStatusDetails() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getVersion() throws TException {
		return "0.1";
	}

	@Override
	public void reinitialize() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setOption(String arg0, String arg1) throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() throws TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean addEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		return arg0 != null && client.addEquipRoom(arg0);
	}

	@Override
	public boolean addGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		return arg0 != null && client.addGeoLocation(arg0);
	}

	@Override
	public boolean addNodeAssignment(String nodeName, String dbName)
			throws MetaException, NoSuchObjectException, TException {
		final boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr && client.addNodeAssignment(nodeName, dbName);
	}

	@Override
  public boolean addNodeGroup(NodeGroup nodeGroup) throws AlreadyExistsException,
      MetaException, TException {

    return nodeGroup != null && client.addNodeGroup(nodeGroup);
  }

	@Override
  public boolean addNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    final boolean expr = nodeGroup == null || isNullOrEmpty(dbName);
    return !expr && client.addNodeGroupAssignment(nodeGroup, dbName);
  }

  @Override
  public boolean addRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(roleName) || isNullOrEmpty(dbName);
    return !expr && client.addRoleAssignment(roleName, dbName);
  }

  @Override
  public boolean addTableNodeDist(String dbName, String tableName, List<String> nodeGroupList)
      throws MetaException, TException {
    final boolean expr = isNullOrEmpty(dbName) || isNullOrEmpty(tableName) || nodeGroupList == null;
    return !expr && client.addTableNodeDist(dbName, tableName, nodeGroupList);
  }

  @Override
  public boolean addUserAssignment(String userName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(userName) || isNullOrEmpty(dbName);
    return !expr && client.addUserAssignment(userName, dbName);
  }

  @Override
  public boolean add_datawarehouse_sql(int arg0, String arg1)
      throws InvalidObjectException, MetaException, TException {

    return false;
  }

	@Override
	public Index add_index(Index arg0, Table arg1)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

  @Override
  public Node add_node(String nodeName, List<String> ipl) throws MetaException,
      TException {
    final boolean expr = isNullOrEmpty(nodeName) || ipl == null;
    checkArgument(expr, "nodeName and ipl shuldn't be null or empty");
    final Node node = client.add_node(nodeName, ipl);
    return node;
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return client.add_partition(checkNotNull(partition, "partation shuldn't be null"));
  }

  @Override
  public int add_partition_files(Partition partition, List<SFile> sfiles)
      throws TException {
    checkNotNull(partition);
    checkNotNull(sfiles);
    return client.add_partition_files(partition, sfiles);
  }

  @Override
  public boolean add_partition_index(Index index, Partition partition)
      throws MetaException, AlreadyExistsException, TException {

    return client.add_partition_index(checkNotNull(index), checkNotNull(partition));
  }

	@Override
	public boolean add_partition_index_files(Index arg0, Partition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Partition add_partition_with_environment_context(Partition arg0,
			EnvironmentContext arg1) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int add_partitions(List<Partition> arg0)
			throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition(String arg0, String arg1,
			List<String> arg2, Subpartition arg3) throws TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int add_subpartition_files(Subpartition arg0, List<SFile> arg1)
			throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean add_subpartition_index(Index arg0, Subpartition arg1)
			throws MetaException, AlreadyExistsException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean add_subpartition_index_files(Index arg0, Subpartition arg1,
			List<SFile> arg2, List<Long> arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean alterNodeGroup(NodeGroup arg0)
			throws AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void alter_database(String arg0, Database arg1)
			throws MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_index(String arg0, String arg1, String arg2, Index arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Node alter_node(String arg0, List<String> arg1, int arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void alter_partition(String arg0, String arg1, Partition arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partition_with_environment_context(String arg0,
			String arg1, Partition arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_partitions(String arg0, String arg1, List<Partition> arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table(String arg0, String arg1, Table arg2)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void alter_table_with_environment_context(String arg0, String arg1,
			Table arg2, EnvironmentContext arg3)
			throws InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void append_busi_type_datacenter(BusiTypeDatacenter arg0)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Partition append_partition(String arg0, String arg1,
			List<String> arg2) throws InvalidObjectException,
			AlreadyExistsException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition append_partition_by_name(String arg0, String arg1,
			String arg2) throws InvalidObjectException, AlreadyExistsException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean assiginSchematoDB(String arg0, String arg1,
			List<FieldSchema> arg2, List<FieldSchema> arg3, List<NodeGroup> arg4)
			throws InvalidObjectException, NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean authentication(String arg0, String arg1)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void cancel_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int close_file(SFile file) throws FileOperationException, MetaException, TException {
		startFunction("close_file ", "fid: " + file.getFid());
    DMProfile.fcloseR.incrementAndGet();

    FileOperationException e = null;
    SFile saved = rs.getSFile(file.getFid());

    try {
      if (saved == null) {
        throw new FileOperationException("Can not find SFile by FID" + file.getFid(), FOFailReason.INVALID_FILE);
      }

      if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        LOG.error("File StoreStatus is not in INCREATE (vs " + saved.getStore_status() + ").");
        throw new FileOperationException("File StoreStatus is not in INCREATE (vs " + saved.getStore_status() + ").",
            FOFailReason.INVALID_STATE);
      }

      // find the valid filelocation, mark it and trigger replication
      if (file.getLocationsSize() > 0) {
        int valid_nr = 0;

        // find valid Online NR
        for (SFileLocation sfl : file.getLocations()) {
          if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            valid_nr++;
          }
        }
        if (valid_nr > 1) {
          LOG.error("Too many file locations provided, expect 1 provided " + valid_nr + " [NOT CLOSED]");
          throw new FileOperationException("Too many file locations provided, expect 1 provided " + valid_nr + " [NOT CLOSED]",
              FOFailReason.INVALID_FILE);
        } else if (valid_nr < 1) {
          LOG.error("Too little file locations provided, expect 1 provided " + valid_nr + " [CLOSED]");
          e = new FileOperationException("Too little file locations provided, expect 1 provided " + valid_nr + " [CLOSED]",
              FOFailReason.INVALID_FILE);
        }
        // finally, do it
        List<SFileLocation> sflToDel = new ArrayList<SFileLocation>();
        for (SFileLocation sfl : file.getLocations()) {
          if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            sfl.setRep_id(0);
            sfl.setDigest(file.getDigest());
            rs.updateSFileLocation(sfl);
          } else {
            sflToDel.add(sfl);
            dm.asyncDelSFL(sfl);
          }
        }
        // BUG-XXX: trunc offline sfls
        if (sflToDel.size() > 0) {
          file.getLocations().removeAll(sflToDel);
        }
      } else {
        LOG.error("Too little file locations provided, expect 1 provided " + file.getLocationsSize() + " [CLOSED]");
        e = new FileOperationException("Too little file locations provided, expect 1 provided " + file.getLocationsSize() + " [CLOSED]",
              FOFailReason.INVALID_FILE);
      }

      file.setStore_status(MetaStoreConst.MFileStoreStatus.CLOSED);
      // keep repnr unchanged
      file.setRep_nr(saved.getRep_nr());
      rs.updateSFile(file);

      if (e != null) {
        throw e;
      }

      synchronized (dm.repQ) {
        dm.repQ.add(new DMRequest(file, DMRequest.DMROperation.REPLICATE, 1));
        dm.repQ.notify();
      }
    } finally {
      DMProfile.fcloseSuccRS.incrementAndGet();
    }
    return 0;
	}

	public String startFunction(String function, String extraLogInfo) {
//    incrementCounter(function);
//    logInfo((getIpAddress() == null ? "" : "source:" + getIpAddress() + " ") + function + extraLogInfo);
    try {
      Metrics.startScope(function);
    } catch (IOException e) {
      LOG.debug("Exception when starting metrics scope"
          + e.getClass().getName() + " " + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
    }
    return function;
  }
	
	@Override
	public int createBusitype(Busitype arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean createSchema(GlobalSchema arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_attribution(Database arg0)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_database(Database arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Device create_device(String arg0, int arg1, String arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SFile create_file(String node_name, int repnr, String db_name, String table_name, List<SplitValue> values) 
			throws FileOperationException, TException {
		DMProfile.fcreate1R.incrementAndGet();
    if (!fileSplitValuesCheck(values)) {
      throw new FileOperationException("Invalid File Split Values: inconsistent version among values?", FOFailReason.INVALID_FILE);
    }
    // TODO: if repnr less than 1, we should increase it to replicate to BACKUP-STORE
    if (repnr <= 1) {
      repnr++;
    }

    Set<String> excl_node = new TreeSet<String>();
    Set<String> excl_dev = new TreeSet<String>();
    Set<String> spec_node = new TreeSet<String>();

    dm.findBackupDevice(excl_dev, excl_node);

    // check if we should create in table's node group
    if (node_name == null && db_name != null && table_name != null) {
      try {
        Table tbl = rs.getTable(db_name, table_name);
        if (tbl.getNodeGroupsSize() > 0) {
          for (NodeGroup ng : tbl.getNodeGroups()) {
            if (ng.getNodesSize() > 0) {
              for (Node n : ng.getNodes()) {
                spec_node.add(n.getNode_name());
              }
            }
          }
        }
      } catch (MetaException me) {
        throw new FileOperationException("getTable:" + db_name + "." + table_name + " + " + me.getMessage(), FOFailReason.INVALID_TABLE);
      }
    }
    // do not select the backup/shared device for the first entry
    FileLocatingPolicy flp;

    if (spec_node.size() > 0) {
      flp = new FileLocatingPolicy(spec_node, excl_dev, FileLocatingPolicy.SPECIFY_NODES, FileLocatingPolicy.RANDOM_DEVS, false);
    } else {
      flp = new FileLocatingPolicy(null, excl_dev, FileLocatingPolicy.EXCLUDE_NODES, FileLocatingPolicy.RANDOM_DEVS, false);
    }

    SFile rf = create_file(flp, node_name, repnr, db_name, table_name, values);
    DMProfile.fcreate1SuccR.incrementAndGet();

    return rf;
	}
	//copy from HiveMetaStore
	 public boolean fileSplitValuesCheck(List<SplitValue> values) {
     long version = -1;

     if (values == null || values.size() <= 0) {
       return true;
     }

     for (SplitValue sv : values) {
       if (version == -1) {
         version = sv.getVerison();
       }
       if (version != sv.getVerison()) {
         return false;
       }
     }

     return true;
   }
	//copy from HiveMetaStore
	private SFile create_file(FileLocatingPolicy flp, String node_name, int repnr, String db_name, String table_name, List<SplitValue> values)
      throws FileOperationException, TException {
		Random rand = new Random();
    String table_path = null;

    if (node_name == null) {
      // this means we should select Best Available Node and Best Available Device;
      try {
        node_name = dm.findBestNode(flp);
        if (node_name == null) {
          throw new IOException("Folloing the FLP(" + flp + "), we can't find any available node now.");
        }
      } catch (IOException e) {
        LOG.error(e, e);
        throw new FileOperationException("Can not find any Best Available Node now, please retry", FOFailReason.SAFEMODE);
      }
    }

    SFile cfile = null;

    // Step 1: find best device to put a file
    if (dm == null) {
      return null;
    }
    try {
      if (flp == null) {
        flp = new FileLocatingPolicy(null, null, FileLocatingPolicy.EXCLUDE_NODES, FileLocatingPolicy.EXCLUDE_DEVS_SHARED, true);
      }
      String devid = dm.findBestDevice(node_name, flp);

      if (devid == null) {
        throw new FileOperationException("Can not find any available device on node '" + node_name + "' now", FOFailReason.NOTEXIST);
      }
      // try to parse table_name
      if (db_name != null && table_name != null) {
        Table tbl;
        try {
          tbl = rs.getTable(db_name, table_name);
        } catch (MetaException me) {
          throw new FileOperationException("Invalid DB or Table name:" + db_name + "." + table_name + " + " + me.getMessage(), FOFailReason.INVALID_TABLE);
        }
        if (tbl == null) {
          throw new FileOperationException("Invalid DB or Table name:" + db_name + "." + table_name, FOFailReason.INVALID_TABLE);
        }
        table_path = tbl.getDbName() + "/" + tbl.getTableName();
      }

      // how to convert table_name to tbl_id?
      cfile = new SFile(0, db_name, table_name, MetaStoreConst.MFileStoreStatus.INCREATE, repnr,
          "SFILE_DEFALUT", 0, 0, null, 0, null, values, MetaStoreConst.MFileLoadStatus.OK);
      cfile = rs.createFile(cfile);
      //cfile = getMS().getSFile(cfile.getFid());
      if (cfile == null) {
        throw new FileOperationException("Creating file with internal error, metadata inconsistent?", FOFailReason.INVALID_FILE);
      }

      do {
        String location = "/data/";

        if (table_path == null) {
          location += "UNNAMED-DB/UNNAMED-TABLE/" + rand.nextInt(Integer.MAX_VALUE);
        } else {
          location += table_path + "/" + rand.nextInt(Integer.MAX_VALUE);
        }
        SFileLocation sfloc = new SFileLocation(node_name, cfile.getFid(), devid, location, 0, System.currentTimeMillis(),
            MetaStoreConst.MFileLocationVisitStatus.OFFLINE, "SFL_DEFAULT");
        if (!rs.createFileLocation(sfloc)) {
          continue;
        }
        List<SFileLocation> sfloclist = new ArrayList<SFileLocation>();
        sfloclist.add(sfloc);
        cfile.setLocations(sfloclist);
        break;
      } while (true);
    } catch (IOException e) {
      throw new FileOperationException("System might in Safe Mode, please wait ... {" + e + "}", FOFailReason.SAFEMODE);
    } catch (InvalidObjectException e) {
      throw new FileOperationException("Internal error: " + e.getMessage(), FOFailReason.INVALID_FILE);
    }

    return cfile;
  }
	
	@Override
	public SFile create_file_by_policy(CreatePolicy policy, int repnr, String db_name, String table_name, List<SplitValue> values)
			throws FileOperationException, TException {
		DMProfile.fcreate2R.incrementAndGet();
    Table tbl = null;
    List<NodeGroup> ngs = null;
    Set<String> ngnodes = new HashSet<String>();

    // Step 1: parse the policy and check arguments
    switch (policy.getOperation()) {
    case CREATE_NEW_IN_NODEGROUPS:
    case CREATE_NEW:
    case CREATE_NEW_RANDOM:
    case CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST:
      // check db, table now
      try {
        tbl = rs.getTable(db_name, table_name);
      } catch (MetaException me) {
        throw new FileOperationException("getTable:" + db_name + "." + table_name + " + " + me.getMessage(), FOFailReason.INVALID_TABLE);
      }
      if (tbl == null) {
          throw new FileOperationException("Invalid DB or Table name:" + db_name + "." + table_name, FOFailReason.INVALID_TABLE);
      }

      // check nodegroups now
      if (policy.getOperation() == CreateOperation.CREATE_NEW_IN_NODEGROUPS) {
        if (policy.getArgumentsSize() <= 0) {
          throw new FileOperationException("Invalid arguments in CreatePolicy.", FOFailReason.INVALID_NODE_GROUPS);
        }
        ngs = tbl.getNodeGroups();
        if (ngs != null && ngs.size() > 0) {
           for (NodeGroup ng : ngs) {
             if (ng.getNodesSize() > 0) {
               for (Node n : ng.getNodes()) {
                 ngnodes.add(n.getNode_name());
               }
             }
           }
        }
        for (String ng : policy.getArguments()) {
          if (!ngnodes.contains(ng)) {
            throw new FileOperationException("Invalid node groups set in CreatePolicy.", FOFailReason.INVALID_NODE_GROUPS);
          }
        }
      } else {
        ngs = tbl.getNodeGroups();
      }
      // check values now
      if (values == null || values.size() == 0) {
        throw new FileOperationException("Invalid file split values.", FOFailReason.INVALID_SPLIT_VALUES);
      }
      List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(tbl.getFileSplitKeys());
      List<PartitionInfo> pis = new ArrayList<PartitionInfo>();
      // find the max version
      long version = 0;
      for (PartitionInfo pi : allpis) {
        if (pi.getP_version() > version) {
          version = pi.getP_version();
        }
      }
      if (values.get(0).getVerison() > version) {
        throw new FileOperationException("Invalid Version specified, provide " + values.get(0).getVerison() + " expected " + version, FOFailReason.INVALID_SPLIT_VALUES);
      } else {
        version = values.get(0).getVerison();
      }
      // remove non-max versions
      for (PartitionInfo pi : allpis) {
        if (pi.getP_version() == version) {
          pis.add(pi);
        }
      }
      int vlen = 0;
      for (PartitionInfo pi : pis) {
        switch (pi.getP_type()) {
        case none:
        case roundrobin:
        case list:
        case range:
          break;
        case interval:
          vlen += 2;
          break;
        case hash:
          vlen += 1;
          break;
        }
      }
      if (vlen != values.size()) {
        throw new FileOperationException("File split value should be " + vlen + " entries.", FOFailReason.INVALID_SPLIT_VALUES);
      }
      long low = -1, high = -1;
      for (int i = 0, j = 0; i < values.size(); i++) {
        SplitValue sv = values.get(i);
        PartitionInfo pi = pis.get(j);

        switch (pi.getP_type()) {
        case none:
        case roundrobin:
        case list:
        case range:
          throw new FileOperationException("Split type " + pi.getP_type() + " shouldn't be set values.", FOFailReason.INVALID_SPLIT_VALUES);
        case interval:
          if (low == -1) {
            try {
              low = Long.parseLong(sv.getValue());
            } catch (NumberFormatException e) {
              throw new FileOperationException("Split value expect Long for interval: " + sv.getValue(), FOFailReason.INVALID_SPLIT_VALUES);
            }
            break;
          }
          if (high == -1) {
            try {
              high = Long.parseLong(sv.getValue());
            } catch (NumberFormatException e) {
              throw new FileOperationException("Split value expect Long for interval: " + sv.getValue(), FOFailReason.INVALID_SPLIT_VALUES);
            }
            // check range
            String interval_unit = pi.getArgs().get(0);
            Double d = Double.parseDouble(pi.getArgs().get(1));
            Long interval_seconds = 0L;
            try {
              interval_seconds = PartitionFactory.getIntervalSeconds(interval_unit, d);
            } catch (Exception e) {
              throw new FileOperationException("Handle interval split: internal error.", FOFailReason.INVALID_SPLIT_VALUES);
            }
            if (high - low != interval_seconds) {
              throw new FileOperationException("Invalid interval range specified: [" + low + ", " + high +
                  "), expect range length: " + interval_seconds + ".", FOFailReason.INVALID_SPLIT_VALUES);
            }
            // unit check
            Long iu = 1L;
            try {
              iu = PartitionFactory.getIntervalUnit(interval_unit);
            } catch (Exception e) {
              throw new FileOperationException("Handle interval split unit: interval error.", FOFailReason.INVALID_SPLIT_VALUES);
            }
            if (low % iu != 0) {
              throw new FileOperationException("The low limit of interval split should be MODed by unit " +
                  interval_unit + "(" + iu + ").", FOFailReason.INVALID_SPLIT_VALUES);
            }
            j++;
            break;
          }
          break;
        case hash:
          low = high = -1;
          long v;
          try {
            // Format: "num-value"
            String[] hv = sv.getValue().split("-");
            if (hv == null || hv.length != 2) {
              throw new FileOperationException("Split value for hash except format: 'bucket_size-value' : " + sv.getValue(),
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            v = Long.parseLong(hv[0]);
            if (v != pi.getP_num()) {
              throw new FileOperationException("Split value of hash bucket_size mismatch: expect " + pi.getP_num() + " but provided " + v,
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            v = Long.parseLong(hv[1]);
          } catch (NumberFormatException e) {
            throw new FileOperationException("Split value expect Long for hash: " + sv.getValue(), FOFailReason.INVALID_SPLIT_VALUES);
          }
          if (v < 0 && v >= pi.getP_num()) {
            throw new FileOperationException("Hash value exceeds valid range: [0, " + pi.getP_num() + ").", FOFailReason.INVALID_SPLIT_VALUES);
          }
          break;
        }
        // check version, column name here
        if (sv.getVerison() != pi.getP_version() ||
            !pi.getP_col().equalsIgnoreCase(sv.getSplitKeyName())) {
          throw new FileOperationException("SplitKeyName mismatch, please check your metadata.", FOFailReason.INVALID_SPLIT_VALUES);
        }

      }
      break;
    case CREATE_AUX_IDX_FILE:
      // ignore db, table, and values check
      break;
    }

    // Step 2: do file creation or file gets now
    boolean do_create = true;
    SFile r = null;

    if (policy.getOperation() == CreateOperation.CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST) {
      // get files by value firstly
      List<SFile> gfs = rs.filterTableFiles(db_name, table_name, values);
      if (gfs != null && gfs.size() > 0) {
        // this means there are many files with this same split value, check if there exists INCREATE file
        for (SFile f : gfs) {
          if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
            // ok, we should return this INCREATE file
            r = f;
            do_create = false;
            break;
          }
        }
      }
    }
    if (do_create) {
      FileLocatingPolicy flp = null;

      // do not select the backup/shared device for the first entry
      switch (policy.getOperation()) {
      case CREATE_NEW_IN_NODEGROUPS:
        flp = new FileLocatingPolicy(ngnodes, dm.backupDevs, FileLocatingPolicy.SPECIFY_NODES, FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
        break;
      case CREATE_NEW:
      case CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST:
      case CREATE_NEW_RANDOM:
        if (ngs != null) {
          // use all available node group's nodes
          for (NodeGroup ng : ngs) {
            if (ng.getNodesSize() > 0) {
              for (Node n : ng.getNodes()) {
                ngnodes.add(n.getNode_name());
              }
            }
          }
          if (policy.getOperation() == CreateOperation.CREATE_NEW_RANDOM) {
            // TODO: Do we need random dev selection here?
            flp = new FileLocatingPolicy(ngnodes, dm.backupDevs, FileLocatingPolicy.RANDOM_NODES, FileLocatingPolicy.RANDOM_DEVS, false);
          } else {
            flp = new FileLocatingPolicy(ngnodes, dm.backupDevs, FileLocatingPolicy.SPECIFY_NODES, FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
          }
        }
        break;
      case CREATE_AUX_IDX_FILE:
        // use all available ndoes
        flp = new FileLocatingPolicy(null, dm.backupDevs, FileLocatingPolicy.EXCLUDE_NODES, FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
        break;
      default:
          throw new FileOperationException("Invalid create operation provided!", FOFailReason.INVALID_FILE);
      }

      if (policy.getOperation() == CreateOperation.CREATE_IF_NOT_EXIST_AND_GET_IF_EXIST) {
        synchronized (file_creation_lock) {
          // final check here
          List<SFile> gfs = rs.filterTableFiles(db_name, table_name, values);
          if (gfs != null && gfs.size() > 0) {
            for (SFile f : gfs) {
              if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                // oh, we should return now
                return f;
              }
            }
          }
          // ok, it means there is no INCREATE files, create one
          r = create_file(flp, null, repnr, db_name, table_name, values);
        }
      } else {
        r = create_file(flp, null, repnr, db_name, table_name, values);
      }
    }
    DMProfile.fcreate2SuccR.incrementAndGet();
    return r;
	}

	@Override
	public boolean create_role(Role arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create_table(Table arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_by_user(Table arg0, User arg1)
			throws AlreadyExistsException, InvalidObjectException,
			MetaException, NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void create_table_with_environment_context(Table arg0,
			EnvironmentContext arg1) throws AlreadyExistsException,
			InvalidObjectException, MetaException, NoSuchObjectException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean create_type(Type arg0) throws AlreadyExistsException,
			InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean create_user(User arg0) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean del_device(String arg0) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

  @Override
  public int del_node(String nodeName) throws MetaException, TException {
    return (!isNullOrEmpty(nodeName) && client.del_node(nodeName)) ? 1 : 0;
  }

  @Override
  public boolean deleteEquipRoom(EquipRoom er) throws MetaException,
      TException {
    return er != null && client.deleteEquipRoom(er);
  }

  @Override
  public boolean deleteGeoLocation(GeoLocation gl) throws MetaException,
      TException {
    return gl != null && client.deleteGeoLocation(gl);
  }


  @Override
  public boolean deleteNodeAssignment(String nodeName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr && client.deleteNodeAssignment(nodeName, dbName);
  }

  @Override
  public boolean deleteNodeGroup(NodeGroup nodeGroup) throws MetaException,
      TException {
    return nodeGroup != null && client.deleteNodeGroup(nodeGroup);
  }

  @Override
  public boolean deleteNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    return client.deleteNodeGroupAssignment(nodeGroup, dbName);
  }


  @Override
  public boolean deleteRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    return client.deleteRoleAssignment(roleName, dbName);
  }

  /**
   * Delete a schema with the assigned param
   *
   * @param arg0
   *          is schema name
   * @return if the param is {@code NULL} or Empty
   *         return false
   *         else return {@link IMetaStoreClient#deleteSchema(String)}
   * @author mzy
   */
  @Override
  public boolean deleteSchema(String arg0) throws MetaException, TException {
    return !isNullOrEmpty(arg0) && client.deleteSchema(arg0);
  }

  /**
   * Delete Table node dist
   *
   * @param arg0
   *          db name
   * @param arg1
   *          table name
   * @param arg2
   *          node group
   * @return if the param is valid and
   *         {@link IMetaStoreClient#deleteTableNodeDist(String, String, List)} is success then
   *         return true
   *         else return false;
   * @author mzy
   */
  @Override
  public boolean deleteTableNodeDist(String arg0, String arg1,
      List<String> arg2) throws MetaException, TException {
    final boolean expr = isNullOrEmpty(arg0) || isNullOrEmpty(arg1) || arg2 == null;
    return !expr && client.deleteTableNodeDist(arg0, arg1, arg2);
  }


  /**
   * Delete User from db by userName and dbName
   *
   * @param userName
   * @param dbName
   * @return if userName and dbName is not null or empty and
   *         {@link IMetaStoreClient#deleteUserAssignment(String, String)} is success then return
   *         true
   *         else return false;
   * @author mzy
   */
  @Override
  public boolean deleteUserAssignment(String userName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(userName) || isNullOrEmpty(dbName);
    return !expr && client.deleteUserAssignment(userName, dbName);
  }


  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
      String partName, String colName) throws NoSuchObjectException,
      MetaException, InvalidObjectException, InvalidInputException,
      TException {
    return client.deletePartitionColumnStatistics(dbName, tableName, partName, colName);
  }

  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName,
      String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException, TException {
    return client.deleteTableColumnStatistics(dbName, tableName, colName);
  }

	@Override
	public void drop_attribution(String arg0, boolean arg1, boolean arg2)
			throws NoSuchObjectException, InvalidOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		
	}

  @Override
  public void drop_database(String name, boolean ifDeleteData, boolean ifIgnoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException,
      MetaException, TException {
    client.dropDatabase(name, ifDeleteData, ifIgnoreUnknownDb);
  }

  @Override
  public boolean drop_index_by_name(String dbName, String tableName, String indexName,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.dropIndex(dbName, tableName, indexName, ifDeleteData);
  }

  @Override
  public boolean drop_partition(String dbName, String tableName, List<String> partValues,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return client.dropPartition(dbName, tableName, partValues, ifDeleteData);
  }

  @Override
  public boolean drop_partition_by_name(String dbName, String tableName,
      String partName, boolean ifDelData) throws NoSuchObjectException,
      MetaException, TException {
    return client.dropPartition(dbName, tableName, partName, ifDelData);
  }

  @Override
  public int drop_partition_files(Partition part, List<SFile> files)
      throws TException {
    return client.drop_partition_files(part, files);
  }

  @Override
  public boolean drop_partition_index(Index index, Partition part)
      throws MetaException, AlreadyExistsException, TException {
    return client.drop_partition_index(index, part);
  }

  @Override
  public boolean drop_partition_index_files(Index index, Partition part,
      List<SFile> file) throws MetaException, TException {
    return client.drop_partition_index_files(index, part, file);
  }


  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return client.drop_role(roleName);
  }

  @Override
  public int drop_subpartition_files(Subpartition subpart, List<SFile> files)
      throws TException {
    return client.drop_subpartition_files(subpart, files);
  }

  @Override
  public boolean drop_subpartition_index(Index index, Subpartition subpart)
      throws MetaException, AlreadyExistsException, TException {
    return client.drop_subpartition_index(index, subpart);
  }


  @Override
  public boolean drop_subpartition_index_files(Index index, Subpartition subpart,
      List<SFile> file) throws MetaException, TException {
    return client.drop_subpartition_index_files(index, subpart, file);
  }

  @Override
  public void drop_table(String dbName, String tableName, boolean ifDelData)
      throws NoSuchObjectException, MetaException, TException {
    client.dropTable(dbName,tableName,ifDelData,true);
  }

  @Override
  public boolean drop_type(String type) throws MetaException,
      NoSuchObjectException, TException {
        return rs.dropType(type);
  }

  @Override
  public boolean drop_user(String userName) throws NoSuchObjectException,
      MetaException, TException {
    return client.drop_user(userName);
  }
	@Override
	public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values) throws MetaException, TException {
		return rs.filterTableFiles(dbName, tabName, values);
	}

	@Override
  public List<Node> find_best_nodes(int nr) throws MetaException,
      TException {
    try {
      List<Node> list = Lists.newArrayList(dm.findBestNodes(nr));
      return list;
    } catch (IOException e) {
      return Lists.newArrayList();
    }
  }
	@Override
  public List<Node> find_best_nodes_in_groups(String arg0, String arg1,
      int arg2, FindNodePolicy arg3) throws MetaException, TException {
    //return Lists.newArrayList(d);
    //TODO to know how to find the best nodes in grops
    return Lists.newArrayList();
  }

	@Override
	public String getDMStatus() throws MetaException, TException {

		return dm.getDMStatus();
	}


  @Override
  public GeoLocation getGeoLocationByName(String geoLocName) throws MetaException,
      NoSuchObjectException, TException {
    return client.getGeoLocationByName(geoLocName);
  }

  @Override
  public List<GeoLocation> getGeoLocationByNames(List<String> geoLocNames)
      throws MetaException, TException {
    return Lists.newArrayList(client.getGeoLocationByNames(geoLocNames));
  }

  @Override
  public String getMP(String node_name, String devid) throws MetaException,
      TException {
    return dm.getMP(node_name, devid);
  }


	@Override
	public long getMaxFid() throws MetaException, TException {
		return rs.getCurrentFID();
	}

	@Override
	public String getNodeInfo() throws MetaException, TException {
		return dm.getNodeInfo();
	}

	@Override
	public GlobalSchema getSchemaByName(String schName)
			throws NoSuchObjectException, MetaException, TException {
			GlobalSchema gs = rs.getSchema(schName);
			return gs;
	}

	@Override
	public long getSessionId() throws MetaException, TException {
		//TODO find the way to the method
		return 0;
	}

	@Override
	public List<SFile> getTableNodeFiles(String dbName, String tabName, String nodeName)
			throws MetaException, TException {
		throw new MetaException("Not implemented yet!");
	}

	@Override
	public List<NodeGroup> getTableNodeGroups(String dbName, String tabName)
			throws MetaException, TException {
		Table tbl = get_table(dbName, tabName);
	    return tbl.getNodeGroups();
	}

	//把所有本地缓存的database返回
	@Override
	public List<Database> get_all_attributions() throws MetaException,
			TException {
		List<Database> dbs = new ArrayList<Database>();
		dbs.addAll(CacheStore.getDatabaseHm().values());
		return dbs;
	}
	@Override
  public List<BusiTypeColumn> get_all_busi_type_cols() throws MetaException,
      TException {
    return Lists.newArrayList(client.get_all_busi_type_cols());
  }

  @Override
  public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
      throws MetaException, TException {
    return Lists.newArrayList(client.get_all_busi_type_datacenters());
  }


	@Override
	public List<String> get_all_databases() throws MetaException, TException {
		List<String> dbNames = new ArrayList<String>();
		dbNames.addAll(CacheStore.getDatabaseHm().keySet());
		return dbNames;
	}

	@Override
	public List<Node> get_all_nodes() throws MetaException, TException {
		List<Node> ns = new ArrayList<Node>();
		ns.addAll(CacheStore.getNodeHm().values());
		return ns;
	}

	@Override
	public List<String> get_all_tables(String dbname) throws MetaException,
			TException {
		return rs.getAllTables(dbname);
	}

	@Override
	public Database get_attribution(String name) throws NoSuchObjectException,
			MetaException, TException {
		return get_database(name);
	}

	@Override
	public String get_config_value(String arg0, String arg1)
			throws ConfigValSecurityException, TException {
		// TODO to implement this method
		return null;
	}

	@Override
	public Database get_database(String dbName) throws NoSuchObjectException,
			MetaException, TException {
			Database db = rs.getDatabase(dbName);
			return db;
	}

	@Override
  public List<String> get_databases(String pattern) throws MetaException,
      TException {
     return Lists.newArrayList(client.getDatabases(pattern));
  }

  @Override
  public String get_delegation_token(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException {
    return client.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

	@Override
	public Device get_device(String devid) throws MetaException, NoSuchObjectException, TException {
		return rs.getDevice(devid);
	}

	@Override
	public List<FieldSchema> get_fields(String dbname, String tablename)
			throws MetaException, UnknownTableException, UnknownDBException, TException {
			Table t = rs.getTable(dbname, tablename);
			if(t == null)
				throw new UnknownTableException("Table not found by name:"+dbname+"."+tablename);
			return t.getSd().getCols();
	}

	@Override
	public SFile get_file_by_id(long fid) throws FileOperationException,
			MetaException, TException {
		// TODO Auto-generated method stub
		try {
			SFile f = rs.getSFile(fid);
			if(f == null)
				throw new FileOperationException("File not found by id:"+fid, FOFailReason.INVALID_FILE);
			return f;
		} catch (Exception e) {
			e.printStackTrace();
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public SFile get_file_by_name(String node, String devid, String location)
			throws FileOperationException, MetaException, TException {
		SFile f = rs.getSFile(devid, location);
		if(f == null)
			throw new FileOperationException("Can not find SFile by name: " + node + ":" + devid + ":" + location, FOFailReason.INVALID_FILE);
		return f;
	}

	@Override
	public Index get_index_by_name(String dbName, String tableName, String indexName)
			throws MetaException, NoSuchObjectException, TException {
			String key = dbName + "." + tableName + "." + indexName;
			Index ind = rs.getIndex(dbName, tableName, indexName);
			
			if(ind == null)
				throw new NoSuchObjectException("Index not found by name:"+key);
			return ind;
	}

	@Override
	public List<String> get_index_names(String dbName, String tblName, short arg2)
			throws MetaException, TException {
		List<String> indNames = new ArrayList<String>();
		for(String key : CacheStore.getIndexHm().keySet()){
			String[] keys = key.split("\\.");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				indNames.add(keys[2]);
			}
		}
		return indNames;
	}

	@Override
	public List<Index> get_indexes(String dbName, String tblName, short arg2)
			throws NoSuchObjectException, MetaException, TException {
		List<Index> inds = new ArrayList<Index>();
		for(String key : CacheStore.getIndexHm().keySet()){
			String[] keys = key.split("\\.");
			if(dbName.equalsIgnoreCase(keys[0]) && tblName.equalsIgnoreCase(keys[1])){
				inds.add(CacheStore.getIndexHm().get(key));
			}
		}
		return inds;
	}

	@Override
	//MetaStoreClient 初始化时会调这个rpc
	public Database get_local_attribution() throws MetaException, TException {
		// TODO Auto-generated method stub
		String dbname = conf.getLocalDbName();
		try {
			Database db = rs.getDatabase(dbname);
			return db;
		} catch (Exception e) {
			throw new MetaException(e.getMessage());
		}
		//如果返回null会抛异常
		//org.apache.thrift.TApplicationException: get_local_attribution failed: unknown result
	}

	@Override
	public List<String> get_lucene_index_names(String arg0, String arg1,
			short arg2) throws MetaException, TException {
		//TODO to implement this method
		return null;
	}

	@Override
	public Node get_node(String node_name) throws MetaException, TException {
			Node n = rs.getNode(node_name);
			return n;
	}


  @Override
  public Partition get_partition(final String dbName, final String tableName,
      final List<String> partVals)
      throws MetaException, NoSuchObjectException, TException {
    return client.getPartition(dbName,tableName,partVals);
  }

  @Override
  public Partition get_partition_by_name(String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException, TException {
        return client.getPartition(dbName, tableName, partName);
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String dbName,
      String tableName, String partitionName, String colName)
      throws NoSuchObjectException, MetaException, InvalidInputException,
      InvalidObjectException, TException {
    return client.getPartitionColumnStatistics(dbName, tableName, partitionName, colName);
  }

  @Override
  public List<SFileRef> get_partition_index_files(Index index, Partition part)
      throws MetaException, TException {
    return client.get_partition_index_files(index, part);
  }

  @Override
  public List<String> get_partition_names(String dbName, String tableName, short maxPart)
      throws MetaException, TException {
    return client.get_partition_names(dbName, tableName, maxPart);
  }

  @Override
  public List<String> get_partition_names_ps(String arg0, String arg1,
      List<String> arg2, short arg3) throws MetaException,
      NoSuchObjectException, TException {
    //TODO implement this method
    return Lists.newArrayList();
  }

  @Override
  public Partition get_partition_with_auth(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, TException {
    return client.getPartitionWithAuthInfo(dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public List<Partition> get_partitions(String dbName, String tableName, short maxParts)
      throws NoSuchObjectException, MetaException, TException {
    List<String> list = client.get_partition_names(dbName, tableName,maxParts);
    List<Partition> plist = Lists.newArrayList();
    for(String str:list){
      plist.add(client.getPartition(dbName, tableName, str));
    }
    return plist;
  }

  @Override
  public List<Partition> get_partitions_by_filter(String arg0, String arg1,
      String arg2, short arg3) throws MetaException,
      NoSuchObjectException, TException {
    //TODO to implement this method
    return Lists.newArrayList();
  }

  @Override
  public List<Partition> get_partitions_by_names(String tblName, String dbName,
      List<String> partVals) throws MetaException, NoSuchObjectException,
      TException {
    return Lists.newArrayList(client.getPartition(tblName, dbName, partVals));
  }

	@Override
	public List<Partition> get_partitions_ps(String arg0, String arg1,
			List<String> arg2, short arg3) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_ps_with_auth(String arg0,
			String arg1, List<String> arg2, short arg3, String arg4,
			List<String> arg5) throws NoSuchObjectException, MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> get_partitions_with_auth(String arg0, String arg1,
			short arg2, String arg3, List<String> arg4)
			throws NoSuchObjectException, MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef arg0,
			String arg1, List<String> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_role_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	//模仿HiveMetaStore中的方法写的
	public List<FieldSchema> get_schema(String dbname, String tablename)
			throws MetaException, UnknownTableException, UnknownDBException, TException {
		try{
			String baseTableName = tablename.split("\\.")[0];
//			Table baseTable = (Table) ms.readObject(ObjectType.TABLE, dbname+"."+baseTableName);
			Table baseTable = rs.getTable(dbname, baseTableName);
			if(baseTable == null)
				throw new UnknownTableException("Table not found by name:"+baseTableName);
			List<FieldSchema> fss = baseTable.getSd().getCols();
			if(baseTable.getPartitionKeys() != null)
				fss.addAll(baseTable.getPartitionKeys());
			
			return fss;
		}catch(Exception e){
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public List<SFileRef> get_subpartition_index_files(Index arg0,
			Subpartition arg1) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Subpartition> get_subpartitions(String arg0, String arg1,
			Partition arg2) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table get_table(String dbname, String tablename) throws MetaException,
			NoSuchObjectException, TException {
			Table t = rs.getTable(dbname, tablename);
			return t;
	}

	@Override
	public ColumnStatistics get_table_column_statistics(String arg0,
			String arg1, String arg2) throws NoSuchObjectException,
			MetaException, InvalidInputException, InvalidObjectException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables) 
			throws MetaException, InvalidOperationException, UnknownDBException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Table> get_table_objects_by_name(String dbname, List<String> names)
			throws MetaException, InvalidOperationException, UnknownDBException,
			TException {
		if (dbname == null || dbname.isEmpty()) {
			throw new UnknownDBException("DB name is null or empty");
		}
		if (names == null) {
			throw new InvalidOperationException("table names are null");
		}
		return rs.getTableObjectsByName(dbname, names);
	}

	@Override
	public List<String> get_tables(String arg0, String arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Type get_type(String arg0) throws MetaException,
			NoSuchObjectException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Type> get_type_all(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean grant_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean grant_role(String arg0, String arg1, PrincipalType arg2,
			String arg3, PrincipalType arg4, boolean arg5)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isPartitionMarkedForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listDBNodeGroups(String dbName) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<EquipRoom> listEquipRoom() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Long> listFilesByDigest(String digest) throws MetaException,
			TException {
		
		return rs.findSpecificDigestFiles(digest);
	}

	@Override
	public List<GeoLocation> listGeoLocation() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<NodeGroup> listNodeGroupByNames(List<String> ngNames)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		
		return rs.listNodeGroupByNames(ngNames);
	}

	@Override
	public List<NodeGroup> listNodeGroups() throws MetaException, TException {
		List<NodeGroup> ngs = new ArrayList<NodeGroup>();
		ngs.addAll(CacheStore.getNodeGroupHm().values());
		return ngs;
	}

	@Override
	public List<Node> listNodes() throws MetaException, TException {
		List<Node> ng = new ArrayList<Node>();
		ng.addAll(CacheStore.getNodeHm().values());
		return ng;
	}

	@Override
	public List<Role> listRoles() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GlobalSchema> listSchemas() throws MetaException, TException {
		List<GlobalSchema> gss = new ArrayList<GlobalSchema>(); 
		gss.addAll(CacheStore.getGlobalSchemaHm().values());
		return gss;
	}

	/*
	void setRange(long fromIncl, long toExcl)

    Set the range of results to return. The execution of the query is modified to return only a 
    subset of results. If the filter would normally return 100 instances, and fromIncl is set to 50, 
    and toExcl is set to 70, then the first 50 results that would have been returned are skipped, 
    the next 20 results are returned and the remaining 30 results are ignored. An implementation should 
    execute the query such that the range algorithm is done at the data store.

    Parameters:
        fromIncl - 0-based inclusive start index
        toExcl - 0-based exclusive end index, or Long.MAX_VALUE for no limit.
    Since:
        2.0
	
	返回的结果集中包含from，不包含to，一共返回from-to个元素
	而zrange的两边是inclusive
	 */
	@Override
	public List<Long> listTableFiles(String dbName, String tabName, int from, int to) 
			throws MetaException, TException {
		
		return rs.listTableFiles(dbName, tabName, from, to-1);
	}

	@Override
	public List<NodeGroup> listTableNodeDists(String dbName, String tabName)
			throws MetaException, TException {
		Table t = get_table(dbName, tabName);
		if(t == null)
			throw new MetaException("No table found by dbname:"+dbName+", tableName:"+tabName);
		return t.getNodeGroups();
	}

	@Override
	public List<User> listUsers() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Device> list_device() throws MetaException, TException {
		return rs.listDevice();
	}

	@Override
	public List<HiveObjectPrivilege> list_privileges(String arg0,
			PrincipalType arg1, HiveObjectRef arg2) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Role> list_roles(String arg0, PrincipalType arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users(Database arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> list_users_names() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void markPartitionForEvent(String arg0, String arg1,
			Map<String, String> arg2, PartitionEventType arg3)
			throws MetaException, NoSuchObjectException, UnknownDBException,
			UnknownTableException, UnknownPartitionException,
			InvalidPartitionException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean migrate2_in(Table arg0, List<Partition> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate2_stage1(String arg0, String arg1,
			List<String> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate2_stage2(String arg0, String arg1, List<String> arg2,
			String arg3, String arg4, String arg5) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean migrate_in(Table arg0, Map<Long, SFile> arg1,
			List<Index> arg2, String arg3, String arg4,
			Map<Long, SFileLocation> arg5) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileLocation> migrate_stage1(String arg0, String arg1,
			List<Long> arg2, String arg3) throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean migrate_stage2(String arg0, String arg1, List<Long> arg2,
			String arg3, String arg4, String arg5, String arg6, String arg7)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyEquipRoom(EquipRoom arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyGeoLocation(GeoLocation arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyNodeGroup(String arg0, NodeGroup arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifySchema(String arg0, GlobalSchema arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Device modify_device(Device arg0, Node arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean modify_user(User arg0) throws NoSuchObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean offline_filelocation(SFileLocation arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean online_filelocation(SFile arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Map<String, String> partition_name_to_spec(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> partition_name_to_vals(String arg0)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String pingPong(String str) throws MetaException, TException {
		return str;
	}

	@Override
	public void rename_partition(String arg0, String arg1, List<String> arg2,
			Partition arg3) throws InvalidOperationException, MetaException,
			TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long renew_delegation_token(String arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean reopen_file(long fid) throws FileOperationException, MetaException, TException {
		DMProfile.freopenR.incrementAndGet();
		startFunction("reopen_file ", "fid: " + fid);
		
    SFile saved = rs.getSFile(fid);
    boolean success = false;

    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + fid, FOFailReason.INVALID_FILE);
    }
    // check if this file is in REPLICATED state, otherwise, complain about that.
    switch (saved.getStore_status()) {
    case MetaStoreConst.MFileStoreStatus.INCREATE:
      throw new FileOperationException("SFile " + fid + " has already been in INCREATE state.", FOFailReason.INVALID_STATE);
    case MetaStoreConst.MFileStoreStatus.CLOSED:
      throw new FileOperationException("SFile " + fid + " is in CLOSE state, please wait.", FOFailReason.INVALID_STATE);
    case MetaStoreConst.MFileStoreStatus.REPLICATED:
      // FIXME: seq reopenSFiles
      synchronized (file_reopen_lock) {
        success = rs.reopenSFile(saved);
      }
      break;
    case MetaStoreConst.MFileStoreStatus.RM_LOGICAL:
    case MetaStoreConst.MFileStoreStatus.RM_PHYSICAL:
      throw new FileOperationException("SFile " + fid + " is in RM-* state, reject all reopens.", FOFailReason.INVALID_STATE);
    }
    return success;
	}

	@Override
	public int restore_file(SFile file) throws FileOperationException, MetaException, TException {
		DMProfile.frestoreR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID" + file.getFid(), FOFailReason.INVALID_FILE);
    }

    if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_LOGICAL) {
      throw new FileOperationException("File StoreStatus is not in RM_LOGICAL.", FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
    rs.updateSFile(saved);
    return 0;
	}

	@Override
	public boolean revoke_privileges(PrivilegeBag arg0) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean revoke_role(String arg0, String arg1, PrincipalType arg2)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public int rm_file_logical(SFile file) throws FileOperationException,	MetaException, TException {
		DMProfile.frmlR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID" + file.getFid(), FOFailReason.INVALID_FILE);
    }

    // only in REPLICATED state can step into RM_LOGICAL
    if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.REPLICATED) {
      throw new FileOperationException("File StoreStatus is not in REPLICATED.", FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.RM_LOGICAL);
    rs.updateSFile(saved);
    return 0;
	}

	@Override
	public int rm_file_physical(SFile file) throws FileOperationException,
			MetaException, TException {
		DMProfile.frmpR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + file.getFid(), FOFailReason.INVALID_FILE);
    }

    if (!(saved.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_LOGICAL)) {
      throw new FileOperationException("File StoreStatus is not in INCREATE/REPLICATED/RM_LOGICAL.", FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.RM_PHYSICAL);
    file = rs.updateSFile(saved);
    file.setLocations(rs.getSFileLocations(file.getFid()));
    synchronized (dm.cleanQ) {
      dm.cleanQ.add(new DMRequest(file, DMRequest.DMROperation.RM_PHYSICAL, 0));
      dm.cleanQ.notify();
    }
    return 0;
	}

	@Override
	public void set_file_repnr(long fid, int repnr)	throws FileOperationException, TException {
//		startFunction("set_file_repnr", "fid " + fid + " repnr " + repnr);
    SFile f = get_file_by_id(fid);
    if (f != null) {
      f.setRep_nr(repnr);
      // FIXME: Caution, this might be a conflict code section for concurrent sfile field modification.
      rs.updateSFile(f);
    }		
	}

	@Override
	public boolean set_loadstatus_bad(long fid) throws MetaException, TException {
		SFile saved = rs.getSFile(fid);
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + fid, FOFailReason.INVALID_FILE);
    }

    saved.setLoad_status(MetaStoreConst.MFileLoadStatus.BAD);
    rs.updateSFile(saved);
    return true;
	}

	@Override
	public List<String> set_ugi(String arg0, List<String> arg1)
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Busitype> showBusitypes() throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public statfs statFileSystem(long arg0, long arg1) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean toggle_safemode() throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void truncTableFiles(String dbName, String tabName) throws MetaException, TException {
//		startFunction("truncTableFiles", "DB: " + dbName + " Table: " + tabName);
//    try {
      rs.truncTableFiles(dbName, tabName);
//    } finally {
//      endFunction("truncTableFiles", true, null);
//    }		
	}

	@Override
	public void update_attribution(Database arg0) throws NoSuchObjectException,
			InvalidOperationException, MetaException, TException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean update_partition_column_statistics(ColumnStatistics arg0)
			throws NoSuchObjectException, InvalidObjectException,
			MetaException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean update_table_column_statistics(ColumnStatistics arg0)
			throws NoSuchObjectException, InvalidObjectException,
			MetaException, InvalidInputException, TException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean user_authority_check(User arg0, Table arg1,
			List<MSOperation> arg2) throws MetaException, TException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public boolean del_filelocation(SFileLocation slf) throws MetaException,
			TException {
		// TODO Auto-generated method stub
		return false;
	}
	@Override
	public List<SFile> get_files_by_ids(List<Long> fids)
			throws FileOperationException, MetaException, TException {
		List<SFile> fl = new ArrayList<SFile>();
		for(Long fid : fids)
		{
			SFile sf = this.get_file_by_id(fid);
			fl.add(sf);
		}
		return fl;
	}
}
	
	 