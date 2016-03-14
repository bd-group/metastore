package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DiskManager.DMProfile;
import org.apache.hadoop.hive.metastore.DiskManager.DeviceInfo;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BusiTypeColumn;
import org.apache.hadoop.hive.metastore.api.BusiTypeDatacenter;
import org.apache.hadoop.hive.metastore.api.Busitype;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.EquipRoom;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GeoLocation;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
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
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.hadoop.hive.metastore.model.MUser;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;
import org.apache.thrift.TException;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class RawStoreImp implements RawStore {

	private static final Log LOG = LogFactory.getLog(RawStoreImp.class);
	private static final Long g_fid_syncer = new Long(0);
  private static long g_fid = 0;

	private final CacheStore cs;

	public RawStoreImp() throws IOException {
		cs = new CacheStore();
	}

  public CacheStore getCs() {
		return cs;
	}

	private long getNextFID() {
    synchronized (g_fid_syncer) {
      return g_fid++;
    }
  }
	public static void setFID(long fid){
		g_fid = fid;
	}
	public static long getFid(){
		return g_fid;
	}

	@Override
	public Configuration getConf() {
		return null;
	}

	@Override
	public void setConf(Configuration arg0) {
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean openTransaction() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean commitTransaction() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void rollbackTransaction() {
		// TODO Auto-generated method stub

	}

	@Override
	public void createDatabase(Database db) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public Database getDatabase(String name) throws NoSuchObjectException {
		try {
			Database d = (Database) cs.readObject(ObjectType.DATABASE, name);
			if(d == null) {
        throw new NoSuchObjectException("There is no database named "+name);
      }
			return d;
			//到底是抛出去，还是自己捕获呢。。
		} catch (JedisConnectionException e) {
			LOG.error(e,e);
		} catch (IOException e) {
			LOG.error(e,e);
		} catch (ClassNotFoundException e) {
			LOG.error(e,e);
		}
		return null;
	}

	@Override
	public boolean dropDatabase(String dbname) throws NoSuchObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean alterDatabase(String dbname, Database db)
			throws NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> getDatabases(String pattern) throws MetaException {
		try {
      return cs.getDatabases(pattern);
    } catch (JedisException e) {
      throw new MetaException(e.getMessage());
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
	}

	@Override
	public List<String> getAllDatabases() throws MetaException {
		List<String> dbs = new ArrayList<String>();
		dbs.addAll(CacheStore.getDatabaseHm().keySet());
		return dbs;
	}

	@Override
	public boolean createType(Type type) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Type getType(String typeName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean dropType(String typeName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public DDLMsg createTable(Table tbl) throws InvalidObjectException,
			MetaException {
	  return null;
	}

	@Override
	public void createOrUpdateDevice(DeviceInfo di, Node node, NodeGroup ng)
			throws InvalidObjectException, MetaException {
	  try {
	    boolean doCreate = false;
	    Device de = (Device) cs.readObject(ObjectType.DEVICE, di.dev);
	    if (de == null) {
	      Node n = getNode(node.getNode_name());
	      if (n == null) {
	        throw new InvalidObjectException("Invalid Node name '" + node.getNode_name() + "'!");
	      }

	      String ng_name = null;
	      if (ng != null) {
          ng_name = ng.getNode_group_name();
        }
	      if (di.mp == null) {
	        de = new Device(di.dev, di.prop, node.getNode_name(), MetaStoreConst.MDeviceStatus.SUSPECT, ng_name);
	      } else {
	        de = new Device(di.dev, di.prop, node.getNode_name(), MetaStoreConst.MDeviceStatus.ONLINE, ng_name);
	      }
	      doCreate = true;
	    } else {
	      if (di.mp != null && de.getStatus() == MetaStoreConst.MDeviceStatus.SUSPECT) {
	        de.setStatus(MetaStoreConst.MDeviceStatus.ONLINE);
	      }
	      if (!de.getNode_name().equals(node.getNode_name()) &&
            DeviceInfo.getType(de.getProp()) == MetaStoreConst.MDeviceProp.GENERAL){
	        Node n = getNode(node.getNode_name());
	        if (n == null) {
	          throw new InvalidObjectException("Invalid Node name '" + node.getNode_name() + "'!");
	        }
	        de.setNode_name(node.getNode_name());
	        doCreate = true;
	      }
	    }
	    if (doCreate){
	      createDevice(de);
	    }
	  } catch (InvalidObjectException e) {
	    throw e;
	  } catch (Exception e) {
	     LOG.error(e,e);
	     throw new MetaException(e.getMessage());
    }
	}

	public void createDevice(Device de) throws JedisException, IOException
	{
		cs.writeObject(ObjectType.DEVICE, de.getDevid(), de, false);
		HashMap<String, Object> old_params = new HashMap<String, Object>();
		old_params.put("devid", de.getDevid());
		old_params.put("node_name", de.getNode_name());
		MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_CREATE_DEVICE, -1l, -1l, null, de, old_params));
	}

	@Override
	public Device modifyDevice(Device dev, Node node) throws MetaException,
			NoSuchObjectException, InvalidObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void offlineDevice(String devid) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createNode(Node node) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean updateNode(Node node) throws MetaException {
		boolean changed = false;
	  try {
      Node n = (Node) cs.readObject(ObjectType.NODE,node.getNode_name());
      if(n == null){
        return false;
      }else{
      	if (n.getStatus() != node.getStatus()) {
          changed = true;
        }
        n.setStatus(node.getStatus());
        n.setIps(node.getIps());
        cs.writeObject(ObjectType.NODE, n.getNode_name(), n, false);

        //不抛异常就说明更新操作成功了，可以发消息了
        if (changed) {
          HashMap<String, Object> old_params = new HashMap<String, Object>();
          long event = 0;

          old_params.put("node_name", node.getNode_name());
          old_params.put("status", node.getStatus());
          if (node.getStatus() == MetaStoreConst.MNodeStatus.SUSPECT ||
              node.getStatus() == MetaStoreConst.MNodeStatus.OFFLINE) {
            event = MSGType.MSG_FAIL_NODE;
          } else if (node.getStatus() == MetaStoreConst.MNodeStatus.ONLINE) {
            event = MSGType.MSG_BACK_NODE;
          }
          if (event != 0) {
            MsgServer.addMsg(MsgServer.generateDDLMsg(event, -1l, -1l, null, n, old_params));
          }
        }
        return true;
      }
    } catch (Exception e) {
      LOG.error(e,e);
      throw new MetaException(e.getMessage());
    }


	}

	@Override
	public boolean delNode(String node_name) throws MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Node getNode(String node_name) throws MetaException {
		try {
			Node n = (Node) cs.readObject(ObjectType.NODE, node_name);
			return n;
		} catch (JedisConnectionException e) {
			LOG.error(e,e);
		} catch (IOException e) {
			LOG.error(e,e);
		} catch (ClassNotFoundException e) {
			LOG.error(e,e);
		}
		return null;
	}

	@Override
	public List<Node> getAllNodes() throws MetaException {
		List<Node> ns = new ArrayList<Node>();
		ns.addAll(CacheStore.getNodeHm().values());
		return ns;
	}

	@Override
	public long countNode() throws MetaException {

		return CacheStore.getNodeHm().size();
	}

	@Override
	public SFile createFile(SFile file) throws InvalidObjectException,MetaException {
		do {
      file.setFid(getNextFID());
      // query on this fid to check if it is a valid fid
      SFile oldf = this.getSFile(file.getFid());
      if (oldf != null) {
        continue;
      }
      break;
    } while (true);
		try {
			cs.writeObject(ObjectType.SFILE, file.getFid()+"", file, true);

			HashMap<String, Object> old_params = new HashMap<String, Object>();
			old_params.put("f_id", file.getFid());
			old_params.put("db_name", file.getDbName());
			old_params.put("table_name", file.getTableName());
			MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_CREATE_FILE, -1l, -1l, null, file, old_params));
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
		return file == null ? null : file.deepCopy();
	}

	/**
	 * might return null
	 */
	@Override
	public SFile getSFile(long fid) throws MetaException {
		try {
			Object o = cs.readObject(ObjectType.SFILE, fid + "");
			if (o == null) {
        return null;
      }
			return (SFile)o;
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	/**
	 * might return null
	 */
	@Override
	public SFile getSFile(String devid, String location) throws MetaException {
		try {
			SFileLocation sfl = getSFileLocation(devid, location);
			if (sfl != null) {
        return getSFile(sfl.getFid());
			} else {
        return null;
      }
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public boolean delSFile(long fid) throws MetaException {
	  boolean success = false;
	  try {
	    SFile sf = getSFile(fid);
	    if (sf == null) {
	      return true;
	    }
	    cs.removeObject(ObjectType.SFILE, fid+"");
	    success = true;
	    HashMap<String, Object> old_params = new HashMap<String, Object>();
	    old_params.put("f_id", sf.getFid());
	    old_params.put("db_name", sf.getDbName());
	    old_params.put("table_name", sf.getTableName() );
	    MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_DEL_FILE, -1l, -1l, null, sf, old_params));
	  } catch(Exception e) {
	    LOG.error(e,e);
	    return false;
	  } finally {
	    if (success) {
	      DMProfile.fdelR.incrementAndGet();
	    }
	  }
	  return success;
	}

	@Override
	public SFile updateSFile(SFile newfile) throws MetaException {
		return updateSFile(newfile, false);
	}

	//为了不改变原有的updatesfile的语义，添加一个是否连sfl一起更新的方法
	public SFile updateSFile(SFile newfile, boolean isWithSfl) throws MetaException {
		SFile sf = this.getSFile(newfile.getFid());
		boolean repnr_changed = false;
	  boolean stat_changed = false;

	  if (sf == null) {
	    throw new MetaException("Invalid SFile object provided!");
	  }
	  if (sf.getRep_nr() != newfile.getRep_nr()) {
	    repnr_changed = true;
	  }
	  if (sf.getStore_status() != newfile.getStore_status()) {
	    stat_changed = true;
	  }

		try {
			// update index here
		  if ((sf.getDigest() == null) ||
		      (!sf.getDigest().equals(newfile.getDigest()))) {
		    // BUG-XXX: if sf.getDigest() is null, we can't call function equal!
        cs.removeLfbdValue(sf.getDigest(), sf.getFid() + "");
      }
			if (stat_changed) {
        cs.removeSfileStatValue(sf.getStore_status(), sf.getFid() + "");
      }
			sf.setRep_nr(newfile.getRep_nr());
      sf.setDigest(newfile.getDigest());
      sf.setRecord_nr(newfile.getRecord_nr());
      sf.setAll_record_nr(newfile.getAll_record_nr());
      sf.setStore_status(newfile.getStore_status());
      sf.setLoad_status(newfile.getLoad_status());
      sf.setLength(newfile.getLength());
      sf.setRef_files(newfile.getRef_files());

      cs.writeObject(ObjectType.SFILE, sf.getFid()+"", sf, true);

      if (stat_changed) {
        // send the SFile state change message
        HashMap<String, Object> old_params = new HashMap<String, Object>();
        old_params.put("f_id", newfile.getFid());
        old_params.put("new_status", newfile.getStore_status());
        old_params.put("db_name", newfile.getDbName());
        old_params.put("table_name", newfile.getTableName());
        MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_STA_FILE_CHANGE, -1l, -1l, null, sf, old_params));
        if (newfile.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
          DMProfile.freplicateR.incrementAndGet();
        }
      }
      if (repnr_changed) {
        // send the SFile state change message
        HashMap<String, Object> old_params = new HashMap<String, Object>();
        old_params.put("f_id", newfile.getFid());
        old_params.put("new_repnr", newfile.getRep_nr());
        old_params.put("db_name", newfile.getDbName());
        old_params.put("table_name", newfile.getTableName());
        MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_FILE_USER_SET_REP_CHANGE, -1l, -1l, null, sf, old_params));
      }

      return sf == null ? null : sf.deepCopy();
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public boolean createFileLocation(SFileLocation location)	throws InvalidObjectException, MetaException {
		try {
			SFileLocation old = getSFileLocation(location.getDevid(), location.getLocation());
	    if (old != null) {
	      return false;
	    }
			SFile sf = (SFile) cs.readObject(ObjectType.SFILE, location.getFid() + "");
			if (sf == null) {
        throw new MetaException("No SFile found by id: " + location.getFid());
      }
			sf.addToLocations(location);
			cs.writeObject(ObjectType.SFILE, sf.getFid()+"", sf, false);

			HashMap<String, Object> old_params = new HashMap<String, Object>();
      old_params.put("f_id", new Long(location.getFid()));
      old_params.put("devid", location.getDevid());
      old_params.put("location", location.getLocation());
      old_params.put("op", "add");
      old_params.put("db_name", sf.getDbName());
      old_params.put("table_name", sf.getTableName());
      MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_REP_FILE_CHANGE, -1l, -1l, null, location, old_params));
      DMProfile.sflcreateR.incrementAndGet();
      return true;
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public List<SFileLocation> getSFileLocations(long fid) throws MetaException {
		SFile f = getSFile(fid);
		if(f != null) {
      return f.getLocations();
    }
		return null;
	}

	@Override
	public List<SFileLocation> getSFileLocations(int status) throws MetaException {
		try {
			List<SFileLocation> sfll = cs.getSFileLocations(status);
			return sfll;
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public List<SFileLocation> getSFileLocations(String devid, long curts,
			long timeout) throws MetaException {
		try {
			return cs.getSFileLocations(devid, curts, timeout);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public SFileLocation getSFileLocation(String devid, String location)
			throws MetaException {
		String sflkey = SFileImage.generateSflkey(location, devid);
		try {
			SFileLocation sfl = (SFileLocation) cs.readObject(ObjectType.SFILELOCATION, sflkey);
			return sfl;
		}catch(Exception e){
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public SFileLocation updateSFileLocation(SFileLocation newsfl) throws MetaException {
		boolean changed = false;
		try {
			SFileLocation sfl = (SFileLocation) cs.readObject(ObjectType.SFILELOCATION,
			    SFileImage.generateSflkey(newsfl.getLocation(), newsfl.getDevid()));
			if (sfl == null) {
        throw new MetaException("Invalid SFileLocation provided");
      }

			sfl.setUpdate_time(System.currentTimeMillis());
			if (sfl.getVisit_status() != newsfl.getVisit_status()) {
        changed = true;
        // update index
        cs.removeSflStatValue(sfl.getVisit_status(),
            SFileImage.generateSflkey(sfl.getLocation(), sfl.getDevid()));
        sfl.setVisit_status(newsfl.getVisit_status());
      }

			sfl.setRep_id(newsfl.getRep_id());
			sfl.setDigest(newsfl.getDigest());

			// 防止缓存中的sfile的location与更新之后的sfl不一致。。。。
			// FIXME 这样手动维护sfile与sfilelocation的关系很麻烦，很容易出错。。。
			SFile sf = (SFile) cs.readObject(ObjectType.SFILE, sfl.getFid() + "");
			if (sf.getLocations() != null) {
			  for (int i = 0; i < sf.getLocationsSize(); i++) {
			    if (sfl.getDevid().equals(sf.getLocations().get(i).getDevid()) &&
			        sfl.getLocation().equals(sf.getLocations().get(i).getLocation())) {
			      sf.getLocations().remove(i);
			      break;
			    }
			  }
			}
			sf.addToLocations(sfl);
			//写入了sfile，也就把sfilelocation写入了
			cs.writeObject(ObjectType.SFILE, sf.getFid() + "", sf, false);

			if (changed) {
	      switch (newsfl.getVisit_status()) {
	      case MetaStoreConst.MFileLocationVisitStatus.ONLINE:
	        DMProfile.sflonlineR.incrementAndGet();
	        break;
	      case MetaStoreConst.MFileLocationVisitStatus.OFFLINE:
	        DMProfile.sflofflineR.incrementAndGet();
	        break;
	      case MetaStoreConst.MFileLocationVisitStatus.SUSPECT:
	        DMProfile.sflsuspectR.incrementAndGet();
	        break;
	      }
	      // send the SFL state change message
	      HashMap<String, Object> old_params = new HashMap<String, Object>();
	      old_params.put("f_id", newsfl.getFid());
	      old_params.put("new_status", newsfl.getVisit_status());
	      old_params.put("devid", newsfl.getDevid());
        old_params.put("location", newsfl.getLocation());
	      old_params.put("db_name", sf.getDbName());
	      old_params.put("table_name", sf.getTableName());
	      MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_REP_FILE_ONOFF, -1l, -1l, null, sfl, old_params));
	    }

			return sfl == null ? null : sfl.deepCopy();
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public boolean delSFileLocation(String devid, String location) throws MetaException {
	  boolean success = false;
		String sflkey = SFileImage.generateSflkey(location, devid);
		try {
			SFileLocation sfl = (SFileLocation) cs.readObject(ObjectType.SFILELOCATION, sflkey);
			if (sfl == null) {
        return true;
      }
			SFile sf = (SFile) cs.readObject(ObjectType.SFILE, sfl.getFid() + "");
			if (sf == null) {
        throw new MetaException("No sfile found by id: " + sfl.getFid());
      }
			if (sf.getLocations() != null) {
			  for (int i = 0; i < sf.getLocationsSize(); i++) {
			    if (sfl.getDevid().equals(sf.getLocations().get(i).getDevid()) &&
			        sfl.getLocation().equals(sf.getLocations().get(i).getLocation())) {
			      sf.getLocations().remove(i);
			      break;
			    }
			  }
			}
			cs.writeObject(ObjectType.SFILE, sf.getFid() + "", sf, false);
			cs.removeObject(ObjectType.SFILELOCATION, sflkey);
			success = true;

			HashMap<String, Object> old_params = new HashMap<String, Object>();
			old_params.put("f_id", sfl.getFid());
			old_params.put("devid", devid);
			old_params.put("location", location);
			old_params.put("db_name", sf.getDbName());
			old_params.put("table_name", sf.getTableName());
			old_params.put("op", "del");
			MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_REP_FILE_CHANGE, -1l, -1l, null, sfl, old_params));

		} catch(Exception e){
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		} finally {
      if (success) {
        DMProfile.sfldelR.incrementAndGet();
      }
    }
		return success;
	}

	@Override
	public boolean dropTable(String dbName, String tableName)
			throws MetaException, NoSuchObjectException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Table getTable(String dbName, String tableName) throws MetaException {
		try {
			Table t = (Table) cs.readObject(ObjectType.TABLE, dbName+"."+tableName);
			return t;
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public Table getTableByID(long id) throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getTableOID(String dbName, String tableName)
			throws MetaException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean addPartition(Partition part) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Partition getPartition(String dbName, String tableName,
			String partName) throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Subpartition getSubpartition(String dbName, String tableName,
			String partName) throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition getPartition(String db_name, String tbl_name,
			List<String> part_vals) throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updatePartition(Partition newPart)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public void updateSubpartition(Subpartition newPart)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean dropPartition(String dbName, String tableName,
			List<String> part_vals) throws MetaException,
			NoSuchObjectException, InvalidObjectException,
			InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dropPartition(String dbName, String tableName,
			String part_name) throws MetaException, NoSuchObjectException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Partition> getPartitions(String dbName, String tableName,
			int max) throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void alterTable(String dbname, String name, Table newTable)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<String> getTables(String dbName, String pattern)
			throws MetaException {
		Iterator<String> iter = CacheStore.getTableHm().keySet().iterator();
		List<String> tn = new LinkedList<String>();
		while (iter.hasNext()) {
			String key = iter.next();
			if (key.startsWith(dbName)) {
				String tabname = key.split("\\.")[1];
				if (tabname.matches(pattern)) {
          tn.add(tabname);
        }
			}
		}
		return tn;
	}

	@Override
	public List<Table> getTableObjectsByName(String dbname,
			List<String> tableNames) throws MetaException, UnknownDBException {
		if(!CacheStore.getDatabaseHm().containsKey(dbname)) {
      throw new UnknownDBException("Can not find database: "+dbname);
    }
		Set<String> noDupNames = new HashSet<String>();
  	noDupNames.addAll(tableNames);
		List<Table> ts = new ArrayList<Table>();
		for(String tblname : noDupNames)
		{
			try {
				Table t = (Table) cs.readObject(ObjectType.TABLE, dbname+"."+tblname);
				if(t != null) {
          ts.add(t);
        }
			} catch (Exception e) {
				LOG.error(e,e);
//				throw new MetaException(e.getMessage());
			}
		}
		return ts;
	}

	@Override
	public List<String> getAllTables(String dbName) throws MetaException {
		List<String> ts = new ArrayList<String>();
		for (String s : CacheStore.getTableHm().keySet()) {
			if (s.startsWith(dbName + ".")) {
        ts.add(s.split("\\.")[1]);
      }
		}
		return ts;
	}

	@Override
	public List<String> listTableNamesByFilter(String dbName, String filter,
			short max_tables) throws MetaException, UnknownDBException {
		throw new MetaException("Please use getAllTables and do filtering by yourself.");
	}

	@Override
	public List<String> listPartitionNames(String db_name, String tbl_name,
			short max_parts) throws MetaException {
		return new ArrayList<String>();
	}

	@Override
	public List<String> listPartitionNamesByFilter(String db_name,
			String tbl_name, String filter, short max_parts)
			throws MetaException {
		throw new MetaException("Please use listPartitionNames and do filtering by yourself.");
	}

	@Override
	public void alterPartition(String db_name, String tbl_name,
			String partName, List<String> part_vals, Partition new_part)
			throws InvalidObjectException, MetaException {
	}

	@Override
	public void alterPartitions(String db_name, String tbl_name,
			List<String> partNames, List<List<String>> part_vals_list,
			List<Partition> new_parts) throws InvalidObjectException,
			MetaException {
	}

	@Override
	public boolean addIndex(Index index) throws InvalidObjectException,
			MetaException {
		return false;
	}

	@Override
	public Index getIndex(String dbName, String origTableName, String indexName)
			throws MetaException {
		try {
			Index in = (Index) cs.readObject(ObjectType.INDEX, dbName+"."+origTableName+"."+indexName);
			return in;
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public boolean dropIndex(String dbName, String origTableName,
			String indexName) throws MetaException {
		return false;
	}

	@Override
	//objectstore 中的实现没管max
	public List<Index> getIndexes(String dbName, String origTableName, int max)
			throws MetaException {
		List<Index> ins = new ArrayList<Index>();
		for(Map.Entry<String, Index> en : CacheStore.getIndexHm().entrySet())
		{
			if(en.getKey().startsWith(dbName+"."+origTableName+".")) {
        ins.add(en.getValue());
      }
		}
		return ins;
	}

	@Override
	public List<String> listIndexNames(String dbName, String origTableName,
			short max) throws MetaException {
		List<String> ins = new ArrayList<String>();
		for(Map.Entry<String, Index> en : CacheStore.getIndexHm().entrySet())
		{
			if(en.getKey().startsWith(dbName+"."+origTableName+".")) {
        ins.add(en.getValue().getIndexName());
      }
		}
		return ins;
	}

	@Override
	public void alterIndex(String dbname, String baseTblName, String name,
			Index newIndex) throws InvalidObjectException, MetaException {
	}

	@Override
	public List<Partition> getPartitionsByFilter(String dbName, String tblName,
			String filter, short maxParts) throws MetaException,
			NoSuchObjectException {
	  throw new MetaException("Please use listPartitionNames and do filtering by yourself.");
	}

	@Override
	public List<Partition> getPartitionsByNames(String dbName, String tblName,
			List<String> partNames) throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Table markPartitionForEvent(String dbName, String tblName,
			Map<String, String> partVals, PartitionEventType evtType)
			throws MetaException, UnknownTableException,
			InvalidPartitionException, UnknownPartitionException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isPartitionMarkedForEvent(String dbName, String tblName,
			Map<String, String> partName, PartitionEventType evtType)
			throws MetaException, UnknownTableException,
			InvalidPartitionException, UnknownPartitionException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addRole(String rowName, String ownerName)
			throws InvalidObjectException, MetaException, NoSuchObjectException {
		return false;
	}

	@Override
	public boolean removeRole(String roleName) throws MetaException,
			NoSuchObjectException {
		return false;
	}

	@Override
	public boolean grantRole(Role role, String userName,
			PrincipalType principalType, String grantor,
			PrincipalType grantorType, boolean grantOption)
			throws MetaException, NoSuchObjectException, InvalidObjectException {
		return false;
	}

	@Override
	public boolean revokeRole(Role role, String userName,
			PrincipalType principalType) throws MetaException,
			NoSuchObjectException {
		return false;
	}

	@Override
	public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
			List<String> groupNames) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName,
			String userName, List<String> groupNames)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName,
			String tableName, String userName, List<String> groupNames)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName,
			String tableName, String partition, String userName,
			List<String> groupNames) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName,
			String tableName, String partitionName, String columnName,
			String userName, List<String> groupNames)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MGlobalPrivilege> listPrincipalGlobalGrants(
			String principalName, PrincipalType principalType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MDBPrivilege> listPrincipalDBGrants(String principalName,
			PrincipalType principalType, String dbName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MTablePrivilege> listAllTableGrants(String principalName,
			PrincipalType principalType, String dbName, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MPartitionPrivilege> listPrincipalPartitionGrants(
			String principalName, PrincipalType principalType, String dbName,
			String tableName, String partName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
			String principalName, PrincipalType principalType, String dbName,
			String tableName, String columnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
			String principalName, PrincipalType principalType, String dbName,
			String tableName, String partName, String columnName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean grantPrivileges(PrivilegeBag privileges)
			throws InvalidObjectException, MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean revokePrivileges(PrivilegeBag privileges)
			throws InvalidObjectException, MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Role getRole(String roleName) throws NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listRoleNames() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<MRoleMap> listRoles(String principalName,
			PrincipalType principalType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition getPartitionWithAuth(String dbName, String tblName,
			List<String> partVals, String user_name, List<String> group_names)
			throws MetaException, NoSuchObjectException, InvalidObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
			short maxParts, String userName, List<String> groupNames)
			throws MetaException, NoSuchObjectException, InvalidObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> listPartitionNamesPs(String db_name, String tbl_name,
			List<String> part_vals, short max_parts) throws MetaException,
			NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Partition> listPartitionsPsWithAuth(String db_name,
			String tbl_name, List<String> part_vals, short max_parts,
			String userName, List<String> groupNames) throws MetaException,
			InvalidObjectException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean updateTableColumnStatistics(ColumnStatistics colStats)
			throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
			List<String> partVals) throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ColumnStatistics getTableColumnStatistics(String dbName,
			String tableName, String colName) throws MetaException,
			NoSuchObjectException, InvalidInputException,
			InvalidObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnStatistics getPartitionColumnStatistics(String dbName,
			String tableName, String partName, List<String> partVals,
			String colName) throws MetaException, NoSuchObjectException,
			InvalidInputException, InvalidObjectException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean deletePartitionColumnStatistics(String dbName,
			String tableName, String partName, List<String> partVals,
			String colName) throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteTableColumnStatistics(String dbName, String tableName,
			String colName) throws NoSuchObjectException, MetaException,
			InvalidObjectException, InvalidInputException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public long cleanupEvents() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Node findNode(String ip) throws MetaException {
		for(Node n : CacheStore.getNodeHm().values())
		{
			for(String p : n.getIps()) {
        if(p.equals(ip)) {
          return n;
        }
      }
		}
		return null;
	}

	@Override
	public boolean addUser(String userName, String passwd, String ownerName)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeUser(String userName) throws MetaException,
			NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<String> listUsersNames() throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean authentication(String userName, String passwd)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public MUser getMUser(String user) {
		// TODO Auto-generated method stub
		return null;
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
	public void findFiles(List<SFile> underReplicated, List<SFile> overReplicated, List<SFile> lingering, long from,
			long to) throws MetaException {
		try {
			cs.findFiles(underReplicated, overReplicated, lingering, from, to);
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public void findVoidFiles(List<SFile> voidFiles) throws MetaException {
		try {
			cs.findVoidFiles(voidFiles);
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public void createPartitionIndex(Index index, Partition part)
			throws InvalidObjectException, MetaException,
			AlreadyExistsException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createPartitionIndex(Index index, Subpartition part)
			throws InvalidObjectException, MetaException,
			AlreadyExistsException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean dropPartitionIndex(Index index, Partition part)
			throws InvalidObjectException, NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dropPartitionIndex(Index index, Subpartition part)
			throws InvalidObjectException, NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void createPartitionIndexStores(Index index, Partition part,
			List<SFile> store, List<Long> originFid)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public void createPartitionIndexStores(Index index, Subpartition part,
			List<SFile> store, List<Long> originFid)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean dropPartitionIndexStores(Index index, Partition part,
			List<SFile> store) throws InvalidObjectException,
			NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean dropPartitionIndexStores(Index index, Subpartition part,
			List<SFile> store) throws InvalidObjectException,
			NoSuchObjectException, MetaException {
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
	public boolean add_datawarehouse_sql(int dwNum, String sql)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<SFileRef> getSubpartitionIndexFiles(Index index,
			Subpartition subpart) throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Subpartition> getSubpartitions(String dbname, String tbl_name,
			Partition part) throws InvalidObjectException,
			NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BusiTypeColumn> getAllBusiTypeCols() throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Partition getParentPartition(String dbName, String tableName,
			String subpart_name) throws NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
			throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void append_busi_type_datacenter(
			BusiTypeDatacenter busiTypeDatacenter)
			throws InvalidObjectException, MetaException, TException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Busitype> showBusitypes() throws MetaException, TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int createBusitype(Busitype busitype) throws InvalidObjectException,
			MetaException, TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Device getDevice(String devid) throws MetaException,	NoSuchObjectException {
	  Device de = null;

		try {
			de = (Device) cs.readObject(ObjectType.DEVICE, devid);
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
		if (de == null) {
		  throw new NoSuchObjectException("Can not find device :"+devid);
		}
		return de;
	}

	@Override
	public boolean delDevice(String devid) throws MetaException {
		Device de = null;
		try {
			de = this.getDevice(devid);
			if(de == null) {
        return true;
      }
		} catch (NoSuchObjectException e) {
			return true;
		}
		try {
			cs.removeObject(ObjectType.DEVICE, de.getDevid());
			HashMap<String, Object> old_params = new HashMap<String, Object>();
			old_params.put("devid", de.getDevid());
			MsgServer.addMsg(MsgServer.generateDDLMsg(MSGType.MSG_DEL_DEVICE, -1l, -1l, null, de, old_params));
			return true;
		} catch (Exception e) {
			throw new MetaException(e.getMessage());
		}
	}

	@Override
	public boolean modifyUser(User user) throws MetaException,
			NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
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
	public List<String> listUsersNames(String dbName) throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GlobalSchema getSchema(String schema_name)
			throws NoSuchObjectException, MetaException {
	  GlobalSchema gs = null;

		try {
			 gs = (GlobalSchema) cs.readObject(ObjectType.GLOBALSCHEMA, schema_name);
		} catch (Exception e) {
			LOG.error(e,e);
			throw new MetaException(e.getMessage());
		}
		if (gs == null) {
		  throw new NoSuchObjectException("Can not find globalschema :"+schema_name);
		}
		return gs;
	}

	@Override
	public boolean modifySchema(String schemaName, GlobalSchema schema)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteSchema(String schemaName)
			throws InvalidObjectException, InvalidInputException,
			NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<GlobalSchema> listSchemas() throws MetaException {
		List<GlobalSchema> gl = new ArrayList<GlobalSchema>();
		gl.addAll(CacheStore.getGlobalSchemaHm().values());
		return gl;
	}

	@Override
	public boolean addNodeGroup(NodeGroup ng) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean alterNodeGroup(NodeGroup ng) throws InvalidObjectException,
			MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean modifyNodeGroup(String ngName, NodeGroup ng)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroup(NodeGroup ng) throws MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listNodeGroups() throws MetaException {
		List<NodeGroup> ngl = new ArrayList<NodeGroup>();
		ngl.addAll(CacheStore.getNodeGroupHm().values());
		return ngl;
	}

	@Override
	public List<NodeGroup> listDBNodeGroups(String dbName) throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addTableNodeDist(String db, String tab, List<String> ng)
			throws MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteTableNodeDist(String db, String tab, List<String> ng)
			throws MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listTableNodeDists(String dbName, String tabName)
			throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createSchema(GlobalSchema schema)
			throws InvalidObjectException, MetaException {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Long> listTableFiles(String dbName, String tableName,
			int begin, int end) throws MetaException {
		try {
      return cs.listTableFiles(dbName,tableName,begin,end);
    } catch (JedisException e ) {
      throw new MetaException(e.getMessage());
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
	}

	@Override
	public List<Long> findSpecificDigestFiles(String digest)
			throws MetaException {
	  try {
	    return cs.listFilesByDegist(digest);
	  } catch (JedisException e ) {
	    throw new MetaException(e.getMessage());
	  } catch (IOException e) {
	    throw new MetaException(e.getMessage());
	  }
	}

	@Override
	public List<SFile> filterTableFiles(String dbName, String tableName,
			List<SplitValue> values) throws MetaException {
	  try {
	    return cs.filterTableFiles(dbName, tableName, values);
	  } catch (JedisException e ) {
      throw new MetaException(e.getMessage());
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    }
	}

	@Override
	public boolean assiginSchematoDB(String dbName, String schemaName,
			List<FieldSchema> fileSplitKeys, List<FieldSchema> part_keys,
			List<NodeGroup> ngs) throws InvalidObjectException,
			NoSuchObjectException, MetaException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<NodeGroup> listNodeGroupByNames(List<String> ngNames)
			throws MetaException {
		List<NodeGroup> ngs = new ArrayList<NodeGroup>();
		for(String name : ngNames) {
			try {
				NodeGroup ng = (NodeGroup) cs.readObject(ObjectType.NODEGROUP, name);
				if(ng != null) {
          ngs.add(ng);
        }
			} catch (Exception e) {
				LOG.error(e,e);
				throw new MetaException(e.getMessage());
			}
		}
		return ngs;
	}

	@Override
	public GeoLocation getGeoLocationByName(String geoLocName)
			throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<GeoLocation> getGeoLocationByNames(List<String> geoLocNames)
			throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addNodeAssignment(String nodename, String dbname)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeAssignment(String nodeName, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Node> listNodes() throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addUserAssignment(String userName, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteUserAssignment(String userName, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<User> listUsers() throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addRoleAssignment(String roleName, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteRoleAssignment(String roleName, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public List<Role> listRoles() throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean addNodeGroupAssignment(NodeGroup ng, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean deleteNodeGroupAssignment(NodeGroup ng, String dbName)
			throws MetaException, NoSuchObjectException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void truncTableFiles(String dbName, String tableName) throws MetaException, NoSuchObjectException {
      for (int i = 0, step = 1000; i < Integer.MAX_VALUE; i+=step) {
        List<Long> files = listTableFiles(dbName, tableName, i, i + step);
        for (int j = 0; j < files.size(); j++) {
        	LOG.debug("truncTableFiles, sfile.size()=" + files.size());
          SFile f = this.getSFile(files.get(j));
          if (f != null) {
            f.setStore_status(MetaStoreConst.MFileStoreStatus.RM_PHYSICAL);
            updateSFile(f);
          }
        }
        if (files.size() < step) {
          break;
        }
     }
	}

	@Override
	public boolean reopenSFile(SFile file) throws MetaException {
		boolean changed = false;
		SFile sf = this.getSFile(file.getFid());
		if (sf == null) {
      throw new MetaException("No SFile found by id " + file.getFid());
    }
		if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
      List<SFileLocation> sfl = sf.getLocations();
      boolean selected = false;
      int idx = 0;

      if (sfl.size() > 0) {
        for (int i = 0; i < sfl.size(); i++) {
          SFileLocation x = sfl.get(i);
          Device d = null;

          try {
            d = (Device) cs.readObject(ObjectType.DEVICE, x.getDevid());
          } catch (Exception e) {
          }

          // BUG-XXX: we do NOT use L1/L4 device as reopen location candidate
          if (x.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE &&
              (d != null && (DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.GENERAL ||
              DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.MASS))) {
            selected = true;
            idx = i;
            break;
          }
        }
        if (!selected) {
          // ok, try to select shared device
          for (int i = 0; i < sfl.size(); i++) {
           SFileLocation x = sfl.get(i);

            if (x.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
              selected = true;
              idx = i;
              break;
            }
          }
        }
        if (selected) {
          // it is ok to reopen, and close other locations

          // BUG-XXX: recheck the SFL status, make sure it exists and is valid!
          if (getSFileLocation(sfl.get(idx).getDevid(), sfl.get(idx).getLocation()) == null ||
              getSFileLocation(sfl.get(idx).getDevid(), sfl.get(idx).getLocation()).getVisit_status()
                != MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            // ABORT REOPEN NOW
            selected = false;
          } else {
            for (int i = 0; i < sfl.size(); i++) {
              if (i != idx) {
                SFileLocation x = sfl.get(i);
                // BUG-XXX: delete the SFL, cause SFL not found exception for incomming REP_DONE
                this.delSFileLocation(x.getDevid(), x.getLocation());
              }
            }
            SFile nf = this.getSFile(file.getFid());
            if (nf != null) {
              nf.setStore_status(MetaStoreConst.MFileStoreStatus.INCREATE);
              this.updateSFile(nf);
            }
          }
        }
      }
      if (selected) {
        changed = true;
      }
    }
		return changed;
	}

	@Override
	public long getCurrentFID() {
		return g_fid;
	}

	@Override
	public List<Device> listDevice() throws MetaException {
		List<Device> dl = new ArrayList<Device>();
		dl.addAll(CacheStore.getDeviceHm().values());
		return dl;
	}

	@Override
	public statfs statFileSystem(long from, long to) throws MetaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long countDevice() throws MetaException {
		return CacheStore.getDeviceHm().size();
	}

	@Override
	public long getMinFID() throws MetaException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public List<String> listDevsByNode(String nodeName) throws MetaException {
		List<String> ls = new LinkedList<String>();
		Iterator<Device> iter = CacheStore.getDeviceHm().values().iterator();
		while(iter.hasNext()){
			Device dev = iter.next();
			if(dev.getNode_name().equals(nodeName)) {
        ls.add(dev.getDevid());
      }
		}
		return ls;
	}

	@Override
	public List<Long> listFilesByDevs(List<String> devids) throws MetaException,
			TException {
		List<Long> ids = new LinkedList<Long>();
		long ct = System.currentTimeMillis();
		for(String devid : devids)
		{
			for(SFileLocation sfl : this.getSFileLocations(devid, ct, 0))
			{
				ids.add(sfl.getFid());
			}
		}
		return ids;
	}

  @Override
  public long countFiles() throws MetaException {
    try {
      return cs.getCounts(ObjectType.SFILE);
    } catch (JedisException e) {
      LOG.error(e, e);
      throw new MetaException(e.getMessage());
    } catch (IOException e) {
      LOG.error(e, e);
      throw new MetaException(e.getMessage());
    }
  }
}
