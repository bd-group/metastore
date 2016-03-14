package org.apache.hadoop.hive.metastore.newms;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.FileOperationException;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;
import org.apache.thrift.TException;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class MsgProcessing {

	private IMetaStoreClient client;
	private static HiveConf hiveConf;
	private final CacheStore cs;
	private static final Log LOG = LogFactory.getLog(MsgProcessing.class);

	public boolean __reconnect() {
	  try {
      client = createMetaStoreClient();
    } catch (Exception e) {
      LOG.error("__reconnect() to MS failed: " + e.getMessage());
    }
	  return client != null;
	}

	public MsgProcessing() throws MetaException, IOException {
	  client = createMetaStoreClient();
	  cs = new CacheStore();
	}

	public void getAllObjects() throws Exception {
		try {
			if (client != null && hiveConf.getBoolVar(ConfVars.NEWMS_IS_GET_ALL_OBJECTS)) {
				long start = System.currentTimeMillis();
				// device
	      List<Device> dl = client.listDevice();
	      for (Device de : dl) {
          cs.writeObject(ObjectType.DEVICE, de.getDevid(), de, false);
        }
	      long end = System.currentTimeMillis();
	      LOG.info("Get devices in " + (end - start) + " ms");
	      start = end;
	      //db
				List<Database> dbs = client.get_all_attributions();
	      for(Database db : dbs)
	      {
	        cs.writeObject(ObjectType.DATABASE, db.getName(), db, false);
	      }
	      end = System.currentTimeMillis();
	      LOG.info("get databases in "+(end-start)+" ms");
	      start = end;

	      //sfile  sfilelocation
	      //把所有的sfile得到应该就不需要再拉取sfilelocation了
	      long maxid = client.getMaxFid();
	      synchronized (RawStoreImp.class) {
	      	if(RawStoreImp.getFid() < maxid)
           {
	      	  //如果这个时候有人创建文件。。。
            RawStoreImp.setFID(maxid+1000);
          }
				}
	      int clientn = 10;
	      long num = maxid/clientn + 1;
	      List<Thread> ths = new LinkedList<Thread>();
	      for(long id = 0;id < maxid; id+= num)
	      {
	      	if(id + num >= maxid) {
            ths.add(new Thread(new GFThread(id, maxid)));
          } else {
            ths.add(new Thread(new GFThread(id, id+num-1)));
          }

	      }
	      for(Thread t : ths) {
          t.start();
        }
	      end = System.currentTimeMillis();

	      start = end;

	      //table  index
	      for(Database db : dbs)
	      {
	        for(String tn : client.getAllTables(db.getName()))
	        {
	        	Table t = client.getTable(db.getName(), tn);
	        	cs.writeObject(ObjectType.TABLE, t.getDbName()+"."+t.getTableName(), t, false);
	        	for(Index in : client.listIndexes(db.getName(), tn,(short) 127)) {
              cs.writeObject(ObjectType.INDEX, db.getName()+"."+tn+"."+in.getIndexName(), in, false);
            }
	        }
	      }
	      end = System.currentTimeMillis();
	      LOG.info("get tables and indexes in "+(end-start)+" ms");
	      start = end;
	      //partition    no partition so far

	      //node
	      for(Node n : client.listNodes()) {
          cs.writeObject(ObjectType.NODE, n.getNode_name(),n, false);
        }
	      end = System.currentTimeMillis();
	      LOG.info("get nodes in "+(end-start)+" ms");
	      start = end;
	      //globalschema
	      for(GlobalSchema gs : client.listSchemas()) {
          cs.writeObject(ObjectType.GLOBALSCHEMA, gs.getSchemaName(), gs, false);
        }
	      end = System.currentTimeMillis();
	      LOG.info("get globalschema in "+(end-start)+" ms");
	      start = end;
	      //nodegroup
	      for(NodeGroup ng : client.listNodeGroups()) {
          cs.writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ng, false);
        }
	      end = System.currentTimeMillis();
	      LOG.info("get nodegroup in "+(end-start)+" ms");
	      start = end;

				try {
					for(Thread t : ths) {
            t.join();
          }
				} catch (InterruptedException e) {
					LOG.error(e,e);
				}
				LOG.info("get sfile and sfilelocation in "+(end-start)+" ms");
				LOG.info("get all objects complete.");
			}
		} catch (Exception e) {
			LOG.error(e,e);
			throw e;
		}
	}

	private class GFThread implements Runnable
	{
		private final long from;
		private final long to;
		private IMetaStoreClient cli;
		public GFThread(long from, long to) {
			this.from = from;
			this.to = to;
			try {
				cli = createMetaStoreClient();
			} catch (MetaException e) {
				LOG.error(e,e);
			}
		}
		@Override
		public void run() {
			long start = System.currentTimeMillis();
			try{
				long num = 1000;

				for (long id = from; id <= to; id += num) {
					LinkedList<Long> ids = new LinkedList<Long>();
		    	for (long fid = id; fid < num + id && fid <= to; fid++) {
		    		ids.add(fid);
		    	}
		    	List<SFile> files = cli.get_files_by_ids(ids);
	    		LOG.info(Thread.currentThread().getId() + ": get_files_by_ids: " + ids.get(0) + " , " +
	    		    ids.get(ids.size() - 1) + ", get number:" + files.size());
	    		for (SFile sf : files) {
	    			cs.writeObject(ObjectType.SFILE, sf.getFid()+"", sf, true);
	    		}
	    	}
			}catch(Exception e){
				LOG.error(e,e);
			}
			cli.close();
			LOG.info(("In GFThread "+Thread.currentThread().getId()+", get file from " + from + " to " + to + " consume " + (System.currentTimeMillis()-start) + " ms"));
		}

	}
	public static IMetaStoreClient createMetaStoreClient() throws MetaException {
		if (hiveConf == null) {
      hiveConf = new HiveConf();
    }
		if (!hiveConf.getBoolVar(ConfVars.NEWMS_IS_USE_METASTORE_CLIENT)) {
			return null;
		}
		HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
			public HiveMetaHook getHook(
					org.apache.hadoop.hive.metastore.api.Table tbl)
					throws MetaException {

				return null;
			}
		};
		return RetryingMetaStoreClient.getProxy(hiveConf, hookLoader, HiveMetaStoreClient.class.getName());
	}

	public void handleMsg(DDLMsg msg) throws JedisConnectionException, IOException, NoSuchObjectException, ClassNotFoundException {
		if (!hiveConf.getBoolVar(ConfVars.NEWMS_IS_USE_METASTORE_CLIENT)) {
			return;
		}
		if (client == null && !__reconnect()) {
      return;
    }
		int eventid = (int) msg.getEvent_id();
		DDLMsg rmsg = null;

		try {
		  switch (eventid) {
		  case MSGType.MSG_NEW_DATABESE:
		  case MSGType.MSG_ALTER_DATABESE:
		  case MSGType.MSG_ALTER_DATABESE_PARAM:
		  {
		    String dbName = (String) msg.getMsg_data().get("db_name");
		    try {
		      Database db = client.getDatabase(dbName);
		      cs.writeObject(ObjectType.DATABASE, dbName, db, false);
		    } catch (NoSuchObjectException e) {
		      LOG.error(e,e);
		    }
		    break;
		  }
		  case MSGType.MSG_DROP_DATABESE:
		  {
		    String dbname = (String) msg.getMsg_data().get("db_name");
		    cs.removeObject(ObjectType.DATABASE, dbname);
		    break;
		  }
		  case MSGType.MSG_ALT_TALBE_NAME:
		  {
		    String dbName = (String) msg.getMsg_data().get("db_name");
		    String tableName = (String) msg.getMsg_data().get("table_name");
		    String oldTableName = (String) msg.getMsg_data().get("old_table_name");
		    String oldKey = dbName + "." + oldTableName;
		    String newKey = dbName + "." + tableName;
		    /*
		    if(CacheStore.getTableHm().remove(oldKey) != null){
		      cs.removeObject(ObjectType.TABLE, oldKey);
		    }
		    */
		    cs.removeObject(ObjectType.TABLE, oldKey);
		    Table tbl = client.getTable(dbName, tableName);
		    cs.writeObject(ObjectType.TABLE, newKey, tbl, false);
		    break;
		  }
		  case MSGType.MSG_NEW_TALBE:
		  case MSGType.MSG_ALT_TALBE_DISTRIBUTE:
		  case MSGType.MSG_ALT_TALBE_PARTITIONING:
		  case MSGType.MSG_ALT_TABLE_SPLITKEYS:
		  case MSGType.MSG_ALT_TALBE_DEL_COL:
		  case MSGType.MSG_ALT_TALBE_ADD_COL:
		  case MSGType.MSG_ALT_TALBE_ALT_COL_NAME:
		  case MSGType.MSG_ALT_TALBE_ALT_COL_TYPE:
		  case MSGType.MSG_ALT_TABLE_PARAM:
		  {
		    String dbName = (String) msg.getMsg_data().get("db_name");
		    String tableName = (String) msg.getMsg_data().get("table_name");
		    String key = dbName + "." + tableName;
		    try {
		      cs.removeObject(ObjectType.TABLE, key);
		      Table tbl = client.getTable(dbName, tableName);
		      cs.writeObject(ObjectType.TABLE, key, tbl, false);
		    } catch (NoSuchObjectException e) {
		      LOG.error(e, e);
		    }
		    /*
				TableImage ti = TableImage.generateTableImage(tbl);
				CacheStore.getTableHm().put(key, tbl);
				cs.writeObject(ObjectType.TABLE, key, ti);
				//table里有nodegroup。。。。
				for(int i = 0; i < ti.getNgKeys().size();i++){
					String action = (String) msg.getMsg_data().get("action");
					if((action != null && !action.equals("delng")) || action == null){
						if(!CacheStore.getNodeGroupHm().containsKey(ti.getNgKeys().get(i))){
							List<String> ngNames = new ArrayList<String>();
							ngNames.add(ti.getNgKeys().get(i));
							List<NodeGroup> ngs = client.listNodeGroups(ngNames);
							NodeGroup ng = ngs.get(0);
							NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
							CacheStore.getNodeGroupHm().put(ng.getNode_group_name(), ng);
							cs.writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
							for(int j = 0; j<ngi.getNodeKeys().size();j++){
								if(!CacheStore.getNodeHm().containsKey(ngi.getNodeKeys().get(j))){
									Node node = client.get_node(ngi.getNodeKeys().get(j));
									cs.writeObject(ObjectType.NODE, ngi.getNodeKeys().get(j), node);
								}
							}
						}
					}
				}
		     */
		    break;
		  }
		  case MSGType.MSG_DROP_TABLE:
		  {
		    String dbName = (String) msg.getMsg_data().get("db_name");
		    String tableName = (String) msg.getMsg_data().get("table_name");
		    String key = dbName + "." + tableName;
		    if (CacheStore.getTableHm().remove(key) != null){
		      cs.removeObject(ObjectType.TABLE, key);
		    }
		    break;
		  }
		  case MSGType.MSG_REP_FILE_CHANGE:
		  {
		    Object id = msg.getMsg_data().get("f_id");
		    if (id == null) {
		      break;
		    }
		    long fid = Long.parseLong(id.toString());
		    SFile sf = null;
		    try {
		      sf = client.get_file_by_id(fid);
		    } catch (FileOperationException e) {
		      // Can not find SFile by FID ...
		      LOG.error(e,e);
		      if (sf == null) {
		        break;
		      }
		    }
		    SFile of = (SFile) cs.readObject(ObjectType.SFILE, fid + "");
		    if (of != null) {
		      cs.removeSfileStatValue(of.getStore_status(), fid + "");
		      cs.removeLfbdValue(of.getDigest(), fid + "");
		      if (of.getLocations() != null && of.getLocationsSize() > 0) {
		        for (SFileLocation sfl: of.getLocations()) {
		          String sflkey = SFileImage.generateSflkey(sfl.getLocation(), sfl.getDevid());
		          cs.removeObject(ObjectType.SFILELOCATION, sflkey);
		        }
		      }
		    }
		    cs.writeObject(ObjectType.SFILE, fid+"", sf, true);
		    break;
		  }
		  case MSGType.MSG_STA_FILE_CHANGE:
		  case MSGType.MSG_REP_FILE_ONOFF:
		  case MSGType.MSG_CREATE_FILE:
		  {
		    Object id = msg.getMsg_data().get("f_id");
		    if (id == null) {
		      break;
		    }
		    long fid = Long.parseLong(id.toString());
		    SFile sf = null;
		    try {
		      sf = client.get_file_by_id(fid);
		    } catch (FileOperationException e) {
		      //Can not find SFile by FID ...
		      LOG.error(e,e);
		      if (sf == null) {
		        break;
		      }
		    }
		    SFile of = (SFile) cs.readObject(ObjectType.SFILE, fid + "");
		    if (of != null) {
		      cs.removeSfileStatValue(of.getStore_status(), fid + "");
		      cs.removeLfbdValue(of.getDigest(), fid + "");
		      if (of.getLocations() != null && of.getLocationsSize() > 0) {
		        for (SFileLocation sfl: of.getLocations()) {
		          String sflkey = SFileImage.generateSflkey(sfl.getLocation(), sfl.getDevid());
		          cs.removeObject(ObjectType.SFILELOCATION, sflkey);
		        }
		      }
		    }

		    cs.writeObject(ObjectType.SFILE, fid + "", sf, true);
		    break;
		  }
		  //在删除文件时，会在之前发几个1307,然后才是4002
		  case MSGType.MSG_DEL_FILE:
		  {
		    long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
		    SFile sf = (SFile)cs.readObject(ObjectType.SFILE, fid + "");
		    if (sf != null) {
		      cs.removeObject(ObjectType.SFILE, fid + "");
		    }
		    break;
		  }
		  case MSGType.MSG_NEW_INDEX:
		  case MSGType.MSG_ALT_INDEX:
		  case MSGType.MSG_ALT_INDEX_PARAM:
		  {
		    String dbName = (String)msg.getMsg_data().get("db_name");
		    String tblName = (String)msg.getMsg_data().get("table_name");
		    String indexName = (String)msg.getMsg_data().get("index_name");
		    if (dbName == null || tblName == null || indexName == null) {
		      break;
		    }
		    Index ind = client.getIndex(dbName, tblName, indexName);
		    String key = dbName + "." + tblName + "." + indexName;
		    cs.writeObject(ObjectType.INDEX, key, ind, false);
		    break;
		  }
		  case MSGType.MSG_DEL_INDEX:
		  {
		    String dbName = (String)msg.getMsg_data().get("db_name");
		    String tblName = (String)msg.getMsg_data().get("table_name");
		    String indexName = (String)msg.getMsg_data().get("index_name");
		    //Index ind = client.getIndex(dbName, tblName, indexName);

		    String key = dbName + "." + tblName + "." + indexName;
		    if(CacheStore.getIndexHm().remove(key) != null) {
		      cs.removeObject(ObjectType.INDEX, key);
		    }
		    break;
		  }

		  case MSGType.MSG_NEW_NODE:
		  case MSGType.MSG_FAIL_NODE:
		  case MSGType.MSG_BACK_NODE:
		  {
		    String nodename = (String)msg.getMsg_data().get("node_name");
		    Node node = client.get_node(nodename);
		    cs.writeObject(ObjectType.NODE, nodename, node, false);
		    break;
		  }

		  case MSGType.MSG_DEL_NODE:
		  {
		    String nodename = (String)msg.getMsg_data().get("node_name");
		    cs.removeObject(ObjectType.NODE, nodename);
		    cs.updateCache(ObjectType.NODEGROUP);
		    cs.updateCache(ObjectType.TABLE);
		    break;
		  }

		  case MSGType.MSG_CREATE_SCHEMA:
		  case MSGType.MSG_MODIFY_SCHEMA_DEL_COL:
		  case MSGType.MSG_MODIFY_SCHEMA_ADD_COL:
		  case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_NAME:
		  case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_TYPE:
		  case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_COMMENT:
		  case MSGType.MSG_MODIFY_SCHEMA_PARAM:
		  {
		    String schema_name = (String)msg.getMsg_data().get("schema_name");
		    try{
		      GlobalSchema s = client.getSchemaByName(schema_name);
		      cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, s, false);
		    }catch(NoSuchObjectException e){
		      LOG.error(e,e);
		    }

		    break;
		  }

		  case MSGType.MSG_MODIFY_SCHEMA_NAME:
		  {
		    String old_schema_name = (String)msg.getMsg_data().get("old_schema_name");
		    String schema_name = (String)msg.getMsg_data().get("schema_name");
		    GlobalSchema gs = CacheStore.getGlobalSchemaHm().get(old_schema_name);
		    if(gs != null)
		    {
		      GlobalSchema ngs = client.getSchemaByName(schema_name);
		      cs.removeObject(ObjectType.GLOBALSCHEMA, old_schema_name);
		      cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, ngs, false);
		    }
		    else{
		      try{
		        GlobalSchema ngs = client.getSchemaByName(schema_name);
		        cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, ngs, false);
		      }
		      catch(NoSuchObjectException e)
		      {
		        LOG.error(e,e);
		      }
		    }
		    break;
		  }

		  case MSGType.MSG_DEL_SCHEMA:
		  {
		    String schema_name = (String)msg.getMsg_data().get("schema_name");
		    cs.removeObject(ObjectType.GLOBALSCHEMA, schema_name);
		    break;
		  }

		  case MSGType.MSG_NEW_NODEGROUP:
		  case MSGType.MSG_ALTER_NODEGROUP:
		  case MSGType.MSG_MODIFY_NODEGROUP:
		  {
		    String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
		    List<String> ngNames = new ArrayList<String>();
		    ngNames.add(nodeGroupName);
		    List<NodeGroup> ngs = client.listNodeGroups(ngNames);
		    if (ngs == null || ngs.size() == 0) {
		      break;
		    }
		    NodeGroup ng = ngs.get(0);
		    cs.writeObject(ObjectType.NODEGROUP, nodeGroupName, ng, false);
		    cs.updateCache(ObjectType.TABLE);
		    break;
		  }
		  case MSGType.MSG_DEL_NODEGROUP:{
		    String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
		    cs.removeObject(ObjectType.NODEGROUP, nodeGroupName);
		    cs.updateCache(ObjectType.TABLE);
		    break;
		  }

		  //what can I do...
		  case MSGType.MSG_GRANT_GLOBAL:
		  case MSGType.MSG_GRANT_DB:
		  case MSGType.MSG_GRANT_TABLE:
		  case MSGType.MSG_GRANT_SCHEMA:
		  case MSGType.MSG_GRANT_PARTITION:
		  case MSGType.MSG_GRANT_PARTITION_COLUMN:
		  case MSGType.MSG_GRANT_TABLE_COLUMN:

		  case MSGType.MSG_REFRESH_ALL:

		  case MSGType.MSG_REVOKE_GLOBAL:
		  case MSGType.MSG_REVOKE_DB:
		  case MSGType.MSG_REVOKE_TABLE:
		  case MSGType.MSG_REVOKE_PARTITION:
		  case MSGType.MSG_REVOKE_SCHEMA:
		  case MSGType.MSG_REVOKE_PARTITION_COLUMN:
		  case MSGType.MSG_REVOKE_TABLE_COLUMN:
		  {
		    //		    	client.
		    break;
		  }

		  case MSGType.MSG_CREATE_DEVICE:
		  {
		  	String devid = msg.getMsg_data().get("devid").toString();
		  	try {
		  	  Device d = client.getDevice(devid);
		  	  if (d == null) {
		  	    break;
		  	  }
		  	  cs.writeObject(ObjectType.DEVICE, devid, d, false);
		  	  // NOTE-XXX: check if this device is offline or disabled,
		  	  // send msg if needed
		  	  HashMap<String, Object> old_params = new HashMap<String, Object>();
		  	  old_params.put("devid", d.getDevid());
		  	  old_params.put("node", d.getNode_name());
		  	  old_params.put("status", d.getStatus());

		  	  switch (d.getStatus()) {
		  	  case MetaStoreConst.MDeviceStatus.OFFLINE:
		  	    rmsg = MsgServer.generateDDLMsg(MSGType.MSG_DEVICE_RO,
		  	        -1l, -1l, null, d, old_params);
		  	    break;
		  	  case MetaStoreConst.MDeviceStatus.DISABLE:
		  	    rmsg = MsgServer.generateDDLMsg(MSGType.MSG_DEVICE_OFFLINE,
		  	        -1l, -1l, null, d, old_params);
		  	    break;
		  	  case MetaStoreConst.MDeviceStatus.ONLINE:
		  	    rmsg = MsgServer.generateDDLMsg(MSGType.MSG_DEVICE_RW,
		  	        -1l, -1l, null, d, old_params);
		  	    break;
		  	  case MetaStoreConst.MDeviceStatus.SUSPECT:
		  	    rmsg = MsgServer.generateDDLMsg(MSGType.MSG_DEVICE_SUSPECT,
		  	        -1l, -1l, null, d, old_params);
		  	    break;
		  	  }
		  	} catch (NoSuchObjectException e) {
		  	  LOG.error(e, e);
		  	}
		  	break;
		  }
		  case MSGType.MSG_DEL_DEVICE:
		  {
		  	String devid = msg.getMsg_data().get("devid").toString();
		  	cs.removeObject(ObjectType.DEVICE, devid);
		  	// BUG-XXX: we should clear the admap entry
		  	ThriftRPC.dm.removeFromADMap(devid, null);
		  	break;
		  }
		  default:
		  {
		    LOG.debug("unhandled msg from metaq: "+msg.getEvent_id());
		    break;
		  }
		  }
		  // ok, we just resend the msg to producer's topic now
		  MsgServer.pdSend(msg);
		  if (rmsg != null) {
        MsgServer.pdSend(rmsg);
      }
		} catch (TException e) {
		  __reconnect();
		}
	}
}
