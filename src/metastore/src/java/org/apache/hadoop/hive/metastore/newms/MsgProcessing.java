package org.apache.hadoop.hive.metastore.newms;


import iie.metastore.MetaStoreClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.hive.metastore.msg.MSGFactory.DDLMsg;
import org.apache.hadoop.hive.metastore.msg.MSGType;
import org.apache.thrift.TException;

import redis.clients.jedis.exceptions.JedisConnectionException;

public class MsgProcessing {

	private MetaStoreClient msClient;
	private NewMSConf conf;
	private CacheStore cs;
	public MsgProcessing(NewMSConf conf) {
		this.conf = conf;
		try {
			msClient = new MetaStoreClient(conf.getMshost(), conf.getMsport());
			cs = new CacheStore(conf);
      Database localdb = msClient.client.get_local_attribution();
      cs.writeObject(ObjectType.DATABASE, localdb.getName(), localdb);
      this.conf.setLocalDbName(localdb.getName());
      List<Database> dbs = msClient.client.get_all_attributions();
      for(Database db : dbs)
      {
        cs.writeObject(ObjectType.DATABASE, db.getName(), db);
      }
      
      List<Device> dl = msClient.client.listDevice();
      for(Device de : dl)
      	cs.writeObject(ObjectType.DEVICE, de.getDevid(), de);
      long fid = msClient.client.getMaxFid();
      RawStoreImp.setFID(fid);
		} catch (MetaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JedisConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	public void handleMsg(DDLMsg msg) throws JedisConnectionException, IOException, NoSuchObjectException, TException, ClassNotFoundException {
		
		int eventid = (int) msg.getEvent_id();
		switch (eventid) {
			case MSGType.MSG_NEW_DATABESE: 
			case MSGType.MSG_ALTER_DATABESE:
			case MSGType.MSG_ALTER_DATABESE_PARAM:
			{
				String dbName = (String) msg.getMsg_data().get("db_name");
				try{
					Database db = msClient.client.getDatabase(dbName);
					cs.writeObject(ObjectType.DATABASE, dbName, db);
				}catch(NoSuchObjectException e ){
					e.printStackTrace();
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
				if(CacheStore.getTableHm().remove(oldKey) != null){
					cs.removeObject(ObjectType.TABLE, oldKey);
				}
				Table tbl = msClient.client.getTable(dbName, tableName);
				TableImage ti = TableImage.generateTableImage(tbl);
				CacheStore.getTableHm().put(newKey, tbl);
				cs.writeObject(ObjectType.TABLE, newKey, ti);
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
				Table tbl = msClient.client.getTable(dbName, tableName);
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
							List<NodeGroup> ngs = msClient.client.listNodeGroups(ngNames);
							NodeGroup ng = ngs.get(0);
							NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
							CacheStore.getNodeGroupHm().put(ng.getNode_group_name(), ng);
							cs.writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
							for(int j = 0; j<ngi.getNodeKeys().size();j++){
								if(!CacheStore.getNodeHm().containsKey(ngi.getNodeKeys().get(j))){
									Node node = msClient.client.get_node(ngi.getNodeKeys().get(j));
									cs.writeObject(ObjectType.NODE, ngi.getNodeKeys().get(j), node);
								}
							}
						}
					}
				}
				break;
			}
			case MSGType.MSG_DROP_TABLE:
			{
				String dbName = (String) msg.getMsg_data().get("db_name");
				String tableName = (String) msg.getMsg_data().get("table_name");
				String key = dbName + "." + tableName;
				if(CacheStore.getTableHm().remove(key) != null){
					cs.removeObject(ObjectType.TABLE, key);
				}
				break;
			}
			
			case MSGType.MSG_REP_FILE_CHANGE:
			case MSGType.MSG_STA_FILE_CHANGE:
			case MSGType.MSG_REP_FILE_ONOFF:
			case MSGType.MSG_CREATE_FILE:
			{
				Object id = msg.getMsg_data().get("f_id");
				if(id == null)		//
				{
					break;
				}
				long fid = Long.parseLong(id.toString());
				SFile sf = null;
				try{
					sf = msClient.client.get_file_by_id(fid);
				}catch(FileOperationException e)
				{
					//Can not find SFile by FID ...
//					System.out.println(e.getMessage());
					e.printStackTrace();
					if(sf == null)
						break;
				}
				SFileImage sfi = SFileImage.generateSFileImage(sf);
				CacheStore.getsFileHm().put(fid+"", sf);
				cs.writeObject(ObjectType.SFILE, fid+"", sfi);
				for(int i = 0;i<sfi.getSflkeys().size();i++)
				{
					cs.writeObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i), sf.getLocations().get(i));
				}
				
				break;
			}
			//在删除文件时，会在之前发几个1307,然后才是4002
			case MSGType.MSG_DEL_FILE:
			{
				long fid = Long.parseLong(msg.getMsg_data().get("f_id").toString());
				SFile sf = (SFile)cs.readObject(ObjectType.SFILE, fid+"");
				if(sf != null)
				{
					if(sf.getLocations() != null)
					{
						for(SFileLocation sfl : sf.getLocations())
						{
							String key = SFileImage.generateSflkey(sfl.getLocation(),sfl.getDevid());
							cs.removeObject(ObjectType.SFILELOCATION, key);
						}
					}
					cs.removeObject(ObjectType.SFILE, fid+"");
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
				if(dbName == null || tblName == null || indexName == null)
					break;
				Index ind = msClient.client.getIndex(dbName, tblName, indexName);
				String key = dbName + "." + tblName + "." + indexName;
				cs.writeObject(ObjectType.INDEX, key, ind);
				break;
			}
			case MSGType.MSG_DEL_INDEX:
			{
				String dbName = (String)msg.getMsg_data().get("db_name");
				String tblName = (String)msg.getMsg_data().get("table_name");
				String indexName = (String)msg.getMsg_data().get("index_name");
				//Index ind = msClient.client.getIndex(dbName, tblName, indexName);
				String key = dbName + "." + tblName + "." + indexName;
				if(CacheStore.getIndexHm().remove(key) != null)
					cs.removeObject(ObjectType.INDEX, key);
				break;
			}
			
			case MSGType.MSG_NEW_NODE:
			case MSGType.MSG_FAIL_NODE:
			case MSGType.MSG_BACK_NODE:
			{
				String nodename = (String)msg.getMsg_data().get("node_name");
				Node node = msClient.client.get_node(nodename);
				cs.writeObject(ObjectType.NODE, nodename, node);
				break;
			}
			
			case MSGType.MSG_DEL_NODE:
			{
				String nodename = (String)msg.getMsg_data().get("node_name");
				cs.removeObject(ObjectType.NODE, nodename);
				break;
			}
			
			case MSGType.MSG_CREATE_SCHEMA:
			case MSGType.MSG_MODIFY_SCHEMA_DEL_COL:
			case MSGType.MSG_MODIFY_SCHEMA_ADD_COL:
			case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_NAME:
			case MSGType.MSG_MODIFY_SCHEMA_ALT_COL_TYPE:
			case MSGType.MSG_MODIFY_SCHEMA_PARAM:
			{
				String schema_name = (String)msg.getMsg_data().get("schema_name");
				try{
					GlobalSchema s = msClient.client.getSchemaByName(schema_name);
					cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, s);
				}catch(NoSuchObjectException e){
					e.printStackTrace();
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
					cs.removeObject(ObjectType.GLOBALSCHEMA, old_schema_name);
					cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, gs);
				}
				else{
					try{
						GlobalSchema ngs = msClient.client.getSchemaByName(schema_name);
						cs.writeObject(ObjectType.GLOBALSCHEMA, schema_name, ngs);
					}
					catch(NoSuchObjectException e)
					{
						e.printStackTrace();
					}
				
				}
			}
			
			case MSGType.MSG_DEL_SCHEMA:
			{
				String schema_name = (String)msg.getMsg_data().get("schema_name");
				cs.removeObject(ObjectType.GLOBALSCHEMA, schema_name);
				break;
			}
			
			case MSGType.MSG_NEW_NODEGROUP:
			{
				String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
				List<String> ngNames = new ArrayList<String>();
				ngNames.add(nodeGroupName);
				List<NodeGroup> ngs = msClient.client.listNodeGroups(ngNames);
				if(ngs == null || ngs.size() == 0)
					break;
				NodeGroup ng = ngs.get(0);
				NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
				CacheStore.getNodeGroupHm().put(ng.getNode_group_name(), ng);
				cs.writeObject(ObjectType.NODEGROUP, ng.getNode_group_name(), ngi);
				for(int i = 0; i<ngi.getNodeKeys().size();i++){
					if(!CacheStore.getNodeHm().containsKey(ngi.getNodeKeys().get(i))){
						Node node = msClient.client.get_node(ngi.getNodeKeys().get(i));
						cs.writeObject(ObjectType.NODE, ngi.getNodeKeys().get(i), node);
					}
				}
				break;
			}
			//case MSGType.MSG_ALTER_NODEGROUP:
			case MSGType.MSG_DEL_NODEGROUP:{
				String nodeGroupName = (String)msg.getMsg_data().get("nodegroup_name");
				cs.removeObject(ObjectType.NODEGROUP, nodeGroupName);
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
	
		    case MSGType.MSG_REVOKE_GLOBAL:
		    case MSGType.MSG_REVOKE_DB:
		    case MSGType.MSG_REVOKE_TABLE:
		    case MSGType.MSG_REVOKE_PARTITION:
		    case MSGType.MSG_REVOKE_SCHEMA:
		    case MSGType.MSG_REVOKE_PARTITION_COLUMN:
		    case MSGType.MSG_REVOKE_TABLE_COLUMN:
		    {
//		    	msClient.client.
		    	break;
		    }
			default:
			{
				System.out.println("unhandled msg : "+msg.toJson());
				break;
			}
		}
	}
}