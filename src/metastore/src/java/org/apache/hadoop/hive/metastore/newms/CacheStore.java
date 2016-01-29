package org.apache.hadoop.hive.metastore.newms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.GlobalSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import com.alibaba.fastjson.JSON;

public class CacheStore {
  private final RedisFactory rf;
  private String findFilesCursor = "0";
  private static boolean initialized = false;
  private static String sha = null;
  private static final Log LOG = LogFactory.getLog(CacheStore.class);

  private static ConcurrentHashMap<String, Database> databaseHm = new ConcurrentHashMap<String, Database>();
  private static ConcurrentHashMap<String, PrivilegeBag> privilegeBagHm = new ConcurrentHashMap<String, PrivilegeBag>();
  private static ConcurrentHashMap<String, Partition> partitionHm = new ConcurrentHashMap<String, Partition>();
  private static ConcurrentHashMap<String, Node> nodeHm = new ConcurrentHashMap<String, Node>();
  private static ConcurrentHashMap<String, NodeGroup> nodeGroupHm = new ConcurrentHashMap<String, NodeGroup>();
  private static ConcurrentHashMap<String, GlobalSchema> globalSchemaHm = new ConcurrentHashMap<String, GlobalSchema>();
  private static ConcurrentHashMap<String, Table> tableHm = new ConcurrentHashMap<String, Table>();
  private static ConcurrentHashMap<String, Index> indexHm = new ConcurrentHashMap<String, Index>();
  private static ConcurrentHashMap<String, Device> deviceHm = new ConcurrentHashMap<String, Device>();
  private static TimeLimitedCacheMap sFileHm = new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);
  private static TimeLimitedCacheMap sflHm = new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);

  public CacheStore() throws IOException {
    rf = new RedisFactory();

    initialize();
  }

  public void close() {
    if (rf != null) {
      rf.destroy();
    }
  }

  public Jedis refreshJedis() throws IOException {
    Jedis j = rf.getDefaultInstance();
    if (j == null) {
      throw new IOException("Invalid connection to Redis server or config error.");
    }
    return j;
  }

  //初始化与类相关的静态属性，为的是这些属性只初始化一次
  private void initialize() throws IOException {
    Exception exp = null;
    int err = 0;

    synchronized (this.getClass()) {
      if (!initialized) {
        Jedis jedis = refreshJedis();

        try {
          //向sorted set中加入一个元素，score是从0开始的整数(listtablefiles方法的from和to，zrange方法中的参数都是从0开始的，正好一致)，自动递增
          //如果元素已经存在，不做任何操作

          String script = "local score = redis.call('zscore',KEYS[1],ARGV[1]);"
              + "if not score then "        //lua里只有false和nil被认为是逻辑的非
              + "local size = redis.call('zcard',KEYS[1]);"
              + "return redis.call('zadd',KEYS[1],size,ARGV[1]); end";
          sha = jedis.scriptLoad(script);

          //每次系统启动时，从redis中读取已经持久化的对象到内存缓存中(SFile和SFileLocation除外)
          if (!new HiveConf().getBoolVar(ConfVars.NEWMS_IS_GET_ALL_OBJECTS)) {
	          long start = System.currentTimeMillis();
	          readAll(ObjectType.DATABASE);
	          readAll(ObjectType.GLOBALSCHEMA);
	          readAll(ObjectType.INDEX);
	          readAll(ObjectType.NODE);
	          readAll(ObjectType.NODEGROUP);
	          readAll(ObjectType.PARTITION);
	          readAll(ObjectType.PRIVILEGE);
	          readAll(ObjectType.TABLE);
	          readAll(ObjectType.DEVICE);
	          readAll(ObjectType.SFILE);
	          readAll(ObjectType.SFILELOCATION);
	          long end = System.currentTimeMillis();
	          LOG.info("loading objects from redis into cache takes " + (end - start) + " ms");
          }
        } catch (JedisException e) {
          exp = e;
          LOG.error(e,e);
          err = -1;
        } catch (IOException e) {
          exp = e;
          LOG.error(e,e);
        } catch (ClassNotFoundException e) {
          exp = e;
          LOG.error(e,e);
        } finally {
          if (err < 0) {
            RedisFactory.putBrokenInstance(jedis);
          } else {
            RedisFactory.putInstance(jedis);
          }
        }
      }
      if (exp != null) {
        throw new IOException("CacheStore Init: " + exp.getMessage());
      }
      initialized = true;
    }
  }

  private void cleanHashMap() {
    databaseHm.clear();
    privilegeBagHm.clear();
    partitionHm.clear();
    nodeHm.clear();
    nodeGroupHm.clear();
    globalSchemaHm.clear();
    tableHm.clear();
    indexHm.clear();
    deviceHm.clear();
    // ignore sFileHm and sflHm
  }

  public boolean reloadHashMap() {
    LOG.info("Flushing memory cache ...");
    cleanHashMap();
    LOG.info("Begin reloading objects ...");
    long start = System.currentTimeMillis();
    try {
      readAll(ObjectType.DATABASE);
      readAll(ObjectType.GLOBALSCHEMA);
      readAll(ObjectType.INDEX);
      readAll(ObjectType.NODE);
      readAll(ObjectType.NODEGROUP);
      readAll(ObjectType.PARTITION);
      readAll(ObjectType.PRIVILEGE);
      readAll(ObjectType.TABLE);
      readAll(ObjectType.DEVICE);
      readAll(ObjectType.SFILE);
      readAll(ObjectType.SFILELOCATION);
    } catch (ClassNotFoundException e) {
      LOG.error(e, e);
    } catch (JedisException e) {
      LOG.error(e, e);
    } catch (IOException e) {
      LOG.error(e, e);
    }
    long end = System.currentTimeMillis();
	  LOG.info("Reloading objects from redis into cache takes " + (end - start) + " ms.");

	  return true;
  }

  public void writeObject(ObjectType.TypeDesc key, String field, Object o,
      boolean updateIndex) throws JedisException, IOException {
    int err = 0;

    switch (key.getId()) {
    case SFILE: {
      	SFile sf = (SFile) o;
      	sFileHm.put(sf.getFid() + "", sf);
      	SFileImage sfi = SFileImage.generateSFileImage(sf);
      	//redis中存入的是sfi
      	o = sfi;
      	Jedis jedis = refreshJedis();
      	try {
      	  Pipeline p = jedis.pipelined();
	      	// update index for listtablefiles
	      	if (sfi.getDbName() != null && sfi.getTableName() != null) {
	      		String k = generateLtfKey(sfi.getTableName(), sfi.getDbName());
	      		p.evalsha(sha, 1, new String[]{k, sfi.getFid() + ""});
	      	}
	      	// update index for filtertablefiles
	      	if (updateIndex) {
	      		String k2 = generateFtlKey(sfi.getValues());
	      		p.sadd(k2, sfi.getFid() + "");
	      	}
	      	// update index for listFilesByDegist
	      	if (updateIndex) {
	      		String k = generateLfbdKey(sfi.getDigest());
	      		p.sadd(k, sfi.getFid() + "");
	      	}
	      	// update index for findFiles
	      	if (updateIndex) {
	      		String k = generateSfStatKey(sfi.getStore_status());
	      		p.sadd(k, sfi.getFid() + "");
	      	}
	      	p.sync();
      	} catch (JedisException e){
      	  err = -1;
          throw e;
        } finally {
          if (err < 0) {
            RedisFactory.putBrokenInstance(jedis);
          } else {
            RedisFactory.putInstance(jedis);
          }
        }
      	for (int i = 0; i < sfi.getSflkeys().size(); i++) {
					writeObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i),
					    sf.getLocations().get(i), updateIndex);
				}
      	break;
      }

    case SFILELOCATION: {
      Jedis jedis = refreshJedis();

      sflHm.put(field, (SFileLocation)o);

      // index for getSFileLocations(int status)
      SFileLocation sfl = (SFileLocation)o;
      try {
      	Pipeline p = jedis.pipelined();
    		p.sadd(generateSflStatKey(sfl.getVisit_status()), field);
    		p.sadd(generateSflDevKey(sfl.getDevid()), field);
    		p.sync();
      } catch(JedisException e) {
        err = -1;
        throw e;
      } finally {
        if (err < 0) {
          RedisFactory.putBrokenInstance(jedis);
        } else {
          RedisFactory.putInstance(jedis);
        }
      }
      break;
    }
    case DATABASE: {
      databaseHm.put(field, (Database)o);
      break;
    }
    case TABLE: {
      tableHm.put(field, (Table)o);
      // table里面有nodegroup，但是并不每次写入一个table时都把它的nodegroup都写入一遍，
      // table和nodegroup之间的关系通过ngkeys关联
      // FIXME 如果某个nodegroup被删除了，table里与它关联的键不会被删除。不过读取table，按ngkeys读取nodegroup时，
      // 如果结果是null就跳过，因此最终可以在table里体现出nodegroup被删除，只是冗余了一个键。nodegroup里的node也是这样处理的。
      Table tbl = (Table)o;
      TableImage ti = TableImage.generateTableImage(tbl);
      o = ti;
      break;
    }
    case INDEX: {
      indexHm.put(field, (Index)o);
      break;
    }
    case NODE: {
      nodeHm.put(field, (Node)o);
      break;
    }
    case NODEGROUP: {
      nodeGroupHm.put(field, (NodeGroup)o);

      NodeGroup ng = (NodeGroup)o;
      NodeGroupImage ngi = NodeGroupImage.generateNodeGroupImage(ng);
      o = ngi;
      break;
    }
    case GLOBALSCHEMA: {
      globalSchemaHm.put(field, (GlobalSchema)o);
      break;
    }
    case PRIVILEGE: {
      privilegeBagHm.put(field, (PrivilegeBag)o);
      break;
    }
    case PARTITION: {
      partitionHm.put(field, (Partition)o);
      break;
    }
    case DEVICE: {
      deviceHm.put(field, (Device)o);
      break;
    }
    }

    /*
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(o);
      jedis.hset(key.getName().getBytes(), field.getBytes(), baos.toByteArray());
     */
    String s = null;
    Jedis jedis = refreshJedis();

    try {
      s = JSON.toJSONString(o);
    } catch (Exception e) {
      LOG.error(e,e);
      throw new IOException(e.getMessage());
    }
    try {
      jedis.hset(key.getName(), field, s);
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public Object readObject(ObjectType.TypeDesc key, String field)throws JedisConnectionException, IOException,ClassNotFoundException {
  	return readObject(key, field, false);
  }

  private Object readObjectFromRedis(ObjectType.TypeDesc key, String field) throws JedisException, IOException, ClassNotFoundException {
    Object o = null;
    String js = null;
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      js = jedis.hget(key.getName(), field);
    } catch (JedisException e){
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }

    if (js == null) {
      return null;
    }
    try {
      o = JSON.parseObject(js, key.getCla());
    } catch (Exception e){
      LOG.error(e, e);
      throw new IOException(e.getMessage());
    }

    switch (key.getId()) {
    case SFILE: {
      //SFile 要特殊处理
      SFileImage sfi = (SFileImage) o;
      List<SFileLocation> locations = new ArrayList<SFileLocation>();
      for (int i = 0; i < sfi.getSflkeys().size(); i++) {
        SFileLocation sfl = (SFileLocation) readObject(ObjectType.SFILELOCATION, sfi.getSflkeys().get(i));
        if (sfl != null) {
          locations.add(sfl);
        }
      }
      SFile sf =  new SFile(sfi.getFid(), sfi.getDbName(), sfi.getTableName(), sfi.getStore_status(), sfi.getRep_nr(),
          sfi.getDigest(), sfi.getRecord_nr(), sfi.getAll_record_nr(), locations, sfi.getLength(),
          sfi.getRef_files(), sfi.getValues(), sfi.getLoad_status());
      sFileHm.put(field, sf);
      o = sf == null ? null : sf.deepCopy();
      break;
    }
    case TABLE: {
      // table
      TableImage ti = (TableImage) o;
      List<NodeGroup> ngs = new ArrayList<NodeGroup>();
      for (int i = 0; i < ti.getNgKeys().size(); i++) {
        NodeGroup ng = (NodeGroup) readObject(ObjectType.NODEGROUP, ti.getNgKeys().get(i));
        if (ng != null) {
          ngs.add(ng);
        }
      }
      Table t = new Table(ti.getTableName(), ti.getDbName(), ti.getSchemaName(),
          ti.getOwner(), ti.getCreateTime(), ti.getLastAccessTime(), ti.getRetention(),
          ti.getSd(), ti.getPartitionKeys(), ti.getParameters(), ti.getViewOriginalText(),
          ti.getViewExpandedText(), ti.getTableType(), ngs, ti.getFileSplitKeys());
      tableHm.put(field, t);
      o = t == null ? null : t.deepCopy();
      break;
    }
    case NODEGROUP: {
      // nodegroup
      NodeGroupImage ngi = (NodeGroupImage) o;
      Set<Node> nodes = new HashSet<Node>();
      for (String s : ngi.getNodeKeys()) {
        Node n = (Node) readObject(ObjectType.NODE, s);
        if (n != null) {
          nodes.add(n);
        }
      }
      NodeGroup ng = new NodeGroup(ngi.getNode_group_name(), ngi.getComment(), ngi.getStatus(), nodes);
      nodeGroupHm.put(field, ng);
      o = ng == null ? null : ng.deepCopy();
      break;
    }
    case DATABASE: {
      Database db = (Database) o;
      databaseHm.put(field, db);
      o = db == null ? null : db.deepCopy();
      break;
    }
    case SFILELOCATION: {
      SFileLocation sfl = (SFileLocation)o;
      sflHm.put(field, sfl);
      o = sfl == null ? null : sfl.deepCopy();
      break;
    }
    case INDEX: {
      Index idx = (Index)o;
      indexHm.put(field, idx);
      o = idx == null ? null : idx.deepCopy();
      break;
    }
    case NODE: {
      Node n = (Node)o;
      nodeHm.put(field, n);
      o = n == null ? null : n.deepCopy();
      break;
    }
    case GLOBALSCHEMA: {
      GlobalSchema gs = (GlobalSchema)o;
      globalSchemaHm.put(field, gs);
      o = gs == null ? null : gs.deepCopy();
      break;
    }
    case PRIVILEGE: {
      PrivilegeBag pb = (PrivilegeBag)o;
      privilegeBagHm.put(field, pb);
      o = pb == null ? null : pb.deepCopy();
      break;
    }
    case PARTITION: {
      Partition p = (Partition)o;
      partitionHm.put(field, p);
      o = p == null ? null : p.deepCopy();
      break;
    }
    case DEVICE: {
      Device d = (Device)o;
      deviceHm.put(field, d);
      o = d == null ? null : d.deepCopy();
      break;
    }
    }

    return o;
  }

  /**
   * 有些时候为了保持redis中存储的对象和内存缓存中的一致，需要刷新内存缓存，可以通过一次读取操作完成
   * @param key
   * @param field
   * @param isRefreshCache	是否通过这次读操作来刷新内存缓存，true的话就忽略缓存，直接从redis中读取对象，然后put到缓存中，达到刷新缓存的效果
   * @return
   * @throws JedisConnectionException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public Object readObject(ObjectType.TypeDesc key, String field, boolean isRefreshCache) throws JedisException, IOException, ClassNotFoundException {
    Object o = null;

    if (isRefreshCache) {
      return readObjectFromRedis(key, field);
    }

    switch (key.getId()) {
    case SFILE: {
      SFile temp = (SFile)sFileHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case SFILELOCATION: {
      SFileLocation temp = (SFileLocation)sflHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case DATABASE: {
      Database temp = databaseHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case TABLE: {
      Table temp  = tableHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case INDEX: {
      Index temp = indexHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case NODE: {
      Node temp = nodeHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case NODEGROUP: {
      NodeGroup temp = nodeGroupHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case GLOBALSCHEMA: {
      GlobalSchema temp = globalSchemaHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case PRIVILEGE: {
      PrivilegeBag temp = privilegeBagHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case PARTITION: {
      Partition temp = partitionHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    case DEVICE: {
      Device temp = deviceHm.get(field);
      o = temp == null ? null : temp.deepCopy();
      break;
    }
    }

    if (o == null) {
      return readObjectFromRedis(key, field);
    } else {
      return o;
    }
  }

  public void removeObject(ObjectType.TypeDesc key, String field) throws JedisException, IOException, ClassNotFoundException {
    int err = 0;

    switch (key.getId()) {
    case SFILE: {
      //删除一个sfile时要把预先建立的一些信息也删掉
      SFile sf = (SFile) readObject(key, field);
      if (sf != null) {
        //删除sfile也要删除sfilelocation
        if (sf.getLocations() != null) {
          for (SFileLocation sfl : sf.getLocations()) {
            String k = SFileImage.generateSflkey(sfl.getLocation(), sfl.getDevid());
            removeObject(ObjectType.SFILELOCATION, k);
          }
        }
        Jedis jedis = refreshJedis();
        try {
          Pipeline p = jedis.pipelined();
          p.srem(generateLfbdKey(sf.getDigest()), field);
          p.srem(generateFtlKey(sf.getValues()), field);
          p.zrem(generateLtfKey(sf.getTableName(), sf.getDbName()), field);
          p.srem(generateSfStatKey(sf.getStore_status()), field);
          LOG.debug("in CacheStore remove:srem(generateSfStatKey(sf.getStore_status()), field);" +
              generateSfStatKey(sf.getStore_status()) + "," + field);
          p.hdel(key.getName(), field);
          p.sync();
        } catch (JedisException e){
          err = -1;
          throw e;
        } finally {
          if (err < 0) {
            RedisFactory.putBrokenInstance(jedis);
          } else {
            RedisFactory.putInstance(jedis);
          }
        }
      }
      break;
    }
    case SFILELOCATION : {
    	SFileLocation sfl = (SFileLocation) readObject(key, field);
      if (sfl != null) {
        Jedis jedis = refreshJedis();
        try {
          Pipeline p = jedis.pipelined();
          p.srem(generateSflStatKey(sfl.getVisit_status()), field);
          p.srem(generateSflDevKey(sfl.getDevid()), field);
          p.hdel(key.getName(), field);
          p.sync();
        } catch (JedisException e) {
          err = -1;
          throw e;
        } finally {
          if (err < 0) {
            RedisFactory.putBrokenInstance(jedis);
          } else {
            RedisFactory.putInstance(jedis);
          }
        }
      }
      break;
    }
    default: {
    	Jedis jedis = refreshJedis();
      try {
        jedis.hdel(key.getName(), field);
      } catch (JedisException e) {
        err = -1;
        throw e;
      } finally {
        if (err < 0) {
          RedisFactory.putBrokenInstance(jedis);
        } else {
          RedisFactory.putInstance(jedis);
        }
      }
    }
    }

    switch (key.getId()) {
    case SFILE:
    	sFileHm.remove(field);
    	break;
    case DATABASE:
      databaseHm.remove(field);
      break;
    case TABLE:
      tableHm.remove(field);
      break;
    case SFILELOCATION:
      sflHm.remove(field);
      break;
    case INDEX:
      indexHm.remove(field);
      break;
    case NODE:
      nodeHm.remove(field);
      break;
    case NODEGROUP:
      nodeGroupHm.remove(field);
      break;
    case GLOBALSCHEMA:
      globalSchemaHm.remove(field);
      break;
    case PRIVILEGE:
      privilegeBagHm.remove(field);
      break;
    case PARTITION:
      partitionHm.remove(field);
      break;
    case DEVICE:
      deviceHm.remove(field);
      break;
    }

  }

  private void readAll(ObjectType.TypeDesc key) throws JedisException, IOException, ClassNotFoundException {
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      Set<String> fields = jedis.hkeys(key.getName());
      int loaderr = 0;

      if (fields != null) {
        if (key.equals(ObjectType.SFILE) || key.equals(ObjectType.SFILELOCATION)) {
          LOG.info("Find " + fields.size() + " " + key.getName() + " in redis, do NOT load.");
          return;
        }
        LOG.info("Load " + fields.size() + " " + key.getName() + " from redis into cache.");
        for (String field : fields) {
          try {
            readObject(key, field, true);
          } catch (Exception e) {
            LOG.error(e, e);
            loaderr++;
          }
        }
        LOG.info("Load " + (fields.size() - loaderr) + " " + key.getName() + " success.");
      }
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public void updateCache(ObjectType.TypeDesc key) throws JedisException, IOException, ClassNotFoundException {
  	this.readAll(key);
  }

  public void updateCache(ObjectType.TypeDesc key, String field) throws JedisException, IOException, ClassNotFoundException {
  	this.readObject(key, field, true);
  }

  public List<Long> listTableFiles(String dbName, String tabName, int from, int to) throws JedisException, IOException {
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      String k = generateLtfKey(tabName, dbName);
      Set<String> ss = jedis.zrange(k, from, to);
      List<Long> ids = new ArrayList<Long>();
      if (ss != null) {
        for (String id : ss) {
          ids.add(Long.parseLong(id));
        }
      }
      return ids;
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values) throws JedisException, IOException {
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      String k = generateFtlKey(values);
      Set<String> mem = jedis.smembers(k);
      List<SFile> rls = new ArrayList<SFile>();
      if (mem != null) {
        for (String id : mem) {
          SFile f = null;
          try {
            f = (SFile) readObject(ObjectType.SFILE, id);
            if (f != null) {
              if (f.getTableName() != null && f.getDbName() != null) {
                if (f.getTableName().equals(tabName) && f.getDbName().equals(dbName)) {
                  if (f.getValues() != null && values != null && f.getValues().containsAll(values)) {
                    rls.add(f);
                  } else if (f.getValues() != null && f.getValuesSize() == 0) {
                    if (values == null || values.size() == 0) {
                      rls.add(f);
                    }
                  } else if (values != null && values.size() == 0) {
                    if (f.getValues() == null || f.getValuesSize() == 0) {
                      rls.add(f);
                    }
                  } else if (values == null && f.getValues() == null) {
                    rls.add(f);
                  }
                }
              } else {
                rls.add(f);
              }
            }
          } catch (Exception e) {
            LOG.error(e,e);
          }
        }
      }
      return rls;
    } catch (JedisException e){
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public List<Long> listFilesByDegist(String degist) throws JedisException, IOException {
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      String k = generateLfbdKey(degist);
      Set<String> ids = jedis.smembers(k);
      List<Long> rl = new ArrayList<Long>();
      if (ids != null) {
        for (String s : ids) {
          rl.add(Long.parseLong(s));
        }
      }
      return rl;
    } catch(JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public List<String> get_all_tables(String dbname) throws JedisException, IOException {
    Jedis jedis = refreshJedis();
    int err = 0;

    try {
      Set<String> k = jedis.hkeys(ObjectType.TABLE.getName());
      List<String> rl = new ArrayList<String>();
      for (String dt : k) {
        if (dt.startsWith(dbname)) {
          rl.add(dt.split("\\.")[1]);
        }
      }
      return rl;
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  /**
   * 获取database的名字
   * @param pattern 匹配规则按照redis实现方式，应该是与old metastore的不一致
   * @return
   */
  public List<String> getDatabases(String pattern) throws JedisException, IOException {
    Jedis jedis = refreshJedis();
    int err = 0;

  	try {
  	  ScanParams sp = new ScanParams();
  	  sp.count(1000);
  	  sp.match(pattern);
  	  List<String> rl = new ArrayList<String>();
  	  String cursor = "0";
  	  do {
  	    ScanResult<Map.Entry<String, String>> result = jedis.hscan(ObjectType.DATABASE.getName(), cursor, sp);
  	    if (result == null) {
  	      break;
  	    }
  	    cursor = result.getStringCursor();
  	    for (Map.Entry<String, String> en : result.getResult()) {
  	      rl.add(en.getKey());
  	    }
  	  } while (!cursor.equals("0"));
  	  return rl;
  	} catch (JedisException e) {
  	  err = -1;
  	  throw e;
  	} finally {
  	  if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
  	}
  }

  public List<SFileLocation> getSFileLocations(int status) throws JedisException, IOException, ClassNotFoundException {
  	long start = System.currentTimeMillis();
  	Jedis jedis = refreshJedis();
  	List<SFileLocation> sfll = new ArrayList<SFileLocation>();
  	int err = 0;

    try {
      ScanResult<String> re = null;
      ScanParams sp = new ScanParams();
      sp.count(5000);
  		String cursor = "0";
  		do {
  		  re = jedis.sscan(generateSflStatKey(status), cursor, sp);
  		  if (re == null) {
  		    break;
  		  }
  		  cursor = re.getStringCursor();
  		  //LOG.debug(cursor +"  "+re.getResult().size());
  		  for (String en : re.getResult()) {
  		    SFileLocation sfl = (SFileLocation) this.readObject(ObjectType.SFILELOCATION, en);
  		    sfll.add(sfl);
  		  }
  		} while (!cursor.equals("0"));
  		LOG.debug("getSFileLocations() consume " + (System.currentTimeMillis() - start) + " ms");
      return sfll;
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

	public void findVoidFiles(List<SFile> voidFiles) throws JedisException, IOException, ClassNotFoundException {
		long start = System.currentTimeMillis();
  	Jedis jedis = refreshJedis();
  	int err = 0;

  	try {
  	  ScanResult<String> re = null;
  	  ScanParams sp = new ScanParams();
  	  sp.count(5000);
  	  String cursor = "0";
  	  do {
  	    re = jedis.sscan(generateSfStatKey(MetaStoreConst.MFileStoreStatus.INCREATE), cursor, sp);
  	    cursor = re.getStringCursor();
  	    for (String en : re.getResult()) {
  	      SFile sf = (SFile)this.readObject(ObjectType.SFILE, en);
  	      boolean ok = false;
  	      for (SFileLocation sfl : sf.getLocations()) {
  	        if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE ||
  	            sfl.getUpdate_time() > (System.currentTimeMillis() - (600 * 1000))) {
  	          ok = true;
  	          break;
  	        }
  	      }
  	      if (!ok) {
  	        voidFiles.add(sf);
  	      }
  	    }
  	  } while (!cursor.equals("0"));
  		LOG.info("findVoidFiles() consume " + (System.currentTimeMillis() - start) + " ms");
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
	}

	public void findFiles(List<SFile> underReplicated, List<SFile> overReplicated, List<SFile> lingering,
      long from, long to) throws JedisException, IOException, ClassNotFoundException, MetaException {
		long start = System.currentTimeMillis();
		long node_nr = CacheStore.getNodeHm().size();
		List<SFile> temp = new LinkedList<SFile>();
		int err = 0;

    if (underReplicated == null || overReplicated == null || lingering == null) {
      throw new MetaException("Invalid input List<SFile> collection. IS NULL");
    }

    if (from < 0 || from > to) {
    	LOG.debug("findFiles() argument invalid: from: " + from + ", to:" + to);
    	return;
    }

  	Jedis jedis = refreshJedis();
    try {
    	ScanParams sp = new ScanParams();
    	sp.count((int) (to - from));
    	ScanResult<Entry<String, String>> re = jedis.hscan(ObjectType.SFILE.getName(), findFilesCursor, sp);

    	if (re != null) {
    	  findFilesCursor = re.getStringCursor();
    	  for (Entry<String, String> en : re.getResult()) {
    	    try {
    	      SFile sf = (SFile) this.readObject(ObjectType.SFILE, en.getKey());
    	      if (sf == null) {
    	        LOG.debug("in CacheStore findFiles(), SFile("+ en.getKey() + ") is null, bad...");
    	      } else {
    	        temp.add(sf);
    	      }
    	    } catch (Exception e) {
    	      // ignore any error
    	    }
    	  }
    	}
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
    LOG.info("findFiles() consume " + (System.currentTimeMillis() - start) + " ms");

    for (SFile m : temp) {
    	List<SFileLocation> l = m.getLocations();

    	if (l == null) {
    		LOG.warn("sfilelocation is null in fid " + m.getFid());
    		continue;
    	}

    	//ztt 若文件处于增量复制状态服务重启了,放到under中,在diskmanager中再做处理
    	 if (m.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
    	   underReplicated.add(m);
    	 }

      // find under replicated files
      if (m.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
          m.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
        int nr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            nr++;
          }
        }
        if (m.getRep_nr() > nr) {
          try {
            underReplicated.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
            LOG.error(e,e);
          }
        }
      }
      // find over  replicated files
      if (m.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        int onlinenr = 0;
        int onrepnr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            onlinenr++;
          }
          if(fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONREP) {
            onrepnr ++;
          }
        }
        if (m.getRep_nr() < onlinenr + onrepnr && onlinenr > 0) {
          try {
            overReplicated.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
          	LOG.error(e,e);
          }
        }
      }
      // find lingering files
      if (m.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
        lingering.add(m);
      }
      if (m.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        int offnr = 0, onnr = 0, suspnr = 0, onrepnr = 0;

        for (SFileLocation fl : l) {
          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            onnr++;
          } else if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE) {
            offnr++;
          } else if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.SUSPECT) {
            suspnr++;
          } else if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONREP) {
            onrepnr++;
          }
        }
        // NOTE: logic is ->
        // if online nr >= requested nr, we try to remove any offline/suspect SFLs,
        // otherwise, if online + offline + suspect >= node_nr, try to remove offline SFLs
        if ((m.getRep_nr() <= onnr && (offnr > 0 || suspnr > 0 || onrepnr > 0)) ||
            (onnr + offnr + suspnr + onrepnr >= node_nr && offnr > 0)) {
          try {
            lingering.add(m);
          } catch (javax.jdo.JDOObjectNotFoundException e) {
            // it means the file slips ...
          	LOG.error(e,e);
          }
        }
      }
    }
	}

	public List<SFileLocation> getSFileLocations(String devid, long curts, long timeout) throws IOException {
		Jedis jedis = refreshJedis();
		List<SFileLocation> sfll = new LinkedList<SFileLocation>();
		int err = 0;
    try{
      ScanResult<String> re = null;
      ScanParams sp = new ScanParams();
      sp.count(5000);
  		String cursor = "0";
  		do{
  			re = jedis.sscan(generateSflDevKey(devid), cursor,sp);
  			cursor = re.getStringCursor();
  			for(String en : re.getResult())
  			{
  		     SFileLocation sfl = null;
					try {
						sfl = (SFileLocation)this.readObject(ObjectType.SFILELOCATION, en);
						if(sfl == null)
						{
							//it means sflDevKey in redis is inconsistent, bad....
							LOG.warn("key("+en+") read from SflDevKey("+generateSflDevKey(devid)+") refers to a non-exist SFileLocation, bad...");
						}
						else if(sfl.getUpdate_time() + timeout < curts) {
              sfll.add(sfl);
            }
					} catch (Exception e) {
						LOG.error(e,e);
					}

  			}
  		}while(!cursor.equals("0"));
    }catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
    return sfll;
	}

  private String generateLtfKey(String tablename, String dbname) {
    String p = "sf.ltf.";
    if (tablename == null || dbname == null) {
      return p;
    }
    return p + dbname + "." + tablename;
  }

  private String generateFtlKey(List<SplitValue> value) {
    String p = "sf.ftf.";
    if (value == null || value.size() == 0) {
      return p + "none";
    }
    List<SplitValue> ls = new ArrayList<SplitValue>();
    for (SplitValue v : value) {
    	if (v.getLevel() == 1) {
        ls.add(v);
      }
    }
    return p + ls.hashCode();
  }

  private String generateLfbdKey(String digest) {
    String p = "sf.lfbd.";
    if (digest == null) {
      return p;
    }
    return p + digest;
  }

  private String generateSflStatKey(int status) {
  	return "sfl.stat." + status;
  }
  private String generateSflDevKey(String devid)
  {
  	return "sfl.dev."+devid;
  }

  private String generateSfStatKey(int status) {
  	if (status < 0) {
      return "sf.stat.*";
    }
  	return "sf.stat." + status;
  }

  public void removeSfileStatValue(int status, String value) throws IOException, JedisException
  {
  	String key = this.generateSfStatKey(status);
  	Jedis jedis = refreshJedis();
  	int err = 0;

    try {
    	jedis.srem(key, value);
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public void removeSflStatValue(int status, String value) throws IOException, JedisException
  {
  	String key = this.generateSflStatKey(status);
  	Jedis jedis = refreshJedis();
  	int err = 0;

    try {
    	jedis.srem(key, value);
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }

  public void removeLfbdValue(String digest, String value) throws IOException, JedisException
  {
  	String key = this.generateLfbdKey(digest);
  	Jedis jedis = refreshJedis();
  	int err = 0;

    try {
    	jedis.srem(key, value);
    } catch (JedisException e) {
      err = -1;
      throw e;
    } finally {
      if (err < 0) {
        RedisFactory.putBrokenInstance(jedis);
      } else {
        RedisFactory.putInstance(jedis);
      }
    }
  }
  public static ConcurrentHashMap<String, Database> getDatabaseHm() {
    return databaseHm;
  }

  public RedisFactory getRf() {
    return rf;
  }

  public long getCounts(ObjectType.TypeDesc key) throws JedisException, IOException {
    long nr = 0;

    switch (key.getId()) {
    case SFILE: {
      Jedis jedis = refreshJedis();
      int err = 0;

      try {
        nr = jedis.hlen(key.getName());
      } catch (JedisException e) {
        err = -1;
        throw e;
      } finally {
        if (err < 0) {
          RedisFactory.putBrokenInstance(jedis);
        } else {
          RedisFactory.putInstance(jedis);
        }
      }
      break;
    }
    default:;
    }
    return nr;
  }

  public static ConcurrentHashMap<String, PrivilegeBag> getPrivilegeBagHm() {
    return privilegeBagHm;
  }

  public static ConcurrentHashMap<String, Partition> getPartitionHm() {
    return partitionHm;
  }

  public static ConcurrentHashMap<String, Node> getNodeHm() {
    return nodeHm;
  }

  public static ConcurrentHashMap<String, NodeGroup> getNodeGroupHm() {
    return nodeGroupHm;
  }

  public static ConcurrentHashMap<String, GlobalSchema> getGlobalSchemaHm() {
    return globalSchemaHm;
  }

  public static ConcurrentHashMap<String, Table> getTableHm() {
    return tableHm;
  }

  public static ConcurrentHashMap<String, Index> getIndexHm() {
    return indexHm;
  }

	public static TimeLimitedCacheMap getsFileHm() {
		return sFileHm;
	}

	public static TimeLimitedCacheMap getSflHm() {
		return sflHm;
	}

	public static ConcurrentHashMap<String, Device> getDeviceHm() {
		return deviceHm;
	}

}

