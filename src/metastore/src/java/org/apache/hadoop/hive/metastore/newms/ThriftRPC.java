package org.apache.hadoop.hive.metastore.newms;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.commons.lang.StringUtils.join;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DiskManager;
import org.apache.hadoop.hive.metastore.DiskManager.DMProfile;
import org.apache.hadoop.hive.metastore.DiskManager.DMRequest;
import org.apache.hadoop.hive.metastore.DiskManager.DeviceInfo;
import org.apache.hadoop.hive.metastore.DiskManager.FLSelector.FLS_Policy;
import org.apache.hadoop.hive.metastore.DiskManager.FileLocatingPolicy;
import org.apache.hadoop.hive.metastore.DiskManager.ReplicateRequest;
import org.apache.hadoop.hive.metastore.DiskManager.RsStatus;
import org.apache.hadoop.hive.metastore.DiskManager.SysMonitor;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler.MSSessionState;
import org.apache.hadoop.hive.metastore.HiveMetaStoreServerContext;
import org.apache.hadoop.hive.metastore.HiveMetaStoreServerEventHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionContext;
import org.apache.hadoop.hive.metastore.MetaStoreEndFunctionListener;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.MultiMetaStoreTimerTask;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
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
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.statfs;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;

/*
 * 没缓存的对象，但是rpc里要得到的
 * BusiTypeColumn,ColumnStatistics,Type,role,User,HiveObjectPrivilege
 */


public class ThriftRPC extends FacebookBase implements
    org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface {

  private final static String DEFAULT_USER_NAME = "_unauthed_user";
  private final static ThriftRPCInfo rpcInfo = new ThriftRPCInfo();
  private final HiveConf hiveConf = new HiveConf(this.getClass());
  private final RawStoreImp rs;
  private IMetaStoreClient client;
  private static boolean initialized = false;

  private static final Log LOG = LogFactory.getLog(ThriftRPC.class);
  private static final ScheduledExecutorService schedule = Executors.newScheduledThreadPool(1);
  public static DiskManager dm;
  //public static LoongStorePolicy lsp ;
  private final Random rand = new Random();
  public static Long file_creation_lock = 0L;
  private List<MetaStoreEndFunctionListener> endFunctionListeners;
  private static int nextSerialNum = 0;
  private static final ThreadLocal<Integer> threadLocalId = new ThreadLocal<Integer>() {
    @Override
    protected synchronized Integer initialValue() {
      return new Integer(nextSerialNum++);
    }
  };

  private static final ThreadLocal<IMetaStoreClient> __cli = new ThreadLocal<IMetaStoreClient>() {
    @Override
    protected synchronized IMetaStoreClient initialValue() {
      return null;
    }
  };

  private final HiveMetaStore.HMSHandler.MSSessionState msss = new MSSessionState();

  // This will only be set if the metastore is being accessed from a metastore Thrift server,
  // not if it is from the CLI. Also, only if the TTransport being used to connect is an
  // instance of TSocket.
  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  private void init() throws IOException {
    if (!initialized) {
      try {
        //每20秒打印一次rpcInfo基本信息
        schedule.scheduleAtFixedRate(new Runnable(){
          @Override
          public void run() {
            LOG.info(rpcInfo);
          }
        }, 20, 20, TimeUnit.SECONDS);

        dm = new DiskManager(hiveConf, LOG, RsStatus.NEWMS);
        //lsp = new LoongStorePolicy();

        // FIXME: add multimetastoretimer here, call every 10 seconds?
        Timer t = new Timer("MultiMetaStoreTimer");
        t.schedule(new MultiMetaStoreTimerTask(hiveConf, 10, 120), 1 * 1000, 10 * 1000);

        //dump rpcInfo per minute
        schedule.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            String path = hiveConf.getVar(HiveConf.ConfVars.NEWMS_RPC_INFO_FILENAME);
            try {
              rpcInfo.dumpToFile(path);
            } catch (IOException e) {
              LOG.error("Error in dump rpc info to file\n", e);
            }
          }
        }, 1, 1, TimeUnit.MINUTES);
      } catch (Exception e) {
        LOG.error("Init ThriftRPC server failed", e);
        throw new IOException(e.getMessage());
      }
      initialized = true;
    }
  }

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  // This will return null if the metastore is not being accessed from a metastore Thrift server,
  // or if the TTransport being used to connect is not an instance of TSocket.
  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  public static final String AUDIT_FORMAT =
      "user=%s\t" + // ugi
          "ip=%s\t" + // remote IP
          "cmd=%s\t"; // command
  public static final Log auditLog = LogFactory.getLog(
      ThriftRPC.class.getName() + ".audit");

  private static ThreadLocal<Formatter> auditFormatter =
      new ThreadLocal<Formatter>() {
        @Override
        protected Formatter initialValue() {
          return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
        }
      };

  private final void logAuditEvent(String cmd) {
    if (cmd == null) {
      return;
    }

    final Formatter fmt = auditFormatter.get();

    ((StringBuilder) fmt.out()).setLength(0);

    String address;
    address = getIpAddress();
    if (address == null) {
      address = "unknown-ip-addr";
    }
    String user = "";
    HiveMetaStoreServerContext serverContext = HiveMetaStoreServerEventHandler
        .getServerContext(msss.getSessionId());
    if (serverContext != null) {
      user += serverContext.getUserName() + "(" + serverContext.isAuthenticated() + ")";
    } else {
      user = "unknown";
    }

    auditLog.info(fmt.format(AUDIT_FORMAT, user, address, cmd).toString());
  }

  public ThriftRPC() throws IOException {
    super("NewMS-RPC");
    init();
    rs = new RawStoreImp();
    try {
      endFunctionListeners = MetaStoreUtils.getMetaStoreListeners(
          MetaStoreEndFunctionListener.class, hiveConf,
          hiveConf.getVar(HiveConf.ConfVars.METASTORE_END_FUNCTION_LISTENERS));
    } catch (MetaException e) {
      LOG.error(e, e);
      throw new IOException(e.getMessage());
    }
    // ok, call FLSelector Framework here!
    if (dm != null) {
      try {
        String intbls = hiveConf.getVar(HiveConf.ConfVars.FLSELECTOR_WATCH_NONE);

        if (intbls != null && !intbls.equals("")) {
          DiskManager.flselector.initWatchedTables(rs, intbls.split(";"), FLS_Policy.NONE);
        }
        intbls = hiveConf.getVar(HiveConf.ConfVars.FLSELECTOR_WATCH_FAIR_NODES);
        if (intbls != null && !intbls.equals("")) {
          DiskManager.flselector.initWatchedTables(rs, intbls.split(";"), FLS_Policy.FAIR_NODES);
        }
        intbls = hiveConf.getVar(HiveConf.ConfVars.FLSELECTOR_WATCH_ORDERED_ALLOC);
        if (intbls != null && !intbls.equals("")) {
          DiskManager.flselector.initWatchedTables(rs, intbls.split(";"), FLS_Policy.ORDERED_ALLOC_DEVS);
        }
      } catch (Exception e) {
        LOG.error("Init FLSelector Framework failed!");
        LOG.error(e, e);
      }
    }
  }

  private boolean __do_reconnect() {
    boolean success = false;
    HiveMetaStoreServerContext context = this.getServerContext();

    try {
      __cli.set(MsgProcessing.createMetaStoreClient());
      if (__cli.get() != null) {
        success = true;
        // BUG-XXX: we have to set client timeout here, otherwise, more and more queries
        // will send to OldMS.
        __cli.get().setTimeout(120);
      }
    } catch (MetaException e) {
    }

    if (success && this.getServerContext().isAuthenticated()) {
      try {
        success = this.authentication(context.getUserName(), context.getPassword());
        if (!success) {
          // this means passwd changes or connection failed, active shutdown now
          this.shutdown();
        }
      } catch (NoSuchObjectException e) {
      } catch (MetaException e) {
      } catch (TException e) {
      }
    }
    return success;
  }

  private class _ProxyThriftRPC implements InvocationHandler {
    private ThriftRPC rpc = null;

    public _ProxyThriftRPC(ThriftRPC rpc) {
      this.rpc = rpc;
    }

    public Iface getRPC() {
      return (Iface) Proxy.newProxyInstance(ThriftRPC.class.getClassLoader(),
          ThriftRPC.class.getInterfaces(), this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      Object result = null;
      long startTime = System.nanoTime();
      boolean isRetry1 = false, isRetry2 = false;

      /*
       * 在客户端失效重连阶段会先从clients中移除失效的client，然后重新创建并认证新client
       * 如果移除旧client后认证新client前客户端调用rpc中使用client的方法可能会导致NullPointerException
       * 造成严重错误，因此需要线程自旋判断client是否为中间状态
       */
      while (true) {
        try {
          result = method.invoke(rpc, args);
          rpcInfo.captureInfo(method.getName(), System.nanoTime() - startTime);
          return result;
        } catch (UndeclaredThrowableException e) {
          rpcInfo.incErr();
          throw e.getCause();
        } catch (InvocationTargetException e) {
          if ((e.getCause() instanceof NullPointerException)) {
            // this means we should do client connect
            if (!isRetry1) {
              __do_reconnect();
              isRetry1 = true;
            } else {
              rpcInfo.incErr();
              throw e.getCause();
            }
          } else if ((e.getCause() instanceof TApplicationException) ||
              (e.getCause() instanceof TProtocolException) ||
              (e.getCause() instanceof TTransportException)) {
            // 发生连接异常，自动重建连接
            if (!isRetry2) {
              LOG.error("Some error occured when call method " + methodName + ", try reconnect.");
              __do_reconnect();
              isRetry2 = true;
            } else {
              rpcInfo.incErr();
              throw e.getCause();
            }
          } else {
            rpcInfo.incErr();
            throw e.getCause();
          }
        }
      }
    }
  }

  public Iface newProxy() throws IOException {
    return new _ProxyThriftRPC(this).getRPC();
  }

  @Override
  public AbstractMap<String, Long> getCounters() {
    AbstractMap<String, Long> counters = new HashMap<String, Long>();

    // Allow endFunctionListeners to add any counters they have collected
    if (endFunctionListeners != null) {
      for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
        listener.exportCounters(counters);
      }
    }

    return counters;
  }

  @Override
  public String getCpuProfile(int profileDurationInSec) throws TException {
    // TODO HiveMetaStore just return ""
    // HiveMetaStoreClient didn't call this method
    return "";
  }

  @Override
  public fb_status getStatus() {
    return fb_status.ALIVE;
  }

  @Override
  public String getVersion() throws TException {
    endFunction(startFunction("getVersion", ""), true, null);
    return "3.2";
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down the new object store...");
    // BUG-XXX: do NOT shutdown timer thread and RS, each connection(thread) might call
    // this function once
    /*if (rs != null) {
      rs.shutdown();
    }
    schedule.shutdown();*/
    if (__cli.get() != null) {
      __cli.get().close();
      __cli.set(null);
    }
    LOG.info("NewMS shutdown complete.");
  }

  @Override
  public boolean addEquipRoom(EquipRoom arg0) throws MetaException,
      TException {
    return arg0 != null && __cli.get().addEquipRoom(arg0);
  }

  @Override
  public boolean addGeoLocation(GeoLocation arg0) throws MetaException,
      TException {
    return arg0 != null && __cli.get().addGeoLocation(arg0);
  }

  @Override
  public boolean addNodeAssignment(String nodeName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr
        && __cli.get().addNodeAssignment(nodeName, dbName);
  }

  @Override
  public boolean addNodeGroup(NodeGroup nodeGroup) throws AlreadyExistsException,
      MetaException, TException {
    return nodeGroup != null
        && __cli.get().addNodeGroup(nodeGroup);
  }

  @Override
  public boolean addNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    final boolean expr = nodeGroup == null || isNullOrEmpty(dbName);
    return !expr
        && __cli.get().addNodeGroupAssignment(nodeGroup,
            dbName);
  }

  @Override
  public boolean addRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(roleName) || isNullOrEmpty(dbName);
    return !expr
        && __cli.get().addRoleAssignment(roleName, dbName);
  }

  @Override
  public boolean addTableNodeDist(String dbName, String tableName, List<String> nodeGroupList)
      throws MetaException, TException {
    final boolean expr = isNullOrEmpty(dbName) || isNullOrEmpty(tableName) || nodeGroupList == null;
    return !expr
        && __cli.get().addTableNodeDist(dbName, tableName,
            nodeGroupList);
  }

  @Override
  public boolean addUserAssignment(String userName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    final boolean expr = isNullOrEmpty(userName) || isNullOrEmpty(dbName);
    return !expr
        && __cli.get().addUserAssignment(userName, dbName);
  }

  @Override
  public boolean add_datawarehouse_sql(int dwNum, String sql)
      throws InvalidObjectException, MetaException, TException {
    return __cli.get().addDatawareHouseSql(dwNum, sql);
  }

  @Override
  public Index add_index(Index index, Table indexTable)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    __cli.get().createIndex(index, indexTable);
    return __cli.get().getIndex(index.getDbName(),
        index.getOrigTableName(), index.getIndexName());
  }

  @Override
  public Node add_node(String nodeName, List<String> ipl) throws MetaException,
      TException {
    // final boolean expr = isNullOrEmpty(nodeName) || ipl == null;
    // checkArgument(expr, "nodeName and ipl shuldn't be null or empty");
    // final Node node = __cli.get().add_node(nodeName, ipl);
    // return node;
    return __cli.get().add_node(nodeName, ipl);
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return __cli.get().add_partition(
        checkNotNull(partition, "partation shuldn't be null"));
  }

  @Override
  public int add_partition_files(Partition partition, List<SFile> sfiles)
      throws TException {
    return __cli.get().add_partition_files(
        checkNotNull(partition), checkNotNull(sfiles));
  }

  @Override
  public boolean add_partition_index(Index index, Partition partition)
      throws MetaException, AlreadyExistsException, TException {
    return __cli.get().add_partition_index(
        checkNotNull(index), checkNotNull(partition));
  }

  @Override
  public boolean add_partition_index_files(Index index, Partition part, List<SFile> file,
      List<Long> originfid) throws MetaException, TException {
    return __cli.get().add_partition_index_files(index,
        part, file, originfid);
  }

  @Override
  public Partition add_partition_with_environment_context(Partition part,
      EnvironmentContext envContext) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    // TODO mzy
    return null;
  }

  @Override
  public int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return __cli.get().add_partitions(partitions);
  }

  /**
   * just return true like HiveMetaStore
   *
   * @author mzy
   */
  @Override
  public boolean add_subpartition(String arg0, String arg1,
      List<String> arg2, Subpartition arg3) throws TException {
    return true;
  }

  @Override
  public int add_subpartition_files(Subpartition subpart, List<SFile> files)
      throws TException {
    return __cli.get()
        .add_subpartition_files(subpart, files);
  }

  @Override
  public boolean add_subpartition_index(Index index, Subpartition subpart)
      throws MetaException, AlreadyExistsException, TException {
    return __cli.get()
        .add_subpartition_index(index, subpart);
  }

  @Override
  public boolean add_subpartition_index_files(Index index, Subpartition subpart,
      List<SFile> file, List<Long> originfid) throws MetaException, TException {
    return __cli.get().add_subpartition_index_files(index,
        subpart, file, originfid);
  }

  @Override
  public boolean alterNodeGroup(NodeGroup ng)
      throws AlreadyExistsException, MetaException, TException {
    return __cli.get().alterNodeGroup(ng);
  }

  @Override
  public void alter_database(String name, Database db)
      throws MetaException, NoSuchObjectException, TException {
    __cli.get().alterDatabase(name, db);
  }

  @Override
  public void alter_index(String dbName, String tblName, String indexName, Index index)
      throws InvalidOperationException, MetaException, TException {
    __cli.get().alter_index(dbName, tblName, indexName,
        index);
  }

  @Override
  public Node alter_node(String nodeName, List<String> ipl, int status)
      throws MetaException, TException {
    return __cli.get().alter_node(nodeName, ipl, status);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    __cli.get().alter_partition(dbName, tblName, newPart);
  }

  @Override
  public void alter_partition_with_environment_context(String dbName,
      String name, Partition newPart, EnvironmentContext arg3)
      throws InvalidOperationException, MetaException, TException {
    __cli.get().renamePartition(dbName, name, null, newPart);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {
    __cli.get().alter_partitions(dbName, tblName, newParts);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    __cli.get().alter_table(defaultDatabaseName, tblName,
        table);
    Table ntbl = null;

    List<FieldSchema> tableCols = new ArrayList<FieldSchema>();
    List<FieldSchema> ntblCols = new ArrayList<FieldSchema>();

    List<FieldSchema> tmpCols = new ArrayList<FieldSchema>();
    tmpCols.addAll(table.getSd().getCols());

    Map<String, String> fieldschemaMap = new HashMap<String, String>();

    for(int i = 0 ; i < 150 ; i++ )
    {
      ntbl = rs.getTable(table.getDbName(), table.getTableName());
      if(ntbl == null)
      {
        try {
          Thread.sleep(200);
        } catch (Exception e) {

        }
        continue;
      }
      tableCols.clear();
      tableCols.addAll(table.getSd().getCols());

      ntblCols.clear();
      ntblCols.addAll(ntbl.getSd().getCols());

      tableCols.removeAll(ntblCols);
      ntblCols.removeAll(tmpCols);

      // removeall求两者的差后，如果size相等，则继续判断
      if(tableCols.size()==ntblCols.size())
      {
        fieldschemaMap.clear();
        Iterator<FieldSchema> bIterator = ntblCols.iterator();
        while(bIterator.hasNext())
        {
          FieldSchema tFieldSchema = bIterator.next();
          fieldschemaMap.put(tFieldSchema.getName(), tFieldSchema.getType()+tFieldSchema.getComment());
        }

        boolean done = true;

        Iterator<FieldSchema> aIterator = tableCols.iterator();
        while(aIterator.hasNext())
        {
          FieldSchema tFieldSchema = aIterator.next();
          try {
            if((fieldschemaMap.get(tFieldSchema.getName())==null)
                ||(!fieldschemaMap.get(tFieldSchema.getName()).equals(tFieldSchema.getType()+tFieldSchema.getComment())))
             {
               done = false;
               break;
             }

          } catch (Exception e) {
            // TODO: handle exception
          }
        }
        if(done == true)
        {
          break;
        }
      }
      try {
        Thread.sleep(200);
      } catch (Exception e) {
        // TODO: handle exception
      }
    }
  }

  @Override
  public void alter_table_with_environment_context(final String dbname,
      final String name, final Table newTable,
      final EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException {
    // TODO implement this method HiveMetaStoreClient didn't call this method
  }

  @Override
  public void append_busi_type_datacenter(BusiTypeDatacenter busiTypeDatacenter)
      throws InvalidObjectException, MetaException, TException {
    __cli.get().append_busi_type_datacenter(
        busiTypeDatacenter);
  }

  @Override
  public Partition append_partition(String tableName, String dbName,
      List<String> partVals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException {
    return __cli.get().appendPartition(tableName, dbName,
        partVals);
  }

  @Override
  public Partition append_partition_by_name(String tableName, String dbName,
      String name) throws InvalidObjectException, AlreadyExistsException,
      MetaException, TException {
    return __cli.get().appendPartition(tableName, dbName,
        name);
  }

  @Override
  public boolean assiginSchematoDB(String dbName, String schemaName,
      List<FieldSchema> fileSplitKeys, List<FieldSchema> partKeys, List<NodeGroup> ngs)
      throws InvalidObjectException, NoSuchObjectException,
      MetaException, TException {
    return __cli.get().assiginSchematoDB(dbName, schemaName,
        fileSplitKeys, partKeys, ngs);
  }

  @Override
  public boolean authentication(String userName, String passwd)
      throws NoSuchObjectException, MetaException, TException {
    incrementCounter("user_authentication");

    if (__cli.get().authentication(userName, passwd)) {
      HiveMetaStoreServerContext serverContext = HiveMetaStoreServerEventHandler
          .getServerContext(msss.getSessionId());
      if (serverContext != null) {
        serverContext.setUserName(userName);
        serverContext.setPassword(passwd);
        serverContext.setAuthenticated(true);
      }
      return true;
    } else {
      return false;
    }
  }

  private HiveMetaStoreServerContext getServerContext() {
    return HiveMetaStoreServerEventHandler.getServerContext(msss.getSessionId());
  }

  @Override
  public void cancel_delegation_token(String tokenStrForm) throws MetaException,
      TException {
    __cli.get().cancelDelegationToken(tokenStrForm);
  }

  @Override
  public int close_file(SFile file) throws FileOperationException, MetaException, TException {
    startFunction("close_file ", "fid: " + file.getFid());
    DMProfile.fcloseR.incrementAndGet();

    FileOperationException e = null;
    SFile saved = rs.getSFile(file.getFid());
    String devid = "FDev", loc = "FLoc";

    try {
      if (file.getDigest() == null) {
        LOG.warn("File " + file.getFid() + " contains NULL digest.");
        file.setDigest("DETECT_NULL_DIGEST");
      }
      if (saved == null) {
        throw new FileOperationException("Can not find SFile by FID" + file.getFid(),
            FOFailReason.INVALID_FILE);
      }

      if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        LOG.error("File " + file.getFid() + " StoreStatus is not in INCREATE (vs " + saved.getStore_status() + ").");
        throw new FileOperationException("File StoreStatus is not in INCREATE (vs "
            + saved.getStore_status() + ").",
            FOFailReason.INVALID_STATE);
      }

      // find the valid filelocation, mark it and trigger replication
      if (file.getLocationsSize() > 0) {
        int valid_nr = 0;

        // find valid Online NR
        for (SFileLocation sfl : file.getLocations()) {
          if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            valid_nr++;
            devid = sfl.getDevid();
            loc = sfl.getLocation();
          }
        }
        if (valid_nr > 1) {
          LOG.error("File " + file.getFid() + " Too many file locations provided, expect 1 provided " + valid_nr
              + " [NOT CLOSED]");
          throw new FileOperationException("Too many file locations provided, expect 1 provided "
              + valid_nr + " [NOT CLOSED]",
              FOFailReason.INVALID_FILE);
        } else if (valid_nr < 1) {
          LOG.error("File " + file.getFid() + " Too little file locations provided, expect 1 provided " + valid_nr
              + " [CLOSED]");
          e = new FileOperationException("Too little file locations provided, expect 1 provided "
              + valid_nr + " [CLOSED]",
              FOFailReason.INVALID_FILE);
        }
        // finally, do it
        List<SFileLocation> sflToDel = new ArrayList<SFileLocation>();
        for (SFileLocation sfl : file.getLocations()) {
          if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            sfl.setRep_id(0);
            sfl.setDigest(file.getDigest());
            rs.updateSFileLocation(sfl);
          } else if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
            // this is an increp SFL, ok, accept it
          } else {
            sflToDel.add(sfl);
            dm.asyncDelSFL(sfl);
          }
        }
        // BUG-XXX: trunc offline sfls
        if (sflToDel.size() > 0) {
          file.getLocations().removeAll(sflToDel);
        }
        // BUG-XXX: if client didn't call online_sfilelocation, then file and saved are inconsistent.
        // So, we have to check saved sfile here
        for (SFileLocation sfl : saved.getLocations()) {
          // double check and delete invalid SFLs, this might result in false warnings in SFL delete
          if (sfl.getVisit_status() != MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            if (sfl.getDevid().equals(devid) && sfl.getLocation().equals(loc)) {
              // this is the master SFL, warn on it
              LOG.warn("Master copy is not in ONLINE state: fid " + file.getFid() + " sfl devid "
                  + sfl.getDevid() + " loc " + sfl.getLocation() + " state " + sfl.getVisit_status());
            } else if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
              // this is an increp SFL, ok, accept it
              LOG.info("Close an INCREP file: fid " + file.getFid() + " sfl devid "
                  + sfl.getDevid() + " loc " + sfl.getLocation() + " state " + sfl.getVisit_status());
            } else {
              dm.asyncDelSFL(sfl);
            }
          }
        }
      } else {
        LOG.error("File " + file.getFid() + " Too little file locations provided, expect 1 provided "
            + file.getLocationsSize() + " [CLOSED]");
        e = new FileOperationException("Too little file locations provided, expect 1 provided "
            + file.getLocationsSize() + " [CLOSED]",
            FOFailReason.INVALID_FILE);
      }

      // NOTE-XXX: check repnr here. If we can go at this line, we ONLY has one valid SFL here.
      if (1 >= saved.getRep_nr()) {
        file.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
      } else {
        file.setStore_status(MetaStoreConst.MFileStoreStatus.CLOSED);
      }

      // keep repnr unchanged
      file.setRep_nr(saved.getRep_nr());
      rs.updateSFile(file, false);

      if (e != null) {
        throw e;
      }

      synchronized (dm.repQ) {
        dm.repQ.add(new DMRequest(file, DMRequest.DMROperation.REPLICATE, 1));
        dm.repQ.notify();
      }
    } finally {
      endFunction("close_file", true, e);
      DMProfile.fcloseSuccRS.incrementAndGet();
    }
    return 0;
  }

  private void logInfo(String m) {
    LOG.info(threadLocalId.get().toString() + ": " + m);
    logAuditEvent(m);
  }

  public String startFunction(String function, String extraLogInfo) {
    incrementCounter(function);
    logInfo((getIpAddress() == null ? "" : "source:" + getIpAddress() + " ") + function +
        extraLogInfo);
    try {
      Metrics.startScope(function);
    } catch (IOException e) {
      LOG.debug("Exception when starting metrics scope"
          + e.getClass().getName() + " " + e.getMessage());
      MetaStoreUtils.printStackTrace(e);
    }
    return function;
  }
  public String startFunction(String function) {
    return startFunction(function, "");
  }
  public String startMultiTableFunction(String function, String db, List<String> tbls) {
    String tableNames = join(tbls, ",");
    return startFunction(function, " : db=" + db + " tbls=" + tableNames);
  }
  public String startTableFunction(String function, String db, String tbl) {
    return startFunction(function, " : db=" + db + " tbl=" + tbl);
  }
  public String startPartitionFunction(String function, String db, String tbl,
      List<String> partVals) {
    return startFunction(function, " : db=" + db + " tbl=" + tbl
        + "[" + join(partVals, ",") + "]");
  }

  public void endFunction(String function, boolean successful, Exception e) {
    endFunction(function, new MetaStoreEndFunctionContext(successful, e));
  }

  public void endFunction(String function, MetaStoreEndFunctionContext context) {
    try {
      Metrics.endScope(function);
    } catch (IOException e) {
      LOG.debug("Exception when closing metrics scope" + e);
    }

    for (MetaStoreEndFunctionListener listener : endFunctionListeners) {
      listener.onEndFunction(function, context);
    }
  }

  @Override
  public int createBusitype(Busitype bt) throws InvalidObjectException,
      MetaException, TException {
    return __cli.get().createBusitype(bt);
  }

  @Override
  public boolean createSchema(GlobalSchema schema)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    boolean r = __cli.get().createSchema(schema);
    // BUG-XXX: we should check whether this SCHEMA has been created.
    GlobalSchema gs = null;

    for (int i = 0; gs == null && i < 25; i++) {
      try {
        gs = rs.getSchema(schema.getSchemaName());
        if (gs != null) {
          break;
        }
      } catch (NoSuchObjectException e) {
      }
      try {
        Thread.sleep(200);
      } catch (Exception e) {
      }
    }
    if (gs != null) {
      LOG.info("createSchema: " + schema.getSchemaName() + " OK.");
    } else {
      LOG.info("createSchema: " + schema.getSchemaName() + " still progressing.");
    }
    return r;
  }

  @Override
  public void create_attribution(Database db)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, TException {
    __cli.get().create_attribution(db);
    // BUG-XXX: we should check whether this DB has been created.
    try {
      Database ndb = null;
      for (int i = 0; ndb == null && i < 25; i++) {
        try {
          ndb = rs.getDatabase(db.getName());
          if (ndb != null) {
            break;
          }
        } catch (NoSuchObjectException e) {
        }
        try {
          Thread.sleep(200);
        } catch (Exception e) {
        }
      }
      if (ndb != null) {
        LOG.info("create_attribution: " + db.getName() + " OK.");
      } else {
        LOG.info("create_attribution: " + db.getName() + " still progressing.");
      }
    } catch (Exception e) {
    }
  }

  @Override
  public void create_database(Database db) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    __cli.get().createDatabase(db);
    // BUG-XXX: we should check whether this DB has been created.
    try {
      Database ndb = null;
      for (int i = 0; ndb == null && i < 25; i++) {
        try {
          ndb = rs.getDatabase(db.getName());
          if (ndb != null) {
            break;
          }
        } catch (NoSuchObjectException e) {
        }
        try {
          Thread.sleep(200);
        } catch (Exception e) {
        }
      }
      if (ndb != null) {
        LOG.info("create_database: " + db.getName() + " OK.");
      } else {
        LOG.info("create_database: " + db.getName() + " still progressing.");
      }
    } catch (Exception e) {
    }
  }

  @Override
  public Device create_device(String devId, int prop, String nodeName)
      throws MetaException, TException {
    return __cli.get().createDevice(devId, prop, nodeName);
  }

  private int validate_file_repnr(int repnr) {
      if (repnr <= 0) {
        repnr = 1;
      }
      if (repnr > 5) {
        repnr = 5;
      }
      return repnr;
    }

  private SFile create_file_wo_location(int repnr, String dbName, String tableName,
      List<SplitValue> values)
    throws FileOperationException, TException {
    DMProfile.fcreate1R.incrementAndGet();

    if (!fileSplitValuesCheck(values)) {
      throw new FileOperationException("Invalid File Split Values: inconsistent version among values?", FOFailReason.INVALID_FILE);
    }
    SFile cfile = new SFile(0, dbName, tableName, MetaStoreConst.MFileStoreStatus.INCREATE, repnr,
        "SFILE_DEFAULT_X", 0, 0, null, 0, null, values, MetaStoreConst.MFileLoadStatus.OK);
    cfile = rs.createFile(cfile);
    if (cfile == null) {
      throw new FileOperationException("Creating file with interval error, metadata inconsistent?", FOFailReason.INVALID_FILE);
    }

    DMProfile.fcreate1SuccR.incrementAndGet();
    return cfile;
  }

  @Override
  public SFile create_file(String node_name, int repnr, String db_name, String table_name,
      List<SplitValue> values)
      throws FileOperationException, TException {
    DMProfile.fcreate1R.incrementAndGet();

    startFunction("create_file_v1:", "node " + node_name + " db:" + db_name +
          " table: " + table_name + " values: " + values);
    if (!fileSplitValuesCheck(values)) {
      throw new FileOperationException(
          "Invalid File Split Values: inconsistent version among values?",
          FOFailReason.INVALID_FILE);
    }
    repnr = validate_file_repnr(repnr);

    Set<String> excl_dev = dm.disabledDevs;
    Set<String> spec_node = new TreeSet<String>();

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
        throw new FileOperationException("getTable:" + db_name + "." + table_name + " + "
            + me.getMessage(), FOFailReason.INVALID_TABLE);
      }
    }
    // do not select the backup/shared device for the first entry
    FileLocatingPolicy flp;

    if (spec_node.size() > 0) {
      flp = new FileLocatingPolicy(spec_node, excl_dev, FileLocatingPolicy.SPECIFY_NODES,
          FileLocatingPolicy.RANDOM_DEVS, false);
    } else {
      flp = new FileLocatingPolicy(null, excl_dev, FileLocatingPolicy.EXCLUDE_NODES,
          FileLocatingPolicy.RANDOM_DEVS, false);
    }

    SFile rf = create_file(flp, node_name, repnr, db_name, table_name, values);
    DMProfile.fcreate1SuccR.incrementAndGet();
    LOG.info("create_file_v1: db=" + db_name + " tbl=" + table_name +
          " node=" + node_name + " -> " + rf.getFid());

    return rf;
  }

  // copy from HiveMetaStore
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

  private String analyzeSplitTypes(List<FieldSchema> fileSplitkeys) {
    String colname = "";

    if (fileSplitkeys.size() == 0) {
    } else if (fileSplitkeys.size() == 1) {
      FieldSchema filesplitkey = fileSplitkeys.get(0);
      colname = filesplitkey.getName();
    } else {
      // 倒数第二个分区
      FieldSchema twoFieldSchema = fileSplitkeys.get(fileSplitkeys.size() - 2);
      String twocolname = twoFieldSchema.getName();
      String twoComment = twoFieldSchema.getComment();
      PartitionInfo twopi = PartitionFactory.PartitionInfo.fromJson(twoComment);
      int twopversion = twopi.getP_version();

      // 最后一个分区
      FieldSchema lastFieldSchema = fileSplitkeys.get(fileSplitkeys.size() - 1);
      String lastcolname = lastFieldSchema.getName();
      String lastComment = lastFieldSchema.getComment();
      PartitionInfo lastpi = PartitionFactory.PartitionInfo.fromJson(lastComment);
      int lastpversion = lastpi.getP_version();
      // 判断最后两个分区的p_version是否一致
      if (lastpversion == twopversion) {
        colname = twocolname + "-" + lastcolname;
      } else {
        colname = lastcolname;
      }
    }
    return colname;
  }

  public String get_sfile_location(String db_name, String table_name, List<SplitValue> values)
      throws FileOperationException {
    String locationString = "";

    if (db_name == null || table_name == null) {
      locationString = "/UNNAMED-DB/UNNAMED-TABLE/" + rand.nextInt(Integer.MAX_VALUE);
      return locationString;
    }
    try {
      Table tbl = rs.getTable(db_name, table_name);
      if (tbl == null) {
        throw new FileOperationException(
            "Invalid DB or Table name:" + db_name + "." + table_name, FOFailReason.INVALID_TABLE);
      }
      StorageDescriptor sd = tbl.getSd();
      String tblLocation = sd.getLocation();
      // FIXME: change 21 to XXXX
      tblLocation = tblLocation.substring(21);
      List<FieldSchema> fileSplitKeys = tbl.getFileSplitKeys();
      String fileSplitNames = analyzeSplitTypes(fileSplitKeys);
      String[] colsNames = fileSplitNames.split("-");
      if (colsNames.length == 1) {
        SplitValue sv = values.get(0);
        String splitKeyName = sv.getSplitKeyName();
        // 判断splitValue的splitKeyName是否正确
        if (!colsNames[0].equalsIgnoreCase(splitKeyName)) {
          throw new FileOperationException("The splitKeyName of SplitValue is invalid!",
              FOFailReason.INVALID_SPLIT_VALUES);
        }
        String val = sv.getValue();
        // tblLocation要把前面的hdfs路径去掉
        locationString = tblLocation + "/" + splitKeyName + "=" + val;
      } else if (colsNames.length == 2) {
        String splitkeyname1 = "";
        String splitkeyname2 = "";
        String val1 = "";
        String val2 = "";
        if (values.size() == 4) {
          splitkeyname1 = colsNames[0];
          splitkeyname2 = colsNames[1];
          val1 = values.get(0).getValue();
          val2 = values.get(2).getValue();
        } else if (values.size() == 3) {
          if (values.get(1).getSplitKeyName().equalsIgnoreCase(colsNames[0])) {
            // AAB
            splitkeyname1 = colsNames[0];
            splitkeyname2 = colsNames[1];
            val1 = values.get(0).getValue();
            val2 = values.get(2).getValue();
          } else if (values.get(1).getSplitKeyName().equalsIgnoreCase(colsNames[1])) {
            //ABB
            splitkeyname1 = colsNames[0];
            splitkeyname2 = colsNames[1];
            val1 = values.get(0).getValue();
            val2 = values.get(1).getValue();
          }
        }
        locationString = tblLocation + "/" +
            splitkeyname1 + "=" + val1 + "/" + splitkeyname2 + "=" + val2;
      }
    } catch (MetaException me) {
      throw new FileOperationException("Invalid DB or Table name:" + db_name + "." + table_name
          + " + " + me.getMessage(), FOFailReason.INVALID_TABLE);
    }
    return locationString;
  }

  // copy from HiveMetaStore
  private SFile create_file(FileLocatingPolicy flp, String node_name, int repnr, String db_name,
      String table_name, List<SplitValue> values)
      throws FileOperationException, TException {
    String devid = null;

    if (dm == null) {
      return null;
    }

    String location = get_sfile_location(db_name, table_name, values);

    if (node_name == null) {
      // this means we should select Best Available Node and Best Available Device;
      // FIXME: add FLSelector here, filter already used nodes, update will used nodes;
      try {
        repnr = DiskManager.flselector.updateRepnr(db_name + "." + table_name, repnr);
        // call FLSelector's main function
        switch (DiskManager.flselector.FLSelector_switch(db_name + "." + table_name)) {
        default:
        case NONE:
          node_name = dm.findBestNode(flp);
          break;
        case FAIR_NODES:{
          if (db_name != null && table_name != null && values.size() == 3) {
            try {
              String l2keys[] = values.get(2).getValue().split("-");
              if (l2keys.length == 2) {
                node_name = DiskManager.flselector.findBestNode(dm, flp, db_name + "." + table_name,
                    Long.parseLong(values.get(0).getValue()), Long.parseLong(l2keys[1]));
                LOG.info("FLSelector choose " + node_name +
                    " for " + db_name + "." + table_name +
                    " L1Key=" + values.get(0).getValue() +
                    " L2Key=" + l2keys[1] + ", repnr=" + repnr);
              }
            }
            catch (NumberFormatException nfe) {
              LOG.error(nfe, nfe);
            }
            // NOTE-XXX: if user has set type order, use it!
            if (DiskManager.flselector.isTableOrdered(db_name + "." + table_name)) {
              flp.dev_mode = FileLocatingPolicy.ORDERED_ALLOC;
              flp.accept_types = DiskManager.flselector.getDevTypeListAfterIncludeHint(
                  db_name + "." + table_name,
                  MetaStoreConst.MDeviceProp.__AUTOSELECT_R1__);
            }
          }
          break;
        }
        case ORDERED_ALLOC_DEVS:
          // NOTE-XXX: in order_alloc node mode, we use specified device type to filter nodes, and
          // random select in 5 freest nodes.
          flp.node_mode = FileLocatingPolicy.ORDERED_ALLOC;
          flp.dev_mode = FileLocatingPolicy.ORDERED_ALLOC;
          flp.accept_types = DiskManager.flselector.getDevTypeListAfterIncludeHint(
              db_name + "." + table_name,
              MetaStoreConst.MDeviceProp.__AUTOSELECT_R1__);
          break;
        }

        if (node_name == null) {
          node_name = dm.findBestNode(flp);
        }
        if (node_name == null) {
          throw new IOException("Following the FLP(" + flp + "), we can't find any available node now.");
        }
      } catch (IOException e) {
        LOG.error(e, e);
        throw new FileOperationException("Can not find any Best Available Node now, please retry",
            FOFailReason.SAFEMODE);
      }
    }

    SFile cfile = null;

    // Step 1: find best device to put a file
    try {
      if (flp == null) {
        flp = new FileLocatingPolicy(null, null, FileLocatingPolicy.EXCLUDE_NODES,
            FileLocatingPolicy.EXCLUDE_DEVS_SHARED, true);
      }
      //ztt 说明没有使用龙存策略或者是使用了龙存策略，但是没找到可用设备
      if (devid == null) {
        devid = dm.findBestDevice(node_name, flp);
      }

      if (devid == null) {
        throw new FileOperationException("Can not find any available device on node '" + node_name
            + "' now", FOFailReason.NOTEXIST);
      }

      // how to convert table_name to tbl_id?
      cfile = new SFile(0, db_name, table_name, MetaStoreConst.MFileStoreStatus.INCREATE, repnr,
          "SFILE_DEFALUT", 0, 0, null, 0, null, values, MetaStoreConst.MFileLoadStatus.OK);
      cfile = rs.createFile(cfile);
      // cfile = getMS().getSFile(cfile.getFid());
      if (cfile == null) {
        throw new FileOperationException(
            "Creating file with internal error, metadata inconsistent?", FOFailReason.INVALID_FILE);
      }

      do {
        SFileLocation sfloc = new SFileLocation(node_name, cfile.getFid(), devid, location, 0,
            System.currentTimeMillis(),
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
      throw new FileOperationException("System might in Safe Mode, please wait ... {" + e + "}",
          FOFailReason.SAFEMODE);
    } catch (InvalidObjectException e) {
      throw new FileOperationException("Internal error: " + e.getMessage(),
          FOFailReason.INVALID_FILE);
    }

    return cfile;
  }

  @Override
  public SFile create_file_by_policy(CreatePolicy policy, int repnr, String db_name,
      String table_name, List<SplitValue> values)
      throws FileOperationException, TException {
    DMProfile.fcreate2R.incrementAndGet();
    Table tbl = null;
    List<NodeGroup> ngs = null;
    Set<String> ngnodes = new HashSet<String>();

    startFunction("create_file_by_policy:", "CP " + policy.getOperation() +
        " db: " + db_name + " table: " + table_name + " values: " + values);
    repnr = validate_file_repnr(repnr);

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
        throw new FileOperationException("getTable:" + db_name + "." + table_name + " + "
            + me.getMessage(), FOFailReason.INVALID_TABLE);
      }
      if (tbl == null) {
        throw new FileOperationException("Invalid DB or Table name:" + db_name + "." + table_name,
            FOFailReason.INVALID_TABLE);
      }

      // check nodegroups now
      if (policy.getOperation() == CreateOperation.CREATE_NEW_IN_NODEGROUPS) {
        if (policy.getArgumentsSize() <= 0) {
          throw new FileOperationException("Invalid arguments in CreatePolicy.",
              FOFailReason.INVALID_NODE_GROUPS);
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
            throw new FileOperationException("Invalid node groups set in CreatePolicy.",
                FOFailReason.INVALID_NODE_GROUPS);
          }
        }
      } else {
        ngs = tbl.getNodeGroups();
      }
      // check values now
      if (values == null || values.size() == 0) {
        throw new FileOperationException("Invalid file split values.",
            FOFailReason.INVALID_SPLIT_VALUES);
      }
      List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(tbl
          .getFileSplitKeys());
      List<PartitionInfo> pis = new ArrayList<PartitionInfo>();
      // find the max version
      long version = 0;
      for (PartitionInfo pi : allpis) {
        if (pi.getP_version() > version) {
          version = pi.getP_version();
        }
      }
      if (values.get(0).getVerison() > version) {
        throw new FileOperationException("Invalid Version specified, provide "
            + values.get(0).getVerison() + " expected " + version,
            FOFailReason.INVALID_SPLIT_VALUES);
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
        throw new FileOperationException("File split value should be " + vlen + " entries.",
            FOFailReason.INVALID_SPLIT_VALUES);
      }
      long low = -1,
      high = -1;
      for (int i = 0, j = 0; i < values.size(); i++) {
        SplitValue sv = values.get(i);
        PartitionInfo pi = pis.get(j);

        switch (pi.getP_type()) {
        case none:
        case roundrobin:
        case list:
        case range:
          throw new FileOperationException("Split type " + pi.getP_type()
              + " shouldn't be set values.", FOFailReason.INVALID_SPLIT_VALUES);
        case interval:
          if (low == -1) {
            try {
              low = Long.parseLong(sv.getValue());
            } catch (NumberFormatException e) {
              throw new FileOperationException("Split value expect Long for interval: "
                  + sv.getValue(), FOFailReason.INVALID_SPLIT_VALUES);
            }
            break;
          }
          if (high == -1) {
            try {
              high = Long.parseLong(sv.getValue());
            } catch (NumberFormatException e) {
              throw new FileOperationException("Split value expect Long for interval: "
                  + sv.getValue(), FOFailReason.INVALID_SPLIT_VALUES);
            }
            // check range
            String interval_unit = pi.getArgs().get(0);
            Double d = Double.parseDouble(pi.getArgs().get(1));
            Long interval_seconds = 0L;
            try {
              interval_seconds = PartitionFactory.getIntervalSeconds(interval_unit, d);
            } catch (Exception e) {
              throw new FileOperationException("Handle interval split: internal error.",
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            if (high - low != interval_seconds) {
              throw new FileOperationException("Invalid interval range specified: [" + low + ", "
                  + high +
                  "), expect range length: " + interval_seconds + ".",
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            // unit check
            Long iu = 1L;
            try {
              iu = PartitionFactory.getIntervalUnit(interval_unit);
            } catch (Exception e) {
              throw new FileOperationException("Handle interval split unit: interval error.",
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            if (low % iu != 0) {
              throw new FileOperationException(
                  "The low limit of interval split should be MODed by unit " +
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
              throw new FileOperationException(
                  "Split value for hash except format: 'bucket_size-value' : " + sv.getValue(),
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            v = Long.parseLong(hv[0]);
            if (v != pi.getP_num()) {
              throw new FileOperationException("Split value of hash bucket_size mismatch: expect "
                  + pi.getP_num() + " but provided " + v,
                  FOFailReason.INVALID_SPLIT_VALUES);
            }
            v = Long.parseLong(hv[1]);
          } catch (NumberFormatException e) {
            throw new FileOperationException("Split value expect Long for hash: " + sv.getValue(),
                FOFailReason.INVALID_SPLIT_VALUES);
          }
          if (v < 0 && v >= pi.getP_num()) {
            throw new FileOperationException("Hash value exceeds valid range: [0, " + pi.getP_num()
                + ").", FOFailReason.INVALID_SPLIT_VALUES);
          }
          break;
        }
        // check version, column name here
        if (sv.getVerison() != pi.getP_version() ||
            !pi.getP_col().equalsIgnoreCase(sv.getSplitKeyName())) {
          throw new FileOperationException("SplitKeyName mismatch, please check your metadata.",
              FOFailReason.INVALID_SPLIT_VALUES);
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
        // this means there are many files with this same split value, check if there exists
        // INCREATE file
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
        flp = new FileLocatingPolicy(ngnodes, dm.disabledDevs, FileLocatingPolicy.SPECIFY_NODES,
            FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
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
            flp = new FileLocatingPolicy(ngnodes, dm.disabledDevs, FileLocatingPolicy.RANDOM_NODES,
                FileLocatingPolicy.RANDOM_DEVS, false);
          } else {
            flp = new FileLocatingPolicy(ngnodes, dm.disabledDevs, FileLocatingPolicy.SPECIFY_NODES,
                FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
          }
        }
        break;
      case CREATE_AUX_IDX_FILE:
        // use all available ndoes
        flp = new FileLocatingPolicy(null, dm.disabledDevs, FileLocatingPolicy.EXCLUDE_NODES,
            FileLocatingPolicy.EXCLUDE_DEVS_SHARED, false);
        break;
      default:
        throw new FileOperationException("Invalid create operation provided!",
            FOFailReason.INVALID_FILE);
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
  public boolean create_role(Role role) throws MetaException, TException {
    return __cli.get().create_role(role);
  }

  @Override
  public void create_table(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException,
      TException {
    __cli.get().createTable(tbl);
    // BUG-XXX: we should check whether this TABLE has been created.
    try {
      Table ntbl = null;
      for (int i = 0; ntbl == null && i < 25; i++) {
        ntbl = rs.getTable(tbl.getDbName(), tbl.getTableName());
        if (ntbl != null) {
          break;
        }
        try {
          Thread.sleep(200);
        } catch (Exception e) {
        }
      }
      if (ntbl != null) {
        LOG.info("create_table: " + tbl.getDbName() + "." + tbl.getTableName() + " OK.");
      } else {
        LOG.info("create_table: " + tbl.getDbName() + "." + tbl.getTableName() + " still progressing.");
      }
    } catch (Exception e) {
    }
  }

  @Override
  public void create_table_by_user(Table tbl, User user)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    __cli.get().createTableByUser(tbl, user);
  }

  @Override
  public void create_table_with_environment_context(final Table tbl,
      final EnvironmentContext envContext) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException,
      TException {
    // TODO implement this method
    throw new MetaException("not implemented yet.");
  }

  @Override
  public boolean create_type(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return rs.createType(type);
  }

  @Override
  public boolean create_user(User user) throws InvalidObjectException,
      MetaException, TException {
    return __cli.get().create_user(user);
  }

  @Override
  public boolean del_device(String devId) throws MetaException, TException {
    return __cli.get().delDevice(devId);
  }

  @Override
  public int del_node(String nodeName) throws MetaException, TException {
    return (!isNullOrEmpty(nodeName) && __cli.get().del_node(nodeName)) ? 1 : 0;
  }

  @Override
  public boolean deleteEquipRoom(EquipRoom er) throws MetaException,
      TException {
    return er != null && __cli.get().deleteEquipRoom(er);
  }

  @Override
  public boolean deleteGeoLocation(GeoLocation gl) throws MetaException,
      TException {
    return gl != null && __cli.get().deleteGeoLocation(gl);
  }

  @Override
  public boolean deleteNodeAssignment(String nodeName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    boolean expr = isNullOrEmpty(nodeName) || isNullOrEmpty(dbName);
    return !expr && __cli.get().deleteNodeAssignment(nodeName, dbName);
  }

  @Override
  public boolean deleteNodeGroup(NodeGroup nodeGroup) throws MetaException,
      TException {
    return nodeGroup != null
        && __cli.get().deleteNodeGroup(nodeGroup);
  }

  @Override
  public boolean deleteNodeGroupAssignment(NodeGroup nodeGroup, String dbName)
      throws MetaException, TException {
    return __cli.get().deleteNodeGroupAssignment(nodeGroup,
        dbName);
  }

  @Override
  public boolean deleteRoleAssignment(String roleName, String dbName)
      throws MetaException, NoSuchObjectException, TException {
    return __cli.get()
        .deleteRoleAssignment(roleName, dbName);
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
    return !isNullOrEmpty(arg0)
        && __cli.get().deleteSchema(arg0);
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
    return !expr
        && __cli.get().deleteTableNodeDist(arg0, arg1, arg2);
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
    return !expr
        && __cli.get()
            .deleteUserAssignment(userName, dbName);
  }

  // FIXME kandaozhe
  @Override
  public boolean delete_partition_column_statistics(String dbName, String tableName,
      String partName, String colName) throws NoSuchObjectException,
      MetaException, InvalidObjectException, InvalidInputException,
      TException {
    return __cli.get().deletePartitionColumnStatistics(
        dbName, tableName, partName, colName);
  }

  @Override
  public boolean delete_table_column_statistics(String dbName, String tableName,
      String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException, TException {
    return __cli.get().deleteTableColumnStatistics(dbName,
        tableName, colName);
  }

  @Override
  public void drop_attribution(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException,
      MetaException, TException {
    __cli.get().dropDatabase(name, deleteData,
        ignoreUnknownDb);
  }

  @Override
  public void drop_database(String name, boolean ifDeleteData, boolean ifIgnoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException,
      MetaException, TException {
    __cli.get().dropDatabase(name, ifDeleteData,
        ifIgnoreUnknownDb);
  }

  @Override
  public boolean drop_index_by_name(String dbName, String tableName, String indexName,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return __cli.get().dropIndex(dbName, tableName,
        indexName, ifDeleteData);
  }

  @Override
  public boolean drop_partition(String dbName, String tableName, List<String> partValues,
      boolean ifDeleteData) throws NoSuchObjectException, MetaException,
      TException {
    return __cli.get().dropPartition(dbName, tableName,
        partValues, ifDeleteData);
  }

  @Override
  public boolean drop_partition_by_name(String dbName, String tableName,
      String partName, boolean ifDelData) throws NoSuchObjectException,
      MetaException, TException {
    return __cli.get().dropPartition(dbName, tableName,
        partName, ifDelData);
  }

  @Override
  public int drop_partition_files(Partition part, List<SFile> files)
      throws TException {
    // TODO zy
    return __cli.get().drop_partition_files(part, files);
  }

  @Override
  public boolean drop_partition_index(Index index, Partition part)
      throws MetaException, AlreadyExistsException, TException {
    return __cli.get().drop_partition_index(index, part);
  }

  @Override
  public boolean drop_partition_index_files(Index index, Partition part,
      List<SFile> file) throws MetaException, TException {
    // TODO zy
    return __cli.get().drop_partition_index_files(index,
        part, file);
  }

  @Override
  public boolean drop_role(String roleName) throws MetaException, TException {
    return __cli.get().drop_role(roleName);
  }

  @Override
  public int drop_subpartition_files(Subpartition subpart, List<SFile> files)
      throws TException {
    return __cli.get().drop_subpartition_files(subpart,
        files);
  }

  @Override
  public boolean drop_subpartition_index(Index index, Subpartition subpart)
      throws MetaException, AlreadyExistsException, TException {
    return __cli.get().drop_subpartition_index(index,
        subpart);
  }

  @Override
  public boolean drop_subpartition_index_files(Index index, Subpartition subpart,
      List<SFile> file) throws MetaException, TException {
    // TODO zy
    return __cli.get().drop_subpartition_index_files(index,
        subpart, file);
  }

  @Override
  public void drop_table(String dbName, String tableName, boolean ifDelData)
      throws NoSuchObjectException, MetaException, TException {
    __cli.get().dropTable(dbName, tableName, ifDelData, true);
  }

  @Override
  public boolean drop_type(String name) throws MetaException,
      NoSuchObjectException, TException {
    /*
     * startFunction("drop_type", ": " + name);
     *
     * boolean success = false;
     * Exception ex = null;
     * try {
     * // TODO:pc validate that there are no types that refer to this
     * success = rs.dropType(name);
     * } catch (Exception e) {
     * ex = e;
     * if (e instanceof MetaException) {
     * throw (MetaException) e;
     * } else if (e instanceof NoSuchObjectException) {
     * throw (NoSuchObjectException) e;
     * } else {
     * MetaException me = new MetaException(e.toString());
     * me.initCause(e);
     * throw me;
     * }
     * } finally {
     * endFunction("drop_type", success, ex);
     * }
     * return success;
     */
    // FIXME can not be sent to old metastore
    return false;
  }

  @Override
  public boolean drop_user(String userName) throws NoSuchObjectException,
      MetaException, TException {
    return __cli.get().drop_user(userName);
  }

  @Override
  public List<SFile> filterTableFiles(String dbName, String tabName, List<SplitValue> values)
      throws MetaException, TException {
    return rs.filterTableFiles(dbName, tabName, values);
  }

  @Override
  public List<Node> find_best_nodes(int nr) throws MetaException,
      TException {
    if (nr > 0) {
      try {
        return dm.findBestNodes(nr);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    } else if (nr < 0) {
      // call findBestNodesBySingleDev
      try {
        return dm.findBestNodesBySingleDev(-nr);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
    return new ArrayList<Node>();
  }

  @Override
  public List<Node> find_best_nodes_in_groups(String dbName, String tableName,
      int nr, FindNodePolicy fnp) throws MetaException, TException {
    List<Node> r = new ArrayList<Node>();
    Set<String> fromSet = new TreeSet<String>();
    Table tbl = null;

    tbl = this.get_table(dbName, tableName);
    if (tbl.getNodeGroupsSize() > 0) {
      for (NodeGroup ng : tbl.getNodeGroups()) {
        if (ng.getNodesSize() > 0) {
          switch (fnp) {
          case SINGLE_NG:
          case ALL_NGS:
            for (Node n : ng.getNodes()) {
              fromSet.add(n.getNode_name());
            }
            break;
          }
          if (fnp == FindNodePolicy.SINGLE_NG) {
            break;
          }
        }
      }
    }
    if (fromSet.size() > 0) {
      try {
        r = dm.findBestNodes(fromSet, nr);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
    return r;

  }

  @Override
  public String getDMStatus() throws MetaException, TException {

    return dm.getDMStatus();
  }

  @Override
  public GeoLocation getGeoLocationByName(String geoLocName) throws MetaException,
      NoSuchObjectException, TException {
    // return rs.getGeoLocationByName(geoLocName);
    return __cli.get().getGeoLocationByName(geoLocName);
  }

  @Override
  public List<GeoLocation> getGeoLocationByNames(List<String> geoLocNames)
      throws MetaException, TException {
    // return rs.getGeoLocationByNames(geoLocNames);
    return __cli.get().getGeoLocationByNames(geoLocNames);
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
    if (dm != null) {
      return dm.getNodeInfo();
    }
    return "+FAIL: No DiskManger!\n";
  }

  @Override
  public GlobalSchema getSchemaByName(String schName)
      throws NoSuchObjectException, MetaException, TException {
    GlobalSchema gs = rs.getSchema(schName);
    return gs;
  }

  @Override
  public long getSessionId() throws MetaException, TException {
    // TODO just do this need msss
    return 0L;
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

  // 把所有本地缓存的database返回
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
    // return rs.getAllBusiTypeCols();
    return __cli.get().get_all_busi_type_cols();
  }

  @Override
  public List<BusiTypeDatacenter> get_all_busi_type_datacenters()
      throws MetaException, TException {
    // return rs.get_all_busi_type_datacenters();
    return __cli.get().get_all_busi_type_datacenters();
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
  	startFunction("get_all_databases");

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.getAllDatabases();
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_all_databases", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Node> get_all_nodes() throws MetaException, TException {
    List<Node> ns = new ArrayList<Node>();
    ns.addAll(CacheStore.getNodeHm().values());
    return ns;
  }

  @Override
  public List<String> get_all_tables(String dbName) throws MetaException,
      TException {
  	startFunction("get_all_tables", ": db=" + dbName);

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.getAllTables(dbName);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_all_tables", ret != null, ex);
    }
    return ret;
  }

  @Override
  public Database get_attribution(String name) throws NoSuchObjectException,
      MetaException, TException {
    // TODO zy
    return get_database(name);
  }

  @Override
  public String get_config_value(String name, String defaultValue)
      throws ConfigValSecurityException, TException {
    // TODO rs doesn't implement this method need HiveConf
    return __cli.get().getConfigValue(name, defaultValue);
  }

  @Override
  public Database get_database(String dbName) throws NoSuchObjectException,
      MetaException, TException {
  	startFunction("get_database", ": " + dbName);
    Database db = null;
    Exception ex = null;
    try {
      db = rs.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      ex = e;
      throw e;
    } catch (Exception e) {
      ex = e;
      assert (e instanceof RuntimeException);
      throw (RuntimeException) e;
    } finally {
      endFunction("get_database", db != null, ex);
    }
    return db;
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException,
      TException {
  	startFunction("get_databases", ": " + pattern);

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.getDatabases(pattern);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_databases", ret != null, ex);
    }
    return ret;
  }

  @Override
  public String get_delegation_token(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException {
    // TODO rs doesn't impelement this method need HiveConf
    return __cli.get().getDelegationToken(owner,
        renewerKerberosPrincipalName);
  }

  @Override
  public Device get_device(String devid) throws MetaException, NoSuchObjectException, TException {
    return rs.getDevice(devid);
  }

  @Override
  public List<FieldSchema> get_fields(String db, String tableName)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
  	startFunction("get_fields", ": db=" + db + "tbl=" + tableName);
    String[] names = tableName.split("\\.");
    String base_table_name = names[0];

    Table tbl;
    List<FieldSchema> ret = null;
    Exception ex = null;
    try {
      try {
        tbl = get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      boolean getColsFromSerDe = SerDeUtils.shouldGetColsFromSerDe(
          tbl.getSd().getSerdeInfo().getSerializationLib());
      if (!getColsFromSerDe) {
        ret = tbl.getSd().getCols();
      } else {
        try {
          Deserializer s = MetaStoreUtils.getDeserializer(hiveConf, tbl);
          ret = MetaStoreUtils.getFieldsFromDeserializer(tableName, s);
        } catch (SerDeException e) {
          StringUtils.stringifyException(e);
          throw new MetaException(e.getMessage());
        }
      }
    } catch (Exception e) {
      ex = e;
      if (e instanceof UnknownDBException) {
        throw (UnknownDBException) e;
      } else if (e instanceof UnknownTableException) {
        throw (UnknownTableException) e;
      } else if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_fields", ret != null, ex);
    }

    return ret;
  }

  // for each file, lookup cached device firstly
  private void identifySharedDevice(List<SFileLocation> lsfl) throws MetaException,
      NoSuchObjectException {
    if (lsfl == null) {
      return;
    }
    for (SFileLocation sfl : lsfl) {
      DeviceInfo di = dm.getDeviceInfo(sfl.getDevid());
      if (di == null || di.prop < 0) {
        Device d = rs.getDevice(sfl.getDevid());
        if (DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.SHARED ||
            DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.BACKUP) {
          if (DiskManager.identify_shared_device) {
            StringBuilder sb = new StringBuilder();
            for (String node : dm.getNodesByDevice(d.getDevid())) {
              sb.append(node);
              sb.append(";");
            }
            sfl.setNode_name(sb.toString());
          } else {
            sfl.setNode_name("");
          }
        }
      } else {
        if (di.getType() == MetaStoreConst.MDeviceProp.SHARED ||
            di.getType() == MetaStoreConst.MDeviceProp.BACKUP) {
          if (DiskManager.identify_shared_device) {
            StringBuilder sb = new StringBuilder();
            for (String node : dm.getNodesByDevice(di.dev)) {
              sb.append(node);
              sb.append(";");
            }
            sfl.setNode_name(sb.toString());
          } else {
            sfl.setNode_name("");
          }
        }
      }
    }
  }

  @Override
  public SFile get_file_by_id(long fid) throws FileOperationException,
      MetaException, TException {
    DMProfile.fgetR.incrementAndGet();
    SFile r = rs.getSFile(fid);
    if (r == null) {
      throw new FileOperationException("File not found by id:" + fid, FOFailReason.INVALID_FILE);
    }

    switch (r.getStore_status()) {
    case MetaStoreConst.MFileStoreStatus.RM_LOGICAL:
    case MetaStoreConst.MFileStoreStatus.RM_PHYSICAL:
      if (r.getLocations() != null) {
        r.getLocations().clear();
      }
      break;
    default:
      //r.setLocations(rs.getSFileLocations(fid));
    }
    try {
      identifySharedDevice(r.getLocations());
    } catch (NoSuchObjectException e) {
      LOG.warn("Identify Shared Device failed: no such device.");
      LOG.error(e, e);
    }
    return r;
  }

  @Override
  public SFile get_file_by_name(String node, String devid, String location)
      throws FileOperationException, MetaException, TException {
  	DMProfile.fgetR.incrementAndGet();
    SFile r = rs.getSFile(devid, location);
    if (r == null) {
      throw new FileOperationException("Can not find SFile by name: " + node + ":" + devid + ":"
          + location, FOFailReason.INVALID_FILE);
    }

    switch (r.getStore_status()) {
    case MetaStoreConst.MFileStoreStatus.RM_LOGICAL:
    case MetaStoreConst.MFileStoreStatus.RM_PHYSICAL:
      if (r.getLocations() != null) {
        r.getLocations().clear();
      }
      break;
    default:
//    	r.setLocations(rs.getSFileLocations(r.getFid()));
    }
    try {
      identifySharedDevice(r.getLocations());
    } catch (NoSuchObjectException e) {
      LOG.warn("Identify Shared Device failed: no such device.");
      LOG.error(e, e);
    }

    return r;
  }

  @Override
  public Index get_index_by_name(String dbName, String tblName, String indexName)
      throws MetaException, NoSuchObjectException, TException {
  	startFunction("get_index_by_name", ": db=" + dbName + " tbl="
        + tblName + " index=" + indexName);

    Index ret = null;
    Exception ex = null;
    try {
      ret = get_index_by_name_core(rs, dbName, tblName, indexName);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("drop_index_by_name", ret != null, ex);
    }
    return ret;
  }

  private Index get_index_by_name_core(final RawStore ms, final String db_name,
      final String tbl_name, final String index_name)
      throws MetaException, NoSuchObjectException, TException {
    Index index = ms.getIndex(db_name, tbl_name, index_name);

    if (index == null) {
      throw new NoSuchObjectException(db_name + "." + tbl_name
          + " index=" + index_name + " not found");
    }
    return index;
  }

  @Override
  public List<String> get_index_names(String dbName, String tblName, short maxIndexes)
      throws MetaException, TException {
  	startTableFunction("get_index_names", dbName, tblName);

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.listIndexNames(dbName, tblName, maxIndexes);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_index_names", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Index> get_indexes(String dbName, String tblName, short maxIndexes)
      throws NoSuchObjectException, MetaException, TException {
  	startTableFunction("get_indexes", dbName, tblName);

    List<Index> ret = null;
    Exception ex = null;
    try {
      ret = rs.getIndexes(dbName, tblName, maxIndexes);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_indexes", ret != null, ex);
    }
    return ret;
  }

  @Override
  // MetaStoreClient 初始化时会调这个rpc
  public Database get_local_attribution() throws MetaException, TException {
    String dbname = hiveConf.getVar(HiveConf.ConfVars.LOCAL_ATTRIBUTION);
    try {
      Database db = rs.getDatabase(dbname);
      return db;
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
    // 如果返回null会抛异常
    // org.apache.thrift.TApplicationException: get_local_attribution failed: unknown result
  }

  @Override
  public List<String> get_lucene_index_names(String db_name, String tbl_name, short max_indexes)
      throws MetaException, TException {
    throw new MetaException("Not implemented yet!");
  }

  @Override
  public Node get_node(String nodeName) throws MetaException, TException {
    return rs.getNode(nodeName);
  }

  @Override
  public Partition get_partition(final String db_name, final String tbl_name,
      final List<String> part_vals)
      throws MetaException, NoSuchObjectException, TException {
  	startPartitionFunction("get_partition", db_name, tbl_name, part_vals);

    Partition ret = null;
    Exception ex = null;
    try {
      // TODO: fix it
      ret = rs.getPartition(db_name, tbl_name, part_vals);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partition", ret != null, ex);
    }
    return ret;
  }

  @Override
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
      throws MetaException, NoSuchObjectException, TException {
  	startFunction("get_partition_by_name", ": db=" + db_name + " tbl="
        + tbl_name + " part=" + part_name);

    Partition ret = null;
    Exception ex = null;
    try {
      ret = get_partition_by_name_core(rs, db_name, tbl_name, part_name);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partition_by_name", ret != null, ex);
    }
    return ret;
  }

  private Partition get_partition_by_name_core(final RawStore ms, final String db_name,
      final String tbl_name, final String part_name)
      throws MetaException, NoSuchObjectException, TException {
    Partition p = ms.getPartition(db_name, tbl_name, part_name);

    if (p == null) {
      throw new NoSuchObjectException(db_name + "." + tbl_name
          + " partition (" + part_name + ") not found");
    }
    return p;
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(String dbName,
      String tableName, String partitionName, String colName)
      throws NoSuchObjectException, MetaException, InvalidInputException,
      InvalidObjectException, TException {
    // TODO copy from HiveMetaStore
    // return rs.getPartitionColumnStatistics(dbName, tableName, partitionName, null, colName);
    return __cli.get().getPartitionColumnStatistics(dbName,
        tableName, partitionName, colName);
  }

  @Override
  public List<SFileRef> get_partition_index_files(Index index, Partition part)
      throws MetaException, TException {
    // return rs.getPartitionIndexFiles(index, part);
    return __cli.get()
        .get_partition_index_files(index, part);
  }

  @Override
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
      throws MetaException, TException {
  	startTableFunction("get_partition_names", db_name, tbl_name);

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.listPartitionNames(db_name, tbl_name, max_parts);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partition_names", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<String> get_partition_names_ps(final String db_name,
      final String tbl_name, final List<String> part_vals, final short max_parts) throws MetaException,
      NoSuchObjectException, TException {
  	startPartitionFunction("get_partitions_names_ps", db_name, tbl_name, part_vals);
    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.listPartitionNamesPs(db_name, tbl_name, part_vals, max_parts);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions_names_ps", ret != null, ex);
    }
    return ret;
  }

  @Override
  public Partition get_partition_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final String user_name, final List<String> group_names)
      throws MetaException, NoSuchObjectException, TException {
  	startPartitionFunction("get_partition_with_auth", db_name, tbl_name,
        part_vals);

    Partition ret = null;
    Exception ex = null;
    try {
      ret = rs.getPartitionWithAuth(db_name, tbl_name, part_vals,
          user_name, group_names);
    } catch (InvalidObjectException e) {
      ex = e;
      throw new NoSuchObjectException(e.getMessage());
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partition_with_auth", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
  	startTableFunction("get_partitions", db_name, tbl_name);

    List<Partition> ret = null;
    Exception ex = null;
    try {
      ret = rs.getPartitions(db_name, tbl_name, max_parts);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Partition> get_partitions_by_filter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException,
      NoSuchObjectException, TException {
  	startTableFunction("get_partitions_by_filter", dbName, tblName);

    List<Partition> ret = null;
    Exception ex = null;
    try {
      ret = rs.getPartitionsByFilter(dbName, tblName, filter, maxParts);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions_by_filter", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Partition> get_partitions_by_names(String tblName, String dbName,
      List<String> partNames) throws MetaException, NoSuchObjectException,
      TException {
  	 startTableFunction("get_partitions_by_names", dbName, tblName);

     List<Partition> ret = null;
     Exception ex = null;
     try {
       ret = rs.getPartitionsByNames(dbName, tblName, partNames);
     } catch (Exception e) {
       ex = e;
       if (e instanceof MetaException) {
         throw (MetaException) e;
       } else if (e instanceof NoSuchObjectException) {
         throw (NoSuchObjectException) e;
       } else if (e instanceof TException) {
         throw (TException) e;
       } else {
         MetaException me = new MetaException(e.toString());
         me.initCause(e);
         throw me;
       }
     } finally {
       endFunction("get_partitions_by_names", ret != null, ex);
     }
     return ret;
  }

  @Override
  public List<Partition> get_partitions_ps(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts) throws MetaException,
      NoSuchObjectException, TException {
  	startPartitionFunction("get_partitions_ps", db_name, tbl_name, part_vals);

    List<Partition> ret = null;
    Exception ex = null;
    try {
      ret = get_partitions_ps_with_auth(db_name, tbl_name, part_vals,
          max_parts, null, null);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions_ps", ret != null, ex);
    }

    return ret;
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(final String db_name,
      final String tbl_name, final List<String> part_vals,
      final short max_parts, final String userName,
      final List<String> groupNames) throws NoSuchObjectException, MetaException,
      TException {
  	startPartitionFunction("get_partitions_ps_with_auth", db_name, tbl_name,
        part_vals);
    List<Partition> ret = null;
    Exception ex = null;
    try {
      ret = rs.listPartitionsPsWithAuth(db_name, tbl_name, part_vals, max_parts,
          userName, groupNames);
    } catch (InvalidObjectException e) {
      ex = e;
      throw new MetaException(e.getMessage());
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions_ps_with_auth", ret != null, ex);
    }
    return ret;
  }

  @Override
  public List<Partition> get_partitions_with_auth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws NoSuchObjectException, MetaException, TException {
  	startTableFunction("get_partitions_with_auth", dbName, tblName);

    List<Partition> ret = null;
    Exception ex = null;
    try {
      ret = rs.getPartitionsWithAuth(dbName, tblName, maxParts,
          userName, groupNames);
    } catch (InvalidObjectException e) {
      ex = e;
      throw new NoSuchObjectException(e.getMessage());
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else if (e instanceof TException) {
        throw (TException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_partitions_with_auth", ret != null, ex);
    }
    return ret;
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException, TException {
    /*
     * if (hiveObject.getObjectType() == HiveObjectType.COLUMN) {
     * String partName = getPartName(hiveObject);
     * return rs.getColumnPrivilegeSet(hiveObject.getDbName(), hiveObject
     * .getObjectName(), partName, hiveObject.getColumnName(), userName,
     * groupNames);
     * } else if (hiveObject.getObjectType() == HiveObjectType.PARTITION) {
     * String partName = getPartName(hiveObject);
     * return rs.getPartitionPrivilegeSet(hiveObject.getDbName(),
     * hiveObject.getObjectName(), partName, userName, groupNames);
     * } else if (hiveObject.getObjectType() == HiveObjectType.DATABASE) {
     * return rs.getDBPrivilegeSet(hiveObject.getDbName(), userName,
     * groupNames);
     * } else if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
     * return rs.getTablePrivilegeSet(hiveObject.getDbName(), hiveObject
     * .getObjectName(), userName, groupNames);
     * } else if (hiveObject.getObjectType() == HiveObjectType.GLOBAL) {
     * return rs.getUserPrivilegeSet(userName, groupNames);
     * }
     */
    return __cli.get().get_privilege_set(hiveObject,
        userName, groupNames);
  }

  private String getPartName(HiveObjectRef hiveObject) throws MetaException {
    String partName = null;
    List<String> partValue = hiveObject.getPartValues();
    if (partValue != null && partValue.size() > 0) {
      Table table = rs.getTable(hiveObject.getDbName(), hiveObject
          .getObjectName());
      partName = Warehouse
          .makePartName(table.getPartitionKeys(), partValue);
    }
    return partName;
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    // return rs.listRoleNames();
    return __cli.get().listRoleNames();
  }

  @Override
  // 模仿HiveMetaStore中的方法写的
  public List<FieldSchema> get_schema(String db, String tableName)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
  	startFunction("get_schema", ": db=" + db + "tbl=" + tableName);
    boolean success = false;
    Exception ex = null;
    try {
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];

      Table tbl;
      try {
        tbl = get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      List<FieldSchema> fieldSchemas = get_fields(db, base_table_name);

      if (tbl == null || fieldSchemas == null) {
        throw new UnknownTableException(tableName + " doesn't exist");
      }

      if (tbl.getPartitionKeys() != null) {
        // Combine the column field schemas and the partition keys to create the
        // whole schema
        fieldSchemas.addAll(tbl.getPartitionKeys());
      }
      success = true;
      return fieldSchemas;
    } catch (Exception e) {
      ex = e;
      if (e instanceof UnknownDBException) {
        throw (UnknownDBException) e;
      } else if (e instanceof UnknownTableException) {
        throw (UnknownTableException) e;
      } else if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_schema", success, ex);
    }
  }

  @Override
  public List<SFileRef> get_subpartition_index_files(Index index,
      Subpartition subpart) throws MetaException, TException {
    // return rs.getSubpartitionIndexFiles(index, subpart);
    return __cli.get().get_subpartition_index_files(index,
        subpart);
  }

  @Override
  public List<Subpartition> get_subpartitions(String dbName, String tabName,
      Partition part) throws TException {
    return rs.getSubpartitions(dbName, tabName, part);
  }

  @Override
  public Table get_table(String dbname, String name) throws MetaException,
      NoSuchObjectException, TException {
  	Table t = null;
    startTableFunction("get_table", dbname, name);
    Exception ex = null;
    try {
      t = rs.getTable(dbname, name);
      if (t == null) {
        throw new NoSuchObjectException(dbname + "." + name
            + " table not found");
      }
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof NoSuchObjectException) {
        throw (NoSuchObjectException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_table", t != null, ex);
    }
    return t;
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String dbName,
      String tableName, String colName) throws NoSuchObjectException,
      MetaException, InvalidInputException, InvalidObjectException,
      TException {
    // return rs.getTableColumnStatistics(dbName, tableName, colName);
    return __cli.get().getTableColumnStatistics(dbName,
        tableName, colName);
  }

  @Override
  public List<String> get_table_names_by_filter(String dbName, String filter, short maxTables)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
  	List<String> tables = null;
    startFunction("get_table_names_by_filter", ": db = " + dbName + ", filter = " + filter);
    Exception ex = null;
    try {
      if (dbName == null || dbName.isEmpty()) {
        throw new UnknownDBException("DB name is null or empty");
      }
      if (filter == null) {
        throw new InvalidOperationException(filter + " cannot apply null filter");
      }
      tables = rs.listTableNamesByFilter(dbName, filter, maxTables);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof InvalidOperationException) {
        throw (InvalidOperationException) e;
      } else if (e instanceof UnknownDBException) {
        throw (UnknownDBException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_table_names_by_filter", tables != null, ex);
    }
    return tables;
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> names)
      throws MetaException, InvalidOperationException, UnknownDBException,
      TException {
  	List<Table> tables = null;
    startMultiTableFunction("get_multi_table", dbname, names);
    Exception ex = null;
    try {

      if (dbname == null || dbname.isEmpty()) {
        throw new UnknownDBException("DB name is null or empty");
      }
      if (names == null)
      {
        throw new InvalidOperationException(dbname + " cannot find null tables");
      }
      tables = rs.getTableObjectsByName(dbname, names);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else if (e instanceof InvalidOperationException) {
        throw (InvalidOperationException) e;
      } else if (e instanceof UnknownDBException) {
        throw (UnknownDBException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_multi_table", tables != null, ex);
    }
    return tables;
  }

  @Override
  public List<String> get_tables(String dbname, String pattern)
      throws MetaException, TException {
  	startFunction("get_tables", ": db=" + dbname + " pat=" + pattern);

    List<String> ret = null;
    Exception ex = null;
    try {
      ret = rs.getTables(dbname, pattern);
    } catch (Exception e) {
      ex = e;
      if (e instanceof MetaException) {
        throw (MetaException) e;
      } else {
        MetaException me = new MetaException(e.toString());
        me.initCause(e);
        throw me;
      }
    } finally {
      endFunction("get_tables", ret != null, ex);
    }
    return ret;
  }

  @Override
  public Type get_type(String typeName) throws MetaException,
      NoSuchObjectException, TException {
    // return rs.getType(typeName);
    // FIXME can not be sent to old metastore
    return null;
  }

  @Override
  public Map<String, Type> get_type_all(String name) throws MetaException,
      TException {
    // FIXME HiveMetaStore just do this
  	 startFunction("get_type_all", ": " + name);
     endFunction("get_type_all", false, null);
    throw new MetaException("not yet implemented");
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException,
      TException {
    // return rs.grantPrivileges(privileges);
    return __cli.get().grant_privileges(privileges);
  }

  @Override
  public boolean grant_role(String role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, TException {
    // return rs.grantRole(rs.getRole(role), userName, principalType, grantor,
    // grantorType,grantOption);
    return __cli.get().grant_role(role, userName,
        principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, NoSuchObjectException, UnknownDBException,
      UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    // return rs.isPartitionMarkedForEvent(dbName, tblName, partName, evtType);
    return __cli.get().isPartitionMarkedForEvent(dbName,
        tblName, partName, evtType);
  }

  @Override
  public List<NodeGroup> listDBNodeGroups(String dbName) throws MetaException,
      TException {
    // return rs.listDBNodeGroups(dbName);
    // 缓存的database对象中没有nodegroup的信息
    return __cli.get().listDBNodeGroups(dbName);
  }

  @Override
  public List<EquipRoom> listEquipRoom() throws MetaException, TException {
    // return rs.listEquipRoom();
    return __cli.get().listEquipRoom();
  }

  @Override
  public List<Long> listFilesByDigest(String digest) throws MetaException,
      TException {
  	startFunction("listFilesByDigest:", "digest: " + digest);
    return rs.findSpecificDigestFiles(digest);
  }

  @Override
  public List<GeoLocation> listGeoLocation() throws MetaException, TException {
    // return rs.listGeoLocation();
    return __cli.get().listGeoLocation();
  }

  @Override
  public List<NodeGroup> listNodeGroupByNames(List<String> ngNames)
      throws MetaException, TException {
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
    // return rs.listRoles();
    return __cli.get().listRoles();
  }

  @Override
  public List<GlobalSchema> listSchemas() throws MetaException, TException {
    List<GlobalSchema> gss = new ArrayList<GlobalSchema>();
    gss.addAll(CacheStore.getGlobalSchemaHm().values());
    return gss;
  }

  /*
   * void setRange(long fromIncl, long toExcl)
   *
   * Set the range of results to return. The execution of the query is modified to return only a
   * subset of results. If the filter would normally return 100 instances, and fromIncl is set to
   * 50,
   * and toExcl is set to 70, then the first 50 results that would have been returned are skipped,
   * the next 20 results are returned and the remaining 30 results are ignored. An implementation
   * should
   * execute the query such that the range algorithm is done at the data store.
   *
   * Parameters:
   * fromIncl - 0-based inclusive start index
   * toExcl - 0-based exclusive end index, or Long.MAX_VALUE for no limit.
   * Since:
   * 2.0
   *
   * 返回的结果集中包含from，不包含to，一共返回from-to个元素
   * 而zrange的两边是inclusive
   */
  @Override
  public List<Long> listTableFiles(String dbName, String tabName, int from, int to)
      throws MetaException, TException {

    return rs.listTableFiles(dbName, tabName, from, to - 1);
  }

  @Override
  public List<NodeGroup> listTableNodeDists(String dbName, String tabName)
      throws MetaException, TException {
    Table t = get_table(dbName, tabName);
    if (t == null) {
      throw new MetaException("No table found by dbname:" + dbName + ", tableName:" + tabName);
    }
    return t.getNodeGroups();
  }

  @Override
  public List<User> listUsers() throws MetaException, TException {
    // return rs.listUsers();
    return __cli.get().listUsers();
  }

  @Override
  public List<Device> list_device() throws MetaException, TException {
    return rs.listDevice();
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principalName,
      PrincipalType principalType, HiveObjectRef hiveObject) throws MetaException,
      TException {
    return __cli.get().list_privileges(principalName,
        principalType, hiveObject);
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType)
      throws MetaException, TException {
    // return rs.listRoles();
    return __cli.get().list_roles(principalName,
        principalType);
  }

  @Override
  public List<String> list_users(Database dbName) throws MetaException,
      TException {
    // return rs.listUsersNames(dbName.getName());
    return __cli.get().list_users(dbName);
  }

  @Override
  public List<String> list_users_names() throws MetaException, TException {
    // return rs.listUsersNames();
    return __cli.get().list_users_names();
  }

  @Override
  public void markPartitionForEvent(String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, NoSuchObjectException, UnknownDBException,
      UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    // rs.markPartitionForEvent(dbName, tblName, partVals, evtType);
    __cli.get().markPartitionForEvent(dbName, tblName,
        partVals, evtType);
  }

  @Override
  public boolean migrate2_in(Table tbl, List<Partition> parts,
      List<Index> idxs, String from_dc, String to_nas_devid,
      Map<Long, SFileLocation> fileMap) throws MetaException, TException {
    return __cli.get().migrate2_in(tbl, parts, idxs,
        from_dc, to_nas_devid, fileMap);
  }

  @Override
  public List<SFileLocation> migrate2_stage1(String dbName, String tableName,
      List<String> partNames, String to_dc) throws MetaException, TException {
    return __cli.get().migrate2_stage1(dbName, tableName,
        partNames, to_dc);
  }

  @Override
  public boolean migrate2_stage2(String dbName, String tableName, List<String> partNames,
      String to_dc, String to_db, String to_nas_devid) throws MetaException,
      TException {
    return __cli.get().migrate2_stage2(dbName, tableName,
        partNames, to_dc, to_db, to_nas_devid);
  }

  private boolean isTopAttribution(){
    return hiveConf.getBoolVar(HiveConf.ConfVars.IS_TOP_ATTRIBUTION);
  }

  /**
   * migrate_in
   *
   * 2015-01-20: do migrate_in for GJZX migration to FenZhongXin
   *
   * SFL's node_name should be set by caller!
   */
  @Override
  public boolean migrate_in(Table tbl, Map<Long, SFile> files,
      List<Index> idxs, String from_db, String to_devid,
      Map<Long, SFileLocation> fileMap) throws MetaException, TException {

    LOG.info("Migrate IN: recv " + files.size() + " files from DB:" +
      from_db + " to DB:" + tbl.getDbName() + "." + tbl.getTableName() +
      ", fileMap size=" + fileMap.size());

    // BUG-XXX: should check to_devid here!
    if (dm.getDeviceInfo(to_devid) == null) {
      LOG.error("Target device " + to_devid + " doesn't exist.");
      return false;
    }

    // try to find and create the database, if it doesn't exist
      try {
        rs.getDatabase(tbl.getDbName());
      } catch (NoSuchObjectException e) {
        // get the db from top attribution
        Database db = null;

        if (isTopAttribution()) {
          throw new MetaException("Top Attribution '" + HiveConf.ConfVars.TOP_ATTRIBUTION + "' rejects any data migrations.");
        } else {
          if (HMSHandler.topdcli == null) {
            // connect firstly
            HiveMetaStore.connect_to_top_attribution(hiveConf);
            if (HMSHandler.topdcli == null) {
              throw new MetaException("Top-level attribution metastore is null, please check!");
            }
          }
          synchronized (HMSHandler.topdcli) {
            db = HMSHandler.topdcli.get_attribution(tbl.getDbName());
          }
        }

        if (db != null) {
          rs.createDatabase(db);
        }
        LOG.info("Create database " + tbl.getDbName() + " done locally.");
      }

      // try to get old schema from top attribution
      GlobalSchema schema = null;
      try {
        synchronized (HMSHandler.topdcli) {
          schema = HMSHandler.topdcli.getSchemaByName(tbl.getSchemaName());
        }
        // create schema in local DB
        if (!createSchema(schema)) {
          LOG.error("Create schema " + tbl.getSchemaName() + " failed.");
          throw new MetaException("Create Schema " + tbl.getSchemaName() + " failed.");
        }
      } catch (NoSuchObjectException e) {
        LOG.error(e, e);
        throw new MetaException("Get schema failed: " + e.getMessage());
      } catch (AlreadyExistsException e) {
        // BUG-XXX: when update schema, all tables changed. There is a bug on
        // table change when more than one table exist in a database.
        /*// update it
        if (!modifySchema(tbl.getSchemaName(), schema)) {
          LOG.error("Modify schema " + tbl.getSchemaName() + " failed.");
          throw new MetaException("Modify Schema " + tbl.getSchemaName() + " failed.");
        }
        */
      }

      // try to create the table, if it doesn't exist
      // BUG-XXX: user should set valid node groups
      String hFlag = "Create";
      try {
        create_table(tbl);
      } catch (AlreadyExistsException e) {
        // it is ok, ignore it? alter_table?
        // BUG-XXX: do not update fileSplitKeys
        tbl.setFileSplitKeys(new ArrayList<FieldSchema>());
        alter_table(tbl.getDbName(), tbl.getTableName(), tbl);
        hFlag = "Alter";
      } catch (InvalidObjectException e) {
        LOG.error(e, e);
        throw new MetaException("Bad table object: check it: " + e.getMessage());
      }
      LOG.info(hFlag + " table " + tbl.getTableName() + " done.");

      // try to create the idxs, if they don't exist
      if (idxs != null) {
        for (Index i : idxs) {
          try {
            add_index(i, null);
          } catch (AlreadyExistsException e) {
            // it is ok, ignore it
          }
        }
      }
      LOG.info("Create or ignore indexs: " + idxs.size() + " done.");

      // try to create the file now, without any active locations.
      Set<SFile> fileToDel = new TreeSet<SFile>();
      Set<SFileLocation> sflToDel = new TreeSet<SFileLocation>();
      Map<Long, SFile> oldFidToNewFile = new HashMap<Long, SFile>();

      for (Map.Entry<Long, SFileLocation> entry : fileMap.entrySet()) {
        SFileLocation sfl = entry.getValue();

        try {
          // FIXME: v0.2 to fix: add file values here!
          SFile nfile = create_file_wo_location(1, tbl.getDbName(), tbl.getTableName(),
              files.get(entry.getKey()).getValues());
          fileToDel.add(nfile);
          LOG.info("Add NEW SFILE " + nfile.getFid());

          SFile ofile = files.get(entry.getKey());
          if (ofile != null) {
            nfile.setLength(ofile.getLength());
            nfile.setAll_record_nr(ofile.getAll_record_nr());
            nfile.setRep_nr(ofile.getRep_nr());
            nfile.setDigest(ofile.getDigest());
            nfile.setRecord_nr(ofile.getRecord_nr());
          }
          // NOTE-XXX: node_name should be set by caller!
          sfl.setDevid(to_devid);
          sfl.setFid(nfile.getFid());
          sfl.setRep_id(0);
          sfl.setUpdate_time(System.currentTimeMillis());
          sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);

          LOG.info("Add NEW SFL DEV " + sfl.getDevid() + ", LOC " + sfl.getLocation());
          if (!rs.createFileLocation(sfl)) {
            LOG.info("[ROLLBACK] Failed to create SFL " + sfl.getDevid() + ", LOC " + sfl.getLocation());
            migrate_rollback_tool(fileToDel, sflToDel);
            return false;
          } else {
            sflToDel.add(sfl);
            List<SFileLocation> locations = new ArrayList<SFileLocation>();
            locations.add(sfl);
            nfile.setLocations(locations);
            oldFidToNewFile.put(entry.getKey(), nfile);
          }
        } catch (Exception e) {
          LOG.error(e, e);
          migrate_rollback_tool(fileToDel, sflToDel);
          throw new MetaException(e.getMessage());
        }
      }

      // close the files
      for (Map.Entry<Long, SFile> entry : oldFidToNewFile.entrySet()) {
        close_file(entry.getValue());
      }

      LOG.info("OK, we will migrate Attribution " + from_db + " Table " +
          tbl.getTableName() + "'s " + fileMap.size() + " files to local attribution.");

      return true;
  }

  private void migrate_rollback_tool(Set<SFile> fileToDel, Set<SFileLocation> sflToDel) throws MetaException {
    // rollback to delete all files and locations
    for (SFileLocation fl : sflToDel) {
      rs.delSFileLocation(fl.getDevid(), fl.getLocation());
    }
    for (SFile f : fileToDel) {
      rs.delSFile(f.getFid());
    }
  }

  @Override
  public List<SFileLocation> migrate_stage1(String dbName, String tableName,
      List<Long> partNames, String to_dc) throws MetaException, TException {
    return __cli.get().migrate_stage1(dbName, tableName,
        partNames, to_dc);
  }

  @Override
  public boolean migrate_stage2(String dbName, String tableName, List<Long> files,
      String from_db, String to_db, String to_devid, String user, String password)
      throws MetaException, TException {
    return client
        .migrate_stage2(dbName, tableName, files, from_db, to_db, to_devid, user, password);
  }

  @Override
  public boolean modifyEquipRoom(EquipRoom er) throws MetaException,
      TException {
    // return rs.modifyEquipRoom(er);
    return __cli.get().modifyEquipRoom(er);
  }

  @Override
  public boolean modifyGeoLocation(GeoLocation gl) throws MetaException,
      TException {
    // return rs.modifyGeoLocation(gl);
    return __cli.get().modifyGeoLocation(gl);
  }

  @Override
  public boolean modifyNodeGroup(String schemaName, NodeGroup ng)
      throws MetaException, TException {
    // return rs.modifyNodeGroup(schemaName, ng);
    return __cli.get().modifyNodeGroup(schemaName, ng);
  }

  @Override
  public boolean modifySchema(String schemaName, GlobalSchema schema)
      throws MetaException, TException {
    // return rs.modifySchema(schemaName, schema);
    return __cli.get().modifySchema(schemaName, schema);
  }

  @Override
  public Device modify_device(Device dev, Node node) throws MetaException,
      TException {
    // return rs.modifyDevice(dev, node);
    return __cli.get().changeDeviceLocation(dev, node);
  }

  @Override
  public boolean modify_user(User user) throws NoSuchObjectException,
      MetaException, TException {
    // return rs.modifyUser(user);
    return __cli.get().modify_user(user);
  }

  @Override
  public boolean offline_filelocation(SFileLocation sfl) throws MetaException, TException {
    // try to update the OFFLINE flag immediately
    startFunction("offline_filelocation:", "dev " + sfl.getDevid() + " loc " + sfl.getLocation());
    sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.OFFLINE);
    rs.updateSFileLocation(sfl);
    endFunction("offline_filelocation", true, null);

    return true;
  }

  @Override
  public boolean online_filelocation(SFile file) throws MetaException, TException {
    // reget the file now
    SFile stored_file = get_file_by_id(file.getFid());

    try {
      if (stored_file.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
        throw new MetaException("online filelocation can only do on INCREATE file " + file.getFid() +
            " STATE: " + stored_file.getStore_status());
      }
      if (stored_file.getLocationsSize() != 1) {
        throw new MetaException("Invalid file location in SFile fid: " + stored_file.getFid());
      }
      SFileLocation sfl = stored_file.getLocations().get(0);
      assert sfl != null;
      sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
      rs.updateSFileLocation(sfl);
    } catch (MetaException e) {
      LOG.error(e, e);
      throw e;
    }

    return true;
  }


  @Override
  public Map<String, String> partition_name_to_spec(String part_name)
      throws MetaException, TException {
    // if (part_name.length() == 0) {
    // return new HashMap<String, String>();
    // }
    // return Warehouse.makeSpecFromName(part_name);
    return __cli.get().partitionNameToSpec(part_name);
  }

  @Override
  public List<String> partition_name_to_vals(String part_name)
      throws MetaException, TException {
    // if (part_name.length() == 0) {
    // return new ArrayList<String>();
    // }
    // LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(part_name);
    // List<String> part_vals = new ArrayList<String>();
    // part_vals.addAll(map.values());
    // return part_vals;
    return __cli.get().partitionNameToVals(part_name);
  }

  @Override
  public String pingPong(String str) throws MetaException, TException {
    return str;
  }

  @Override
  public void rename_partition(String dbname, String name, List<String> part_vals,
      Partition newPart) throws InvalidOperationException, MetaException,
      TException {
    // FIXME just use client
    __cli.get().renamePartition(dbname, name, part_vals,
        newPart);
  }

  @Override
  public long renew_delegation_token(String tokenStrForm) throws MetaException,
      TException {
    return __cli.get().renewDelegationToken(tokenStrForm);
  }

  @Override
  public boolean reopen_file(long fid) throws FileOperationException, MetaException, TException {
    DMProfile.freopenR.incrementAndGet();
    startFunction("reopen_file ", "fid: " + fid);
    boolean success = false;

    try {
      SFile saved = rs.getSFile(fid);

      if (saved == null) {
        throw new FileOperationException("Can not find SFile by FID " + fid,
            FOFailReason.NOTEXIST);
      }
      // check if this file is in REPLICATED state, otherwise, complain about that.
      switch (saved.getStore_status()) {
      case MetaStoreConst.MFileStoreStatus.INCREATE:
        throw new FileOperationException("SFile " + fid + " has already been in INCREATE state.",
            FOFailReason.INVALID_STATE);
      case MetaStoreConst.MFileStoreStatus.CLOSED:
        throw new FileOperationException("SFile " + fid + " is in CLOSE state, please wait.",
            FOFailReason.INVALID_STATE);
      case MetaStoreConst.MFileStoreStatus.REPLICATED:
        // FIXME: seq reopenSFiles
        synchronized (MetaStoreConst.file_reopen_lock) {
          success = rs.reopenSFile(saved);
        }
        break;
      case MetaStoreConst.MFileStoreStatus.RM_LOGICAL:
      case MetaStoreConst.MFileStoreStatus.RM_PHYSICAL:
        throw new FileOperationException("SFile " + fid + " is in RM-* state, reject all reopens.",
            FOFailReason.INVALID_STATE);
      }
    } catch (FileOperationException e) {
      LOG.error(e.getMessage());
      throw e;
    }
    return success;
  }

  @Override
  public int restore_file(SFile file) throws FileOperationException, MetaException, TException {
    DMProfile.frestoreR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID" + file.getFid(),
          FOFailReason.INVALID_FILE);
    }

    if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_LOGICAL) {
      throw new FileOperationException("File StoreStatus is not in RM_LOGICAL.",
          FOFailReason.INVALID_STATE);
    }
    saved.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
    rs.updateSFile(saved);
    return 0;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException,
      TException {
    // return rs.revokePrivileges(privileges);
    return __cli.get().revoke_privileges(privileges);
  }

  @Override
  public boolean revoke_role(String role, String userName, PrincipalType principalType)
      throws MetaException, TException {
    // return rs.revokeRole(rs.getRole(role), userName, principalType);
    return __cli.get().revoke_role(role, userName,
        principalType);
  }

  @Override
  public int rm_file_logical(SFile file) throws FileOperationException,
      MetaException, TException {
    DMProfile.frmlR.incrementAndGet();
    SFile saved = rs.getSFile(file.getFid());
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID" + file.getFid(),
          FOFailReason.INVALID_FILE);
    }

    // only in REPLICATED state can step into RM_LOGICAL
    if (saved.getStore_status() != MetaStoreConst.MFileStoreStatus.REPLICATED) {
      throw new FileOperationException("File StoreStatus is not in REPLICATED.",
          FOFailReason.INVALID_STATE);
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
      throw new FileOperationException("Can not find SFile by FID " + file.getFid(),
          FOFailReason.INVALID_FILE);
    }

    if (saved.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
      LOG.info("Try to set REPLICATED to fid " + file.getFid() + " to physically deleted it.");
      saved.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
      saved = rs.updateSFile(saved);
    }

    if (!(saved.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
        saved.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_LOGICAL)) {
      throw new FileOperationException(
          "File StoreStatus is not in INCREATE/REPLICATED/RM_LOGICAL.", FOFailReason.INVALID_STATE);
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
  public void set_file_repnr(long fid, int repnr) throws FileOperationException, TException {
    startFunction("set_file_repnr", "fid " + fid + " repnr " + repnr);
    SFile f = get_file_by_id(fid);
    if (f != null) {
      f.setRep_nr(repnr);
      // FIXME: Caution, this might be a conflict code section for concurrent sfile field
      // modification.
      rs.updateSFile(f);
    }
  }

  @Override
  public boolean set_loadstatus_bad(long fid) throws MetaException, TException {
    startFunction("set_loadstatus_bad", "fid " + fid);
    SFile saved = rs.getSFile(fid);
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + fid,
          FOFailReason.INVALID_FILE);
    }

    DMProfile.loadStatusBad.incrementAndGet();
    saved.setLoad_status(MetaStoreConst.MFileLoadStatus.BAD);
    rs.updateSFile(saved);

    if (saved.getLocationsSize() > 0) {
      SFileLocation sfl = saved.getLocations().get(0);
      SysMonitor.lsba.watchLSB(fid, saved.getDbName(), saved.getTableName(),
          sfl.getNode_name(), sfl.getDevid());
    }
    return true;
  }

  @Override
  public List<String> set_ugi(String username, List<String> list)
      throws MetaException, TException {
    Collections.addAll(list, username);
    return list;
  }

  @Override
  public List<Busitype> showBusitypes() throws InvalidObjectException,
      MetaException, TException {
    // return rs.showBusitypes();
    return __cli.get().showBusitypes();
  }

  @Override
  public statfs statFileSystem(long from, long to) throws MetaException,
      TException {
    // TODO zy
    // return rs.statFileSystem(from, to);
    return __cli.get().statFileSystem(from, to);
  }

  @Override
  public boolean toggle_safemode() throws MetaException, TException {
    if (dm != null) {
      synchronized (dm) {
        dm.safeMode = !dm.safeMode;
      }
    }
    return true;
  }

  @Override
  public void truncTableFiles(String dbName, String tabName) throws MetaException, TException {
     startFunction("truncTableFiles", "DB: " + dbName + " Table: " + tabName);
     try {
    	 rs.truncTableFiles(dbName, tabName);
     } finally {
    	 endFunction("truncTableFiles", true, null);
     }
  }

  @Override
  public void update_attribution(Database db) throws NoSuchObjectException,
      InvalidOperationException, MetaException, TException {
    // rs.alterDatabase(db.getName(), db);
    __cli.get().update_attribution(db);
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics colStats)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, InvalidInputException, TException {
    return __cli.get().updatePartitionColumnStatistics(
        colStats);
    /*
     * String dbName = null;
     * String tableName = null;
     * String partName = null;
     * String colName = null;
     *
     * ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
     * dbName = statsDesc.getDbName().toLowerCase();
     * tableName = statsDesc.getTableName().toLowerCase();
     * partName = lowerCaseConvertPartName(statsDesc.getPartName());
     *
     * statsDesc.setDbName(dbName);
     * statsDesc.setTableName(tableName);
     * statsDesc.setPartName(partName);
     *
     * long time = System.currentTimeMillis() / 1000;
     * statsDesc.setLastAnalyzed(time);
     *
     * List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
     *
     * for (ColumnStatisticsObj statsObj : statsObjs) {
     * colName = statsObj.getColName().toLowerCase();
     * statsObj.setColName(colName);
     * startFunction("write_partition_column_statistics:  db=" + dbName + " table=" + tableName +
     * " part=" + partName + "column=" + colName, "");
     * }
     *
     * colStats.setStatsDesc(statsDesc);
     * colStats.setStatsObj(statsObjs);
     *
     * boolean ret = false;
     *
     * try {
     * List<String> partVals = getPartValsFromName(rs, dbName,
     * tableName, partName);
     * ret = rs.updatePartitionColumnStatistics(colStats, partVals);
     * return ret;
     * } finally {
     * endFunction("write_partition_column_statistics: ", ret != false, null);
     * }
     */
  }

  private String lowerCaseConvertPartName(String partName) throws MetaException {
    boolean isFirst = true;
    Map<String, String> partSpec = Warehouse.makeEscSpecFromName(partName);
    String convertedPartName = new String();

    for (Map.Entry<String, String> entry : partSpec.entrySet()) {
      String partColName = entry.getKey();
      String partColVal = entry.getValue();

      if (!isFirst) {
        convertedPartName += "/";
      } else {
        isFirst = false;
      }
      convertedPartName += partColName.toLowerCase() + "=" + partColVal;
    }
    return convertedPartName;
  }

  private List<String> getPartValsFromName(RawStore ms, String dbName, String tblName,
      String partName) throws MetaException, InvalidObjectException {
    // Unescape the partition name
    LinkedHashMap<String, String> hm = Warehouse.makeSpecFromName(partName);

    // getPartition expects partition values in a list. use info from the
    // table to put the partition column values in order
    Table t = ms.getTable(dbName, tblName);
    if (t == null) {
      throw new InvalidObjectException(dbName + "." + tblName
          + " table not found");
    }

    List<String> partVals = new ArrayList<String>();
    for (FieldSchema field : t.getPartitionKeys()) {
      String key = field.getName();
      String val = hm.get(key);
      if (val == null) {
        throw new InvalidObjectException("incomplete partition name - missing " + key);
      }
      partVals.add(val);
    }
    return partVals;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics colStats)
      throws NoSuchObjectException, InvalidObjectException,
      MetaException, InvalidInputException, TException {
    // return rs.updateTableColumnStatistics(colStats);
    return __cli.get().updateTableColumnStatistics(colStats);
  }

  @Override
  public boolean user_authority_check(User user, Table tbl,
      List<MSOperation> ops) throws MetaException, TException {
    return __cli.get().user_authority_check(user, tbl, ops);
  }

  @Override
  public boolean del_filelocation(SFileLocation sfl) throws MetaException,
      TException {
    boolean r = false;
    SFileLocation saved = rs.getSFileLocation(sfl.getDevid(), sfl.getLocation());

    if(saved != null){
      startFunction("del_filelocation", ": FID " + saved.getFid() + " dev " + saved.getDevid() + " loc " + saved.getLocation());
      r = rs.delSFileLocation(sfl.getDevid(), sfl.getLocation());
      if (r) {
        dm.asyncDelSFL(saved);
      }
      endFunction("del_filelocation", r, null);
    }
    return r;
  }

  @Override
  public List<SFile> get_files_by_ids(List<Long> fids)
      throws FileOperationException, MetaException, TException {
    List<SFile> fl = new ArrayList<SFile>();

    if (fids.size() > 0) {
      for (Long fid : fids) {
        try {
          SFile sf = get_file_by_id(fid);
          fl.add(sf);
        } catch (FileOperationException e) {
          // ignore
        }
      }
    }
    return fl;
  }

  @Override
  public boolean offlineDevicePhysically(String devid) throws MetaException, TException {
  	List<SFileLocation> sfls = rs.getSFileLocations(devid, System.currentTimeMillis(), 0);
    LOG.info("Offline Device " + devid + " physically, hit " + sfls.size() + " SFLs.");
    boolean isSharedDevice = dm.isSharedDevice(devid);

    for (SFileLocation f : sfls) {
      // NOTE: we have already delete the METADATA imediately, but we might leak on physical removals.
      boolean r = rs.delSFileLocation(devid, f.getLocation());
      if (r) {
        if (isSharedDevice) {
          f.setNode_name("");
        }
        dm.asyncDelSFL(f);
      }
    }
    return true;
  }

  @Override
  public boolean flSelectorWatch(String table, int op) throws MetaException, TException {
    // OP => |8bits|  8bits  |  8bits  | 8bits |
    //                          repnr     OP=3
    //                          policy    OP=0
    //         L?L? -> L?L?  ->  L?L?     OP=4
    //                 R4R3      R2R1     OP=5
    int true_op = op & 0xff;

  	switch (true_op) {
    case 0:
      return DiskManager.flselector.watched(table,
          HMSHandler.intToFLS_Policy((op & 0xff00) >>> 8));
    case 1:
      return DiskManager.flselector.unWatched(table);
    case 2:
      return DiskManager.flselector.flushWatched(table);
    case 3:
        return DiskManager.flselector.repnrWatched(table, (op & 0xff00) >>> 8);
    case 4:
      return DiskManager.flselector.orderWatched(table,
          HMSHandler.parseOrderList(op >>> 8));
    case 5:{
      List<Integer> rounds = new ArrayList<Integer>();
      for (int i = 0; i < 4; i++) {
        op >>>= 4;
        rounds.add(op & 0x0f);
      }
      return DiskManager.flselector.roundWatched(table, rounds);
    }
    default:
      return false;
    }
  }

	@Override
	public List<String> listDevsByNode(String nodeName) throws MetaException,
			TException {
		List<String> r = rs.listDevsByNode(nodeName);
		Set<String> s = new TreeSet<String>();
		s.addAll(r);
		r.clear();

		if (dm != null) {
		  List<DeviceInfo> dis;
		  try {
		    dis = dm.findDevices(nodeName);
		    if (dis != null) {
		      for (DeviceInfo di : dis) {
		        s.add(di.dev);
		      }
		    }
		  } catch (IOException e) {
		  }
		}
		r.addAll(s);

		return r;
	}

	@Override
	public List<Long> listFilesByDevs(List<String> devids) throws MetaException,
			TException {
		return rs.listFilesByDevs(devids);
	}

  @Override
  /*
   * dtype: L0-L4, and highest bit stands for INCRep
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#replicate(long, int)
   */
  public int replicate(long fid, int dtype) throws MetaException, FileOperationException,
      TException {
    DMProfile.replicate.incrementAndGet();
    startFunction("replicate ", "fid: " + fid + " to DEV " + DeviceInfo.getTypeStr(dtype));

    SFile saved = rs.getSFile(fid);

    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + fid, FOFailReason.NOTEXIST);
    }
    if (dm != null) {
      dm.submitReplicateRequest(new ReplicateRequest(fid, dtype));
    }
    return 0;
  }

  @Override
  public boolean update_ms_service(int status) throws MetaException, TException {
    if (dm != null) {
      switch (status) {
      default:
      case 0:
        LOG.warn("Change service role to SLAVE.");
        DiskManager.role = DiskManager.Role.SLAVE;
        break;
      case 1:
      {
        LOG.warn("Change service role to MASTER.");
        DiskManager.role = DiskManager.Role.MASTER;
        // reload the memory hash map
        if (dm.rs instanceof RawStoreImp) {
          CacheStore cs = ((RawStoreImp)dm.rs).getCs();
          cs.reloadHashMap();
        }
        break;
      }
      }
    }
    return __cli.get().update_ms_service(status);
  }

  @Override
  public String get_ms_uris() throws MetaException, TException {
    if (dm != null) {
      return dm.alternateURI;
    } else {
      return "";
    }
  }

  @Override
  public boolean update_sfile_nrs(long fid, long rec_nr, long all_rec_nr, long length) throws MetaException,
      FileOperationException, TException {
    SFile saved = rs.getSFile(fid);
    if (saved == null) {
      throw new FileOperationException("Can not find SFile by FID " + fid,
          FOFailReason.INVALID_FILE);
    }

    if (rec_nr >= 0) {
      saved.setRecord_nr(rec_nr);
    }
    if (all_rec_nr >= 0) {
      saved.setAll_record_nr(all_rec_nr);
    }
    if (length >= 0) {
      saved.setLength(length);
    }
    rs.updateSFile(saved);
    return true;
  }

  @Override
  public String getSysInfo() throws MetaException, TException {
    return DiskManager.sm.getSysInfo(dm);
  }

}
