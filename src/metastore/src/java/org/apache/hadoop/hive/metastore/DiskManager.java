package org.apache.hadoop.hive.metastore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.jdo.JDOObjectNotFoundException;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.DiskManager.DMThread.DMReport;
import org.apache.hadoop.hive.metastore.DiskManager.FLSelector.FLS_Policy;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Device;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.Subpartition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.model.MetaStoreConst;
import org.apache.hadoop.hive.metastore.newms.MsgServer;
import org.apache.hadoop.hive.metastore.newms.RawStoreImp;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

public class DiskManager {
    public static long startupTs = System.currentTimeMillis();
    public RawStore rs, rs_s1;
    private final RsStatus rst;
    public static enum RsStatus{
      NEWMS, OLDMS;
    }
    public static enum Role {
      MASTER, SLAVE,
    }
    public static long serverId;
    public static Role role;
    public String alternateURI;

    public Log LOG;
    private final HiveConf hiveConf;
    public final int bsize = 64 * 1024;
    public DatagramSocket server;
    private DMThread dmt;
    private DMCleanThread dmct;
    private DMRepThread dmrt;
    private DMCMDThread dmcmdt;
    public boolean safeMode = true;
    private final Timer timer = new Timer("checker");
    private final Timer bktimer = new Timer("backuper");
    private final DMTimerTask dmtt = new DMTimerTask();
    private final BackupTimerTask bktt = new BackupTimerTask();
    public final Queue<DMRequest> cleanQ = new ConcurrentLinkedQueue<DMRequest>();
    public final Queue<DMRequest> repQ = new ConcurrentLinkedQueue<DMRequest>();
    public final Queue<BackupEntry> backupQ = new ConcurrentLinkedQueue<BackupEntry>();
    public final ConcurrentLinkedQueue<DMReport> reportQueue = new ConcurrentLinkedQueue<DMReport>();
    public final ConcurrentHashMap<String, Long> toReRep = new ConcurrentHashMap<String, Long>();
    public final Map<String, Long> toUnspc = new ConcurrentHashMap<String, Long>();
    public final Map<String, MigrateEntry> rrmap = new ConcurrentHashMap<String, MigrateEntry>();
    public final TreeMap<Long, Long> incRepFiles = new TreeMap<Long, Long>();
    public static final ConcurrentHashMap<String, Long> errSFL = new ConcurrentHashMap<String, Long>();
    public Long incRepFid = -1L;

    public final ConcurrentLinkedQueue<ReplicateRequest> rrq = new ConcurrentLinkedQueue<ReplicateRequest>();

    private HashMap<String, Long> inactiveNodes = new HashMap<String, Long>();

    // TODO: fix me: change it to 30 min
    public long backupTimeout = 1 * 60 * 1000;
    public long backupQTimeout = 5 * 3600 * 1000; // 5 hour
    public long fileSizeThreshold = 64 * 1024 * 1024;
    public Set<String> disabledDevs = new TreeSet<String>();

    // TODO: REP limiting for low I/O bandwidth env
    public AtomicLong closeRepLimit = new AtomicLong(0L);
    public AtomicLong fixRepLimit = new AtomicLong(0L);

    public static long dmsnr = 0;
    public static FLSelector flselector = new FLSelector();
    public static List<Integer> default_orders = new ArrayList<Integer>();

    public static SysMonitor sm = new SysMonitor();

    // NOTE: if identify_shared_device is true, we return SFileLocation with
    // its node name to a accurate node list, otherwise we return SFileLocation
    // with "".
    public static boolean identify_shared_device;

    static {
      // Default do not replicate to L1 devices
      default_orders.add(MetaStoreConst.MDeviceProp.GENERAL);
      default_orders.add(MetaStoreConst.MDeviceProp.MASS);
      default_orders.add(MetaStoreConst.MDeviceProp.SHARED);
    }

    public static class ReplicateRequest {
      public long fid;
      public int dtype;

      public ReplicateRequest(long fid, int dtype) {
        this.fid = fid;
        this.dtype = dtype;
      }

      @Override
      public String toString() {
        return "fid " + fid + " TO " + DeviceInfo.getTypeStr(dtype) +
            (((dtype & 0x80000000) != 0) ? " INCREP" : " REP");
      }
    }

    public void submitReplicateRequest(ReplicateRequest rr) {
      rrq.add(rr);
    }

    public static class FLEntry {
      // single entry for one table
      public String table;
      public FLS_Policy policy = FLS_Policy.NONE;
      public int repnr = -1;
      public long l1Key;
      public long l2KeyMax;
      public TreeMap<Long, String> distribution;
      public TreeMap<String, Long> statis;
      public List<Integer> accept_types;
      // We define 4 rounds of dev location selections:
      // r1: first location (create new file)
      // r2: first replica location (do REP in repthread)
      // r3: other replicas (in do_replicate)
      // r4: increplicates
      // Note-XXX: r2 should be L1 when you want to put your replicas to FAST SSD devices
      public int r1, r2, r3, r4;

      public FLEntry(String table, long l1Key, long l2KeyMax) {
        this.table = table;
        this.l1Key = l1Key;
        this.l2KeyMax = l2KeyMax;
        distribution = new TreeMap<Long, String>();
        statis = new TreeMap<String, Long>();
        accept_types = new ArrayList<Integer>();
        r1 = MetaStoreConst.MDeviceProp.L2;
        r2 = MetaStoreConst.MDeviceProp.L2;
        r3 = MetaStoreConst.MDeviceProp.L2;
        r4 = MetaStoreConst.MDeviceProp.L2;
      }

      public void updateStatis(String node) {
        synchronized (statis) {
          if (statis.containsKey(node)) {
            statis.put(node, statis.get(node) + 1);
          } else {
            statis.put(node, 1L);
          }
        }
      }

      public Set<String> filterNodes(DiskManager dm, Set<String> in) {
        Set<String> r = new HashSet<String>();
        if (in != null) {
          r.addAll(in);
          r.removeAll(statis.keySet());
          r.removeAll(dm.inactiveNodes.keySet());
        }
        if (r.size() == 0) {
          // this means we have used all available nodes, then we use REF COUNT to calculate available nodes
          long level = Long.MAX_VALUE;
          for (Map.Entry<String, Long> e : statis.entrySet()) {
            if (e.getValue() < level) {
              r.clear();
              r.add(e.getKey());
              level = e.getValue();
            } else if (e.getValue() == level) {
              r.add(e.getKey());
            }
          }
        }
        return r;
      }
    }

    // TODO: up to 2014.11.30
    // SysMonitor watch and report system info
    //
    public static class SysMonitor {
      // 1. detect how many files are (truly) opened
      //
      // tracing current opened files, sort them, do table distribution analysis
      //

      // 2. hot node (device) analysis (1min, 5min, 15min)
      //
      // tracing node's read/write bandwidth on each device
      //
      public static HotDeviceTracing hdt = new HotDeviceTracing();

      public static class HotDeviceTracing {
        private final long EXP_1 = 1884;
        private final long EXP_5 = 2014;
        private final long EXP_15 = 2037;
        private final int FSHIFT = 11;
        private final long FIXED_1 = (1 << 11);

        private final ConcurrentHashMap<String, HDTrace> tmap = new ConcurrentHashMap<String, HDTrace>();

        private final Log LOG = LogFactory.getLog(HotDeviceTracing.class);

        private long calc_load(long load, long exp, long n) {
          load *= exp;
          load += n * (FIXED_1 - exp);
          load >>= FSHIFT;
          return load;
        }

        public class DTraceEntry {
          public long read_nr;  // sectors
          public long write_nr; // sectors
          public long iotime;   // Milliseconds
          public long ts = 0;

          @Override
          public String toString() {
            String r = "";
            r += "TS:" + ts + ", Read=" + read_nr + ", Write=" + write_nr + ", IOT=" + iotime;
            return r;
          }
        }

        public class HDTrace {
          public String dev;
          public String node;
          public int devProp;
          public DTraceEntry[] dte;
          public long rbw_1m, rbw_5m, rbw_15m;
          public long wbw_1m, wbw_5m, wbw_15m;
          public long pending_rank;

          public HDTrace() {
            dte = new DTraceEntry[2];
            for (int i = 0; i < 2; i++) {
              dte[i] = new DTraceEntry();
            }
            rbw_1m = rbw_5m = rbw_15m = 0;
            wbw_1m = wbw_5m = wbw_15m = 0;
            pending_rank = 0;
          }
        }

        public void updateDTrace(String node, DeviceInfo di) {
          HDTrace dt = tmap.get(di.dev);
          if (dt == null) {
            dt = new HDTrace();
            dt.node = node;
            dt.dev = di.dev;
            dt.devProp = di.prop;
            HDTrace ndt = tmap.putIfAbsent(di.dev, dt);
            if (ndt != null) {
              dt = ndt;
            }
          }
          synchronized (dt) {
            // copy DTE[0] to DTE[1]
            dt.dte[1].read_nr = dt.dte[0].read_nr;
            dt.dte[1].write_nr = dt.dte[0].write_nr;
            dt.dte[1].iotime = dt.dte[0].iotime;
            dt.dte[1].ts = dt.dte[0].ts;
            // update this entry to DTE[0]
            dt.dte[0].read_nr = di.read_nr;
            dt.dte[0].write_nr = di.write_nr;
            dt.dte[0].iotime = di.err_nr;
            dt.dte[0].ts = System.currentTimeMillis();
          }

          // try to regenerate ranks
          if (dt.dte[0].ts == 0 || dt.dte[1].ts == 0 || (dt.dte[0].ts - dt.dte[1].ts == 0)) {
            return;
          }
          if (dt.dte[0].ts - dt.dte[1].ts < 0) {
            // BUG-XXX: this means system reboot or device reconnect? clear saved loadavg
            dt.rbw_1m = dt.rbw_5m = dt.rbw_15m = 0;
            dt.wbw_1m = dt.wbw_5m = dt.wbw_15m = 0;
            dt.pending_rank = 0;
            LOG.info("Detect system reboot or device reconnect for " +
                dt.node + ":" + dt.dev + ", clear loadavg and rank.");
            return;
          }
          dt.rbw_1m = calc_load(dt.rbw_1m, EXP_1,
              (dt.dte[0].read_nr - dt.dte[1].read_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.rbw_5m = calc_load(dt.rbw_5m, EXP_5,
              (dt.dte[0].read_nr - dt.dte[1].read_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.rbw_15m = calc_load(dt.rbw_15m, EXP_15,
              (dt.dte[0].read_nr - dt.dte[1].read_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.wbw_1m = calc_load(dt.wbw_1m, EXP_1,
              (dt.dte[0].write_nr - dt.dte[1].write_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.wbw_5m = calc_load(dt.wbw_5m, EXP_5,
              (dt.dte[0].write_nr - dt.dte[1].write_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.wbw_15m = calc_load(dt.wbw_15m, EXP_15,
              (dt.dte[0].write_nr - dt.dte[1].write_nr) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
          dt.pending_rank = calc_load(dt.pending_rank, EXP_1,
              (dt.dte[0].iotime - dt.dte[1].iotime) * 512000 / (dt.dte[0].ts - dt.dte[1].ts));
        }

        public String getHotDeviceTracing(DiskManager dm) {
          String r = "#Device -> rbw_1m,rbw_5m,rbw_15m\twbw_1m,wbw_5m,wbw_15m\tpending_rank\n";
          TreeMap<Long, String> sortedHDTMap = new TreeMap<Long, String>();

          for (Map.Entry<String, HDTrace> entry : tmap.entrySet()) {
            DeviceInfo di = dm.admap.get(entry.getKey());
            String a = entry.getValue().node + "," +
                (di == null ?
                    DeviceInfo.getTypeStr(entry.getValue().devProp) :
                      di.getTypeStr()
                ) + "," +
                entry.getKey() + " -> " +
                entry.getValue().rbw_1m + "," +
                entry.getValue().rbw_5m + "," +
                entry.getValue().rbw_15m + "\t" +
                entry.getValue().wbw_1m + "," +
                entry.getValue().wbw_5m + "," +
                entry.getValue().wbw_15m + "\t" +
                entry.getValue().pending_rank + "\n";
            sortedHDTMap.put(entry.getValue().pending_rank, a);
          }
          for (Long k : sortedHDTMap.descendingKeySet()) {
            r += sortedHDTMap.get(k);
          }
          return r;
        }
      }

      // 3. load status bad analysis
      //
      // tracing load status bad calls, do table/node distribution analysis
      public static LSBA lsba = new LSBA();

      public static class LSBA {
        private final ConcurrentHashMap<Long, LoadStatusBad> fids = new ConcurrentHashMap<Long, LoadStatusBad>();
        private final ConcurrentLinkedQueue<LoadStatusBad> tsorted = new ConcurrentLinkedQueue<LoadStatusBad>();
        private final int tsRange = 600 * 1000;

        public class LoadStatusBad {
          public long fid;
          public String db;
          public String table;
          public String node;
          public String devid;
          public long ts;

          public LoadStatusBad(long fid, String db, String table, String node, String devid) {
            this.fid = fid;
            this.db = db;
            this.table = table;
            this.node = node;
            this.devid = devid;
            this.ts = System.currentTimeMillis();
          }
        }

        public void watchLSB(long fid, String db, String table, String node, String devid) {
          LoadStatusBad lsb = new LoadStatusBad(fid, db, table, node, devid);
          LoadStatusBad olsb = fids.put(fid, lsb);
          if (olsb != null) {
            tsorted.remove(olsb);
          }
          tsorted.add(lsb);
        }

        public void trimLSB() {
          while (true) {
            LoadStatusBad lsb = tsorted.peek();
            if (lsb != null) {
              if (lsb.ts + tsRange < System.currentTimeMillis()) {
                tsorted.remove(lsb);
              } else {
                break;
              }
            } else {
              break;
            }
          }
        }

        public String getTableDistribution() {
          HashMap<String, Long> td = new HashMap<String, Long>();
          String r = "";
          trimLSB();

          for (LoadStatusBad lsb : tsorted) {
            Long o = td.get(lsb.db + "." + lsb.table);
            if (o == null) {
              o = new Long(0);
            }
            o++;
            td.put(lsb.db + "." + lsb.table, o);
          }
          for (Map.Entry<String, Long> e : td.entrySet()) {
            r += e.getKey() + "\t" + e.getValue() + "\n";
          }

          return r;
        }

        public String getNodeDistribution() {
          HashMap<String, Long> nd = new HashMap<String, Long>();
          String r = "";
          trimLSB();

          for (LoadStatusBad lsb : tsorted) {
            Long o = nd.get(lsb.node + "." + lsb.devid);
            if (o == null) {
              o = new Long(0);
            }
            o++;
            nd.put(lsb.node + "." + lsb.devid, o);
          }
          for (Map.Entry<String, Long> e : nd.entrySet()) {
            r += e.getKey() + "\t" + e.getValue() + "\n";
          }

          return r;
        }
      }

      // 4. Analyze file read/write/err rate in data nodes
      //
      // node -> pending writes
      // node -> pending reads
      // node -> pending I/Os
      // node -> errs
      // table -> pending I/Os
      // table -> ACC read latency (latency/nr)
      // HOT node, device detection
      public static DSFSTAT dsfstat = new DSFSTAT();

      public static class DSFSTAT {
        public static enum VFSOperation {
          READ_BEGIN, READ_END, WRITE_BEGIN, WRITE_END, ERROR,
        };
        public class DMAudit {
          public String devid;
          public String location;
          public String tag;

          // all nodes should be ntp synced
          public long open;
          public VFSOperation op;
        }
        public class FSTAT {
          public String devid;
          public String location;
          public String tag;

          public long ts; // last update TS
          public long iolat;
          public int rdnr;
          public int wrnr;
          public int ernr;

          public FSTAT(String devid, String location, String tag) {
            this.devid = devid;
            this.location = location;
            this.tag = tag;
            this.ts = System.currentTimeMillis();
            this.rdnr = this.wrnr = this.ernr = 0;
            this.iolat = 0;
          }
        }

        private final ConcurrentHashMap<String, DMAudit> map = new ConcurrentHashMap<String, DMAudit>();
        private final ConcurrentHashMap<String, FSTAT> fstat = new ConcurrentHashMap<String, FSTAT>();
        private final int recycleLatency = 8 * 3600 * 1000;
        private final int fstatInvalidate = 12 * 3600 * 1000;
        private long drop_rde = 0;
        private long drop_wre = 0;
        private long total_err = 0;

        public void updateFStat(String devid, String location, String tag,
            int rdnr, int wrnr, int ernr, long iolat) {
          try {
            FSTAT fs = fstat.get(devid + ":" + location + ":" + tag);
            if (fs != null) {
              synchronized (fs) {
                if (rdnr > 0) {
                  fs.rdnr += rdnr;
                }
                if (wrnr > 0) {
                  fs.wrnr += wrnr;
                }
                if (ernr > 0) {
                  fs.ernr += ernr;
                }
                if (iolat > 0) {
                  fs.iolat += iolat;
                }
                fs.ts = System.currentTimeMillis();
              }
            } else {
              fs = new FSTAT(devid, location, tag);
              fs.ernr += ernr;
              fs.iolat += iolat;
              fstat.putIfAbsent(devid + ":" + location + ":" + tag, fs);
            }
          } catch (Exception e) {
          }
          if (ernr > 0) {
            // this update is an ERROR stat, keep it in errSFL map
            Long curnr = errSFL.putIfAbsent(devid + ":" + location, 1L);
            if (curnr != null) {
              errSFL.put(devid + ":" + location, curnr + 1);
            }
          }
        }

        public void updateDMAudit(List<DMReply> dmrs) {
          if (dmrs != null) {
            for (DMReply dmr : dmrs) {
              if (dmr.type == DMReply.DMReplyType.AUDIT) {
                if (dmr.args != null) {
                  String[] fs = dmr.args.split(",");
                  if (fs.length == 5) {
                    try {
                      // tag:devid:location
                      DMAudit dma = map.get(fs[0] + ":" + fs[2] + ":" + fs[3]);

                      if (dma == null) {
                        // new entry
                        if (fs[4].equals("RDB")) {
                          dma = new DMAudit();
                          dma.op = VFSOperation.READ_BEGIN;
                          try {
                            dma.open = Long.parseLong(fs[1]);
                          } catch (Exception e) {}
                          dma.devid = fs[2];
                          dma.location = fs[3];
                          dma.tag = fs[0];
                          map.putIfAbsent(fs[0] + ":" + fs[2] + ":" + fs[3], dma);
                        } else if (fs[4].equals("WRB")) {
                          dma = new DMAudit();
                          dma.op = VFSOperation.WRITE_BEGIN;
                          try {
                            dma.open = Long.parseLong(fs[1]);
                          } catch (Exception e) {}
                          dma.devid = fs[2];
                          dma.location = fs[3];
                          dma.tag = fs[0];
                          map.putIfAbsent(fs[0] + ":" + fs[2] + ":" + fs[3], dma);
                        } else if (fs[4].equals("RDE")) {
                          drop_rde++;
                        } else if (fs[4].equals("WRE")) {
                          drop_wre++;
                        } else if (fs[4].equals("ERR")) {
                          // ok, try to update FSTAT
                          updateFStat(fs[2], fs[3], fs[0], 0, 0, 1, 0);
                          total_err++;
                        }
                      } else {
                        switch (dma.op) {
                        case READ_BEGIN:{
                          if (fs[4].equals("RDE")) {
                            // ok, try to update FSTAT
                            long cur = dma.open;
                            try {cur = Long.parseLong(fs[1]);} catch (Exception x) {}
                            updateFStat(fs[2], fs[3], fs[0], 1, 0, 0, cur - dma.open);
                            map.remove(dma);
                          } else if (fs[4].equals("ERR")) {
                            // ok, try to update FSTAT
                            long cur = dma.open;
                            try {cur = Long.parseLong(fs[1]);} catch (Exception x) {}
                            updateFStat(fs[2], fs[3], fs[0], 0, 0, 1, cur - dma.open);
                            map.remove(dma);
                            total_err++;
                          }
                          break;
                        }
                        case WRITE_BEGIN:{
                          if (fs[4].equals("WRE")) {
                            // ok, try to update FSTAT
                            long cur = dma.open;
                            try {cur = Long.parseLong(fs[1]);} catch (Exception x) {}
                            updateFStat(fs[2], fs[3], fs[0], 0, 1, 0, cur - dma.open);
                            map.remove(dma);
                          } else if (fs[4].equals("ERR")) {
                            // ok, try to update FSTAT
                            long cur = dma.open;
                            try {cur = Long.parseLong(fs[1]);} catch (Exception x) {}
                            updateFStat(fs[2], fs[3], fs[0], 0, 0, 1, cur - dma.open);
                            map.remove(dma);
                            total_err++;
                          }
                          break;
                        }
                        default:
                        }
                      }
                    } catch (Exception e) {
                    }
                  }
                }
              }
            }
          }
        }

        public void cleanDSFStat(long timeout) {
          try {
            // clean DMAudit
            List<String> toDel = new ArrayList<String>();

            for (Map.Entry<String, DMAudit> e : map.entrySet()) {
              if (e.getValue().open + timeout < System.currentTimeMillis()) {
                toDel.add(e.getKey());
              }
            }
            for (String td : toDel) {
              map.remove(td);
            }
            toDel.clear();
            // clean DSFStat
            for (Map.Entry<String, FSTAT> e : fstat.entrySet()) {
              if (e.getValue().ts + timeout < System.currentTimeMillis()) {
                toDel.add(e.getKey());
              }
            }
            for (String td : toDel) {
              fstat.remove(td);
            }
          } catch (Exception e) {
          }
        }

        private final Comparator<Entry<String, HotResult>> compByNr = new Comparator<Entry<String, HotResult>>() {
          @Override
          public int compare(Entry<String, HotResult> o1, Entry<String, HotResult> o2) {
            return (o2.getValue().nr - o1.getValue().nr) > 0 ? 1 : -1;
          }
        };

        private final Comparator<Entry<String, HotResult>> compByLatency = new Comparator<Entry<String, HotResult>>() {
          @Override
          public int compare(Entry<String, HotResult> o1, Entry<String, HotResult> o2) {
            if (o1.getValue().nr == 0) {
              return 1;
            } else if (o2.getValue().nr == 0) {
              return -1;
            }
            return (o2.getValue().iolat / o2.getValue().nr -
                o1.getValue().iolat / o1.getValue().nr) > 0 ? 1 : -1;
          }
        };

        public class HotResult {
          public long nr;
          public long iolat;

          public HotResult(long nr, long iolat) {
            this.nr = nr;
            this.iolat = iolat;
          }
        }

        public String getHistHots(DiskManager dm) {
          String r = "";
          Map<String, HotResult> hist_hot_node = new HashMap<String, HotResult>();
          Map<String, HotResult> hist_hot_dev = new HashMap<String, HotResult>();
          Map<String, HotResult> hist_hot_table = new HashMap<String, HotResult>();

          for (Map.Entry<String, FSTAT> e : fstat.entrySet()) {
            FSTAT fs = e.getValue();

            HotResult o = hist_hot_dev.get(fs.devid);
            if (o == null) {
              o = new HotResult(0, 0);
              hist_hot_dev.put(fs.devid, o);
            }
            o.nr++;
            o.iolat += fs.iolat;

            Device d = null;
            synchronized (dm.rs) {
              try {
                d = dm.rs.getDevice(fs.devid);
              } catch (Exception e1) {
                dm.LOG.error(e1, e1);
              }
            }
            if (d != null) {
              String node_name = d.getNode_name();

              try {
                if (dm.isSharedDevice(d.getDevid())) {
                  node_name = "SHARED_NODES_OF(" + d.getDevid() + ")";
                }
              } catch (Exception ex) {
                dm.LOG.error(ex, ex);
              }
              HotResult o2 = hist_hot_node.get(node_name);
              if (o2 == null) {
                o2 = new HotResult(0, 0);
                hist_hot_node.put(d.getNode_name(), o2);
              }
              o2.nr++;
              o2.iolat += fs.iolat;
            }

            SFile f = null;
            synchronized (dm.rs) {
              try {
                f = dm.rs.getSFile(fs.devid, fs.location);
              } catch (Exception e1) {
                dm.LOG.error(e1, e1);
              }
            }
            if (f != null) {
              HotResult o3 = hist_hot_table.get(f.getDbName() + "." + f.getTableName());
              if (o3 == null) {
                o3 = new HotResult(0, 0);
                hist_hot_table.put(f.getDbName() + "." + f.getTableName(), o3);
              }
              o3.nr++;
              o3.iolat += fs.iolat;
            }
          }

          r += "Drop RDE " + drop_rde + ", WRE " + drop_wre + ", total ERR " + total_err + "\n";
          List<Entry<String, HotResult>> entries = Lists.newArrayList(hist_hot_dev.entrySet());
          Collections.sort(entries, compByNr);

          r += "Top 10 hot devices in history (by open-close pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " + entries.get(i).getValue().nr + "\n";
          }

          Collections.sort(entries, compByLatency);

          r += "Top 10 hot devices in history (by latency pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " +
                entries.get(i).getValue().iolat / entries.get(i).getValue().nr + "\n";
          }

          entries = Lists.newArrayList(hist_hot_node.entrySet());
          Collections.sort(entries, compByNr);

          r += "Top 10 hot nodes   in history (by open-close pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " + entries.get(i).getValue().nr + "\n";
          }

          Collections.sort(entries, compByLatency);

          r += "Top 10 hot nodes   in history (by latency pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " +
                entries.get(i).getValue().iolat / entries.get(i).getValue().nr + "\n";
          }

          entries = Lists.newArrayList(hist_hot_table.entrySet());
          Collections.sort(entries, compByNr);

          r += "Top 10 hot tables  in history (by open-close pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " + entries.get(i).getValue().nr + "\n";
          }

          Collections.sort(entries, compByLatency);

          r += "Top 10 hot tables  in history (by latency pairs) is:\n";
          for (int i = 0; i < Math.min(10, entries.size()); i++) {
            r += "\t" + entries.get(i).getKey() + " -> " +
                entries.get(i).getValue().iolat / entries.get(i).getValue().nr + "\n";
          }

          return r;
        }
      }

      public String getSysInfo(DiskManager dm) {
        String r = "";

        r += "In Last " + lsba.tsRange / 1000 + " seconds, LSBA distribution is:\n";
        r += lsba.getTableDistribution();
        r += lsba.getNodeDistribution();
        r += "\n";

        if (dm != null) {
          r += "Audit Log Analyse Report:\n";
          r += dsfstat.getHistHots(dm);
          r += "\n";
        }

        if (dm != null) {
          r += "Hot Device Tracking:\n";
          r += hdt.getHotDeviceTracing(dm);
          r += "\n";
        }

        if (dm != null && dm.deviceTracked.size() > 0) {
          r += "Tracked Device:\n";
          r += dm.getTrackedDevice();
          r += "\n";
        }

        return r;
      }
    }

    // FLSelector stands before any file location allocations
    //
    // --> FLSelector --> make FLP -> find_best_node -> find_best_device ...
    //
    public static class FLSelector {
      // considering node load and tables' first file locations
      // Note that the following table should be REGULAR table name as DB.TABLE
      public final Set<String> tableWatched = Collections.synchronizedSet(new HashSet<String>());
      public final Map<String, FLEntry> context = new ConcurrentHashMap<String, FLEntry>();

      public enum FLS_Policy {
        NONE, FAIR_NODES, ORDERED_ALLOC_DEVS, LOONG_STORE,
      }

      public void initWatchedTables(RawStore rs, String[] tables, FLS_Policy policy) throws MetaException {
        for (String table : tables) {
          watched(table, policy);
        }
      }

      public boolean watched(String table, FLS_Policy policy) {
        boolean r = tableWatched.add(table);
        FLEntry e = new FLEntry(table, -1, -1);
        e.policy = policy;
        synchronized (context) {
          if (!context.containsKey(table)) {
            context.put(table, e);
          } else {
            context.get(table).policy = policy;
          }
        }
        return r;
      }

      public boolean unWatched(String table) {
        boolean r = tableWatched.remove(table);
        context.remove(table);
        return r;
      }

      public boolean flushWatched(String table) {
        FLEntry fle = context.get(table);
        if (fle != null) {
          synchronized (fle) {
            fle.distribution.clear();
            fle.statis.clear();
          }
          return true;
        }
        return false;
      }

      public boolean repnrWatched(String table, int repnr) {
        if (repnr <= 0 || repnr > 5) {
          return false;
        }
        FLEntry fle = context.get(table);
        if (fle != null) {
          synchronized (fle) {
            fle.repnr = repnr;
          }
          return true;
        }
        return false;
      }

      public boolean orderWatched(String table, List<Integer> accept_types) {
        FLEntry fle = context.get(table);
        if (fle != null) {
          synchronized (fle) {
            fle.accept_types.clear();
            fle.accept_types.addAll(accept_types);
          }
          return true;
        }
        return false;
      }

      public boolean roundWatched(String table, List<Integer> rounds) {
        if (rounds.size() != 4) {
          return false;
        }
        FLEntry fle = context.get(table);
        if (fle != null) {
          synchronized (fle) {
            fle.r1 = rounds.get(0);
            fle.r2 = rounds.get(1);
            fle.r3 = rounds.get(2);
            fle.r4 = rounds.get(3);
          }
          return true;
        }
        return false;
      }

      public String printWatched() {
        String r = "";

        r += "Table FLSelector Watched: " + tableWatched.size() + " {";
        for (String tbl : tableWatched) {
          r += tbl + ",";
        }
        r += "}\n";

        synchronized (context) {
          for (Map.Entry<String, FLEntry> e : context.entrySet()) {
            r += "Table '" + e.getKey() + "' -> {\n";
            r += "\trepnr=" + e.getValue().repnr + ", ";
            r += "policy=" + e.getValue().policy + ", ";
            r += "order='" + e.getValue().accept_types + "', ";
            r += "R1=" + DeviceInfo.getTypeStr(e.getValue().r1) + ", ";
            r += "R2=" + DeviceInfo.getTypeStr(e.getValue().r2) + ", ";
            r += "R3=" + DeviceInfo.getTypeStr(e.getValue().r3) + ", ";
            r += "R4=" + DeviceInfo.getTypeStr(e.getValue().r4) + ", ";
            r += "l1Key=" + e.getValue().l1Key + ", l2KeyMax?=" + e.getValue().l2KeyMax + "\n";
            synchronized (e.getValue()) {
              if (e.getValue().distribution.size() > 0) {
                r += "\t" + e.getValue().distribution + "\n";
              }
              if (e.getValue().statis.size() > 0) {
                r += "\t" + e.getValue().statis + "\n";
              }
            }
            r += "}\n";
          }
        }
        return r;
      }

      // PLEASE USE REGULAR TABLE NAME HERE
      public int updateRepnr(String table, int repnr) {
        if (!tableWatched.contains(table)) {
          return repnr;
        }
        FLEntry fle = context.get(table);
        if (fle != null && fle.repnr > 0) {
          return fle.repnr;
        } else {
          return repnr;
        }
      }

      // PLEASE USE REGULAR TABLE NAME HERE
      // filter the accept_types by cur_level, all types that BEFORE cur_level are filtered
      public List<Integer> getDevTypeListAfterIncludeHint(String table, int cur_level) {
        List<Integer> r = new ArrayList<Integer>();
        r.addAll(default_orders);

        if (!tableWatched.contains(table)) {
          return r;
        }
        FLEntry fle = context.get(table);
        if (fle != null && fle.accept_types != null) {
          r.clear();
          switch (cur_level) {
          case MetaStoreConst.MDeviceProp.__AUTOSELECT_R1__:
            cur_level = fle.r1;
            break;
          case MetaStoreConst.MDeviceProp.__AUTOSELECT_R2__:
            cur_level = fle.r2;
            break;
          case MetaStoreConst.MDeviceProp.__AUTOSELECT_R3__:
            cur_level = fle.r3;
            break;
          case MetaStoreConst.MDeviceProp.__AUTOSELECT_R4__:
            cur_level = fle.r4;
            break;
          }
          // BUG-XXX: if FLE order size is ZERO, use default orders!
          List<Integer> orders = new ArrayList<Integer>(fle.accept_types);
          if (orders.size() == 0) {
            orders.addAll(default_orders);
          }
          for (Integer lvl : orders) {
            if (MetaStoreConst.checkDeviceOrder(cur_level, lvl)) {
              r.add(lvl);
            }
          }
          if (r.size() == 0) {
            r.addAll(default_orders);
          }
        }

        return r;
      }

      // PLEASE USE REGULAR TABLE NAME HERE
      public boolean isTableOrdered(String table) {
        if (!tableWatched.contains(table)) {
          return false;
        }
        FLEntry fle = context.get(table);
        if (fle != null && fle.accept_types != null && fle.accept_types.size() > 0) {
          return true;
        } else {
          return false;
        }
      }

      // PLEASE USE REGULAR TABLE NAME HERE
      public List<Integer> getDevTypeList(String table) {
        List<Integer> r = new ArrayList<Integer>();
        r.addAll(default_orders);

        if (!tableWatched.contains(table)) {
          return r;
        }
        FLEntry fle = context.get(table);
        if (fle != null && fle.accept_types != null && fle.accept_types.size() > 0) {
          return fle.accept_types;
        } else {
          return r;
        }
      }

      // User should switch on return value: it is policy!
      public FLS_Policy FLSelector_switch(String table) {
        FLEntry e = context.get(table);
        if (e == null) {
          return FLS_Policy.NONE;
        } else {
          return e.policy;
        }
      }

      // This is used by FLSelector framework
      //
      // findBestNode use FLS_Policy: FAIR_NODES
      //
      // PLEASE USE REGULAR TABLE NAME HERE
      public String findBestNode(DiskManager dm, FileLocatingPolicy flp, String table,
          long l1Key, long l2Key) throws IOException {
        String targetNode = null;

        if (!tableWatched.contains(table)) {
          return dm.findBestNode(flp);
        }

        FLEntry dist = context.get(table);
        if (dist == null) {
          targetNode = dm.findBestNode(flp);
          if (targetNode != null) {
            dist = new FLEntry(table, l1Key, l2Key);
            dist.distribution.put(l2Key, targetNode);
            dist.updateStatis(targetNode);
            synchronized (context) {
              if (!context.containsKey(table)) {
                context.put(table, dist);
              } else {
                dist = context.get(table);
                if (!dist.distribution.containsKey(l2Key)) {
                  dist.distribution.put(l2Key, targetNode);
                }
                dist.updateStatis(targetNode);
              }
            }
          }
        } else {
          synchronized (dist) {
            boolean doUpdate = true;

            if (l1Key > dist.l1Key) {
              // drop all old infos
              dist.l1Key = l1Key;
              dist.distribution.clear();
              dist.statis.clear();
            } else if (l1Key == dist.l1Key) {
              // ok, do filter
              flp.nodes = dist.filterNodes(dm, flp.nodes);
            } else {
              // ignore this key
              doUpdate = false;
            }
            // do find now
            targetNode = dm.findBestNode(flp);
            if (targetNode != null && doUpdate) {
              if (!dist.distribution.containsKey(l2Key)) {
                dist.distribution.put(l2Key, targetNode);
              }
              dist.updateStatis(targetNode);
            }
          }
        }
        return targetNode;
      }
    }

    public static class DMProfile {
      public static AtomicLong fcreate1R = new AtomicLong(0);
      public static AtomicLong fcreate1SuccR = new AtomicLong(0);
      public static AtomicLong fcreate2R = new AtomicLong(0);
      public static AtomicLong fcreate2SuccR = new AtomicLong(0);
      public static AtomicLong freopenR = new AtomicLong(0);
      public static AtomicLong fgetR = new AtomicLong(0);
      public static AtomicLong fcloseR = new AtomicLong(0);
      public static AtomicLong fcloseSuccRS = new AtomicLong(0);
      public static AtomicLong freplicateR = new AtomicLong(0);
      public static AtomicLong frmlR = new AtomicLong(0);
      public static AtomicLong frmpR = new AtomicLong(0);
      public static AtomicLong frestoreR = new AtomicLong(0);
      public static AtomicLong fdelR = new AtomicLong(0);
      public static AtomicLong sflcreateR = new AtomicLong(0);
      public static AtomicLong sflonlineR = new AtomicLong(0);
      public static AtomicLong sflofflineR = new AtomicLong(0);
      public static AtomicLong sflsuspectR = new AtomicLong(0);
      public static AtomicLong sfldelR = new AtomicLong(0);
      public static AtomicLong newConn = new AtomicLong(0);
      public static AtomicLong delConn = new AtomicLong(0);
      public static AtomicLong query = new AtomicLong(0);
      public static AtomicLong replicate = new AtomicLong(0);
      public static AtomicLong loadStatusBad = new AtomicLong(0);
      public static AtomicLong sflincrepR = new AtomicLong(0);
    }

    public static class SFLTriple implements Comparable<SFLTriple> {
      public String node;
      public String devid;
      public String location;

      public SFLTriple(String node, String devid, String location) {
        this.node = node;
        this.devid = devid;
        this.location = location;
      }

      @Override
      public int compareTo(SFLTriple b) {
        return node.compareTo(b.node) & devid.compareTo(b.devid) & location.compareTo(b.location);
      }

      @Override
      public String toString() {
        return "N:" + node + ",D:" + devid + ",L:" + location;
      }
    }

    public static class MigrateEntry {
      boolean is_part;
      String to_dc;
      public Partition part;
      public Subpartition subpart;
      public List<Long> files;
      public Map<String, Long> timap;

      public MigrateEntry(String to_dc, Partition part, List<Long> files, Map<String, Long> timap) {
        this.is_part = true;
        this.to_dc = to_dc;
        this.part = part;
        this.files = files;
        this.timap = timap;
      }

      public MigrateEntry(String to_dc, Subpartition subpart, List<Long> files, Map<String, Long> timap) {
        this.is_part = false;
        this.to_dc = to_dc;
        this.subpart = subpart;
        this.files = files;
        this.timap = timap;
      }

      @Override
      public String toString() {
        String r;
        if (is_part) {
          r = "Part    :" + part.getPartitionName() + ",files:" + files.toString();
        } else {
          r = "Subpart :" + subpart.getPartitionName() + ",files:" + files.toString();
        }
        return r;
      }
    }
    public static class BackupEntry {
      public enum FOP {
        ADD_PART, DROP_PART, ADD_SUBPART, DROP_SUBPART,
      }
      public Partition part;
      public Subpartition subpart;
      public List<SFile> files;
      public FOP op;
      public long ttl;

      public BackupEntry(Partition part, List<SFile> files, FOP op) {
        this.part = part;
        this.files = files;
        this.op = op;
        this.ttl = System.currentTimeMillis();
      }
      public BackupEntry(Subpartition subpart, List<SFile> files, FOP op) {
        this.subpart = subpart;
        this.files = files;
        this.op = op;
        this.ttl = System.currentTimeMillis();
      }
      @Override
      public String toString() {
        String r;

        switch (op) {
        case ADD_PART:
          r = "ADD  PART: " + part.getPartitionName() + ",files:" + files.toString();
          break;
        case DROP_PART:
          r = "DROP PART: " + part.getPartitionName() + ",files:" + files.toString();
          break;
        case ADD_SUBPART:
          r = "ADD  SUBPART: " + subpart.getPartitionName() + ",files:" + files.toString();
          break;
        case DROP_SUBPART:
          r = "DROP SUBPART: " + subpart.getPartitionName() + ",files:" + files.toString();
          break;
        default:
          r = "BackupEntry: INVALID OP!";
        }
        return r;
      }
    }

    public static class FileToPart {
      public boolean isPart;
      public SFile file;
      public Partition part;
      public Subpartition subpart;

      public FileToPart(SFile file, Partition part) {
        this.isPart = true;
        this.file = file;
        this.part = part;
      }
      public FileToPart(SFile file, Subpartition subpart) {
        this.isPart = false;
        this.file = file;
        this.subpart = subpart;
      }
    }

    public static class DMReply {
      public enum DMReplyType {
        DELETED, REPLICATED, FAILED_REP, FAILED_DEL, VERIFY, INFO, SLAVE, AUDIT,
        TRACK_DEV,
      }
      DMReplyType type;
      String args;

      @Override
      public String toString() {
        String r = "";
        switch (type) {
        case DELETED:
          r += "DELETED";
          break;
        case REPLICATED:
          r += "REPLICATED";
          break;
        case FAILED_REP:
          r += "FAILED_REP";
          break;
        case FAILED_DEL:
          r += "FAILED_DEL";
          break;
        case VERIFY:
          r += "VERIFY";
          break;
        case INFO:
          r += "INFO";
          break;
        case AUDIT:
          r += "AUDIT";
          break;
        case TRACK_DEV:
          r += "TRACK_DEV";
          break;
        default:
          r += "UNKNOWN";
          break;
        }
        r += ": {" + args + "}";
        return r;
      }
    }

    public static class DMRequest {
      public enum DMROperation {
        REPLICATE, RM_PHYSICAL, MIGRATE,
      }
      SFile file;
      SFile tfile; // target file, only valid when op is MIGRATE
      Map<String, String> devmap;
      DMROperation op;
      String to_dc;
      int begin_idx;
      int failnr = 0;

      public DMRequest(SFile f, DMROperation o, int idx) {
        file = f;
        op = o;
        begin_idx = idx;
      }

      public DMRequest(SFile source, SFile target, Map<String, String> devmap, String to_dc) {
        this.file = source;
        this.tfile = target;
        this.devmap = devmap;
        this.to_dc = to_dc;
        op = DMROperation.MIGRATE;
      }

      @Override
      public String toString() {
        String r;
        switch (op) {
        case REPLICATE:
          r = "REPLICATE: file fid " + file.getFid() + " from idx " + begin_idx + ", repnr " + file.getRep_nr();
          break;
        case RM_PHYSICAL:
          r = "DELETE   : file fid " + file.getFid();
          break;
        case MIGRATE:
          r = "MIGRATE  : file fid " + file.getFid() + " to DC " + to_dc + "fid " + tfile.getFid();
          break;
        default:
          r = "DMRequest: Invalid OP!";
        }
        return r;
      }
    }

    public static class DeviceTrack {
      public String dev;
      long rep_nr;
      long rep_err;
      long rep_lat;
      long del_nr;
      long del_err;
      long del_lat;

      public DeviceTrack(String dev) {
        this.dev = dev;
      }

      @Override
      public String toString() {
        String r = "";
        r += dev + " -> " + rep_nr + "," + rep_err + "," + ((double)rep_lat / rep_nr) + "," +
            del_nr + "," + del_err + "," + ((double)del_lat / del_nr);
        return r;
      }
    }

    public static class DeviceInfo implements Comparable<DeviceInfo> {
      public String dev; // dev name
      public String mp = null; // mount point
      public int prop = -1;
      public int status = MetaStoreConst.MDeviceStatus.ONLINE;
      public boolean isOffline = false;
      public long read_nr;
      public long write_nr;
      public long err_nr;
      public long used;
      public long free;

      public DeviceInfo() {
        mp = null;
        prop = -1;
        isOffline = false;
      }

      public int getType() {
        return prop & MetaStoreConst.MDeviceProp.__TYPE_MASK__;
      }

      public String getTypeStr() {
        switch (prop & MetaStoreConst.MDeviceProp.__TYPE_MASK__) {
        case MetaStoreConst.MDeviceProp.RAM:
          return "L0";
        case MetaStoreConst.MDeviceProp.CACHE:
          return "L1";
        case MetaStoreConst.MDeviceProp.GENERAL:
          return "L2";
        case MetaStoreConst.MDeviceProp.MASS:
          return "L3";
        case MetaStoreConst.MDeviceProp.SHARED:
          return "L4";
        default:
          return "X" + getType();
        }
      }

      public static String getTypeStr(int prop) {
        switch (prop & MetaStoreConst.MDeviceProp.__TYPE_MASK__) {
        case MetaStoreConst.MDeviceProp.RAM:
          return "L0";
        case MetaStoreConst.MDeviceProp.CACHE:
          return "L1";
        case MetaStoreConst.MDeviceProp.GENERAL:
          return "L2";
        case MetaStoreConst.MDeviceProp.MASS:
          return "L3";
        case MetaStoreConst.MDeviceProp.SHARED:
          return "L4";
        default:
          return "X" + (prop & MetaStoreConst.MDeviceProp.__TYPE_MASK__);
        }
      }

      public static int getType(int prop) {
        return prop & MetaStoreConst.MDeviceProp.__TYPE_MASK__;
      }

      // Note that, the quota we save is -quota
      public int getQuota() {
        int keep = (prop & MetaStoreConst.MDeviceProp.__QUOTA_MASK__) >>>
          MetaStoreConst.MDeviceProp.__QUOTA_SHIFT__;
        return keep > 100 ? 0 : (100 - keep);
      }

      public static int getQuota(int prop) {
        int keep = (prop & MetaStoreConst.MDeviceProp.__QUOTA_MASK__) >>>
          MetaStoreConst.MDeviceProp.__QUOTA_SHIFT__;
        return keep > 100 ? 0 : (100 - keep);
      }

      public static int getTags(int prop) {
        return prop & MetaStoreConst.MDeviceProp.__TAG_MASK__;
      }

      public String getUsage() {
        return String.format("%02d",
            (used + free > 0 ? (int)(used * 100 / (used + free)) : -1));
      }

      public DeviceInfo(DeviceInfo old) {
        dev = old.dev;
        mp = old.mp;
        prop = old.prop;
        read_nr = old.read_nr;
        write_nr = old.write_nr;
        err_nr = old.err_nr;
        used = old.used;
        free = old.free;
      }

      @Override
      public int compareTo(DeviceInfo o) {
        return this.dev.compareTo(o.dev);
      }
    }

    public class NodeInfo {
      public long lastRptTs;
      public List<DeviceInfo> dis;
      // to do SFL delete on dservice
      public Set<SFileLocation> toDelete;
      // to do SFL replicate on dservice
      public Set<JSONObject> toRep;
      // to reply verify request, dservice should delete the verified SFL
      public Set<String> toVerify;
      // to do SFL verify on dservice
      public Set<SFileLocation> toCheck;
      public String lastReportStr;
      public long totalReportNr = 0;
      public long totalFileRep = 0;
      public long totalFileDel = 0;
      public long totalFailDel = 0;
      public long totalFailRep = 0;
      public long totalVerify = 0;
      public long totalVYR = 0;
      public long totalCheck = 0;
      public InetAddress address = null;
      public int port = 0;

      public long qrep = 0;
      public long hrep = 0;
      public long drep = 0;
      public long qdel = 0;
      public long hdel = 0;
      public long ddel = 0;
      public long tver = 0;
      public long tvyr = 0;
      public long uptime = 0;
      public double load1 = 0.0;
      public long recvLatency = 0;

      public NodeInfo(List<DeviceInfo> dis) {
        this.lastRptTs = System.currentTimeMillis();
        this.dis = dis;
        this.toDelete = Collections.synchronizedSet(new TreeSet<SFileLocation>());
        this.toRep = Collections.synchronizedSet(new TreeSet<JSONObject>());
        this.toVerify = Collections.synchronizedSet(new TreeSet<String>());
        this.toCheck = Collections.synchronizedSet(new TreeSet<SFileLocation>());
      }

      public String getMP(String devid) {
        synchronized (this) {
          if (dis == null) {
            return null;
          }
          for (DeviceInfo di : dis) {
            if (di.dev.equals(devid)) {
              return di.mp;
            }
          }
          return null;
        }
      }
    }

    // Node -> Device Map
    private final ConcurrentHashMap<String, NodeInfo> ndmap;
    // Active Device Map
    private final ConcurrentHashMap<String, DeviceInfo> admap;
    // Device -> Node Map
    private final ConcurrentHashMap<String, Set<String>> dnmap;
    // Blacklisted Devices (no replicate or delete requests should send to them)
    private final ConcurrentHashMap<String, DeviceInfo> blacklisted;
    // Track Devices' rep/del info
    private final ConcurrentHashMap<String, DeviceTrack> deviceTracked;

    public class BackupTimerTask extends TimerTask {
      private long last_backupTs = System.currentTimeMillis();

      public boolean generateSyncFiles(Set<Partition> parts, Set<Subpartition> subparts, Set<FileToPart> toAdd, Set<FileToPart> toDrop) {
        Date d = new Date(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        String str = new HiveConf().getVar(HiveConf.ConfVars.DM_REPORT_DIR);
        if (str == null) {
          str = System.getProperty("user.dir") + "/backup/sync-" + sdf.format(d);
        } else {
          str = str + "/backup/sync-" + sdf.format(d);
        }
        File dir = new File(str);
        if (!dir.mkdirs()) {
          LOG.error("Make directory " + dir.getPath() + " failed, can't write sync meta files.");
          return false;
        }
        // generate tableName.desc files
        Set<Table> tables = new TreeSet<Table>();
        Map<String, Table> partToTbl = new HashMap<String, Table>();
        for (Partition p : parts) {
          synchronized (rs) {
            Table t;
            try {
              t = rs.getTable(p.getDbName(), p.getTableName());
              tables.add(t);
              partToTbl.put(p.getPartitionName(), t);
            } catch (MetaException e) {
              LOG.error(e, e);
              return false;
            }
          }
        }
        for (Subpartition p : subparts) {
          synchronized (rs) {
            Table t;
            try {
              t = rs.getTable(p.getDbName(), p.getTableName());
              tables.add(t);
              partToTbl.put(p.getPartitionName(), t);
            } catch (MetaException e) {
              LOG.error(e, e);
              return false;
            }
          }
        }
        for (Table t : tables) {
          File f = new File(dir, t.getDbName() + ":" + t.getTableName() + ".desc");
          try {
            if (!f.exists()) {
              f.createNewFile();
            }
            String content = "[fieldInfo]\n";
            for (FieldSchema fs : t.getSd().getCols()) {
              content += fs.getName() + "\t" + fs.getType() + "\n";
            }
            content += "[partitionInfo]\n";
            List<PartitionInfo> pis = PartitionInfo.getPartitionInfo(t.getPartitionKeys());
            for (PartitionInfo pi : pis) {
              content += pi.getP_col() + "\t" + pi.getP_type().getName() + "\t" + pi.getArgs().toString() + "\n";
            }
            FileWriter fw = new FileWriter(f.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
          } catch (IOException e) {
            LOG.error(e, e);
            return false;
          }
        }
        File f = new File(dir, "manifest.desc");
        if (!f.exists()) {
          try {
            f.createNewFile();
          } catch (IOException e) {
            LOG.error(e, e);
            return false;
          }
        }
        String content = "";
        for (FileToPart ftp : toAdd) {
          for (SFileLocation sfl : ftp.file.getLocations()) {
            Device device = null;
            synchronized (rs) {
              try {
                device = rs.getDevice(sfl.getDevid());
              } catch (MetaException e) {
                LOG.error(e, e);
              } catch (NoSuchObjectException e) {
                LOG.error(e, e);
              }
            }
            if (device != null && (DeviceInfo.getType(device.getProp()) == MetaStoreConst.MDeviceProp.BACKUP)) {
              content += sfl.getLocation().substring(sfl.getLocation().lastIndexOf('/') + 1);
              if (ftp.isPart) {
                content += "\tADD\t" + ftp.part.getDbName() + "\t" + ftp.part.getTableName() + "\t";
                for (int i = 0; i < ftp.part.getValuesSize(); i++) {
                  Table t = partToTbl.get(ftp.part.getPartitionName());
                  List<PartitionInfo> pis = PartitionInfo.getPartitionInfo(t.getPartitionKeys());
                  for (PartitionInfo pi : pis) {
                    if (pi.getP_level() == 1) {
                      content += pi.getP_col() + "=";
                      break;
                    }
                  }
                  content += ftp.part.getValues().get(i);
                  if (i < ftp.part.getValuesSize() - 1) {
                    content += ",";
                  }
                }
                content += "\n";
              } else {
                Table t = partToTbl.get(ftp.subpart.getPartitionName());
                List<PartitionInfo> pis = PartitionInfo.getPartitionInfo(t.getPartitionKeys());
                Partition pp;

                synchronized (rs) {
                  try {
                    pp = rs.getParentPartition(ftp.subpart.getDbName(), ftp.subpart.getTableName(), ftp.subpart.getPartitionName());
                  } catch (NoSuchObjectException e) {
                    LOG.error(e, e);
                    break;
                  } catch (MetaException e) {
                    LOG.error(e, e);
                    break;
                  }
                }
                content += "\tADD\t" + ftp.subpart.getDbName() + "\t" + ftp.subpart.getTableName() + "\t";
                for (PartitionInfo pi : pis) {
                  if (pi.getP_level() == 1) {
                    content += pi.getP_col() + "=";
                    for (int i = 0; i < pp.getValuesSize(); i++) {

                      content += pp.getValues().get(i);
                      if (i < pp.getValuesSize() - 1) {
                        content += ",";
                      }
                    }
                    content += "#";
                  }
                  if (pi.getP_level() == 2) {
                    content += pi.getP_col() + "=";
                    for (int i = 0; i < ftp.subpart.getValuesSize(); i++) {

                      content += ftp.subpart.getValues().get(i);
                      if (i < ftp.subpart.getValuesSize() - 1) {
                        content += ",";
                      }
                    }
                  }
                }
                content += "\n";
              }
              break;
            }
          }
        }
        for (FileToPart ftp : toDrop) {
          for (SFileLocation sfl : ftp.file.getLocations()) {
            Device device = null;
            synchronized (rs) {
              try {
                device = rs.getDevice(sfl.getDevid());
              } catch (MetaException e) {
                LOG.error(e, e);
              } catch (NoSuchObjectException e) {
                LOG.error(e, e);
              }
            }
            if (device != null && (DeviceInfo.getType(device.getProp()) == MetaStoreConst.MDeviceProp.BACKUP)) {
              content += sfl.getLocation() + "\tRemove\t" + ftp.part.getDbName() + "\t" + ftp.part.getTableName() + "\t";
              for (int i = 0; i < ftp.part.getValuesSize(); i++) {
                content += ftp.part.getValues().get(i);
                if (i < ftp.part.getValuesSize() - 1) {
                  content += "#";
                }
              }
              content += "\n";
              break;
            }
          }
        }

        try {
          FileWriter fw = new FileWriter(f.getAbsoluteFile());
          BufferedWriter bw = new BufferedWriter(fw);
          bw.write(content);
          bw.close();
        } catch (IOException e) {
          LOG.error(e, e);
          return false;
        }

        return true;
      }

      @Override
      public void run() {

        backupTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_BACKUP_TIMEOUT);
        backupQTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_BACKUPQ_TIMEOUT);
        fileSizeThreshold = hiveConf.getLongVar(HiveConf.ConfVars.DM_BACKUP_FILESIZE_THRESHOLD);

        if (last_backupTs + backupTimeout <= System.currentTimeMillis()) {
          // TODO: generate manifest.desc and tableName.desc
          Set<Partition> parts = new TreeSet<Partition>();
          Set<Subpartition> subparts = new TreeSet<Subpartition>();
          Set<FileToPart> toAdd = new HashSet<FileToPart>();
          Set<FileToPart> toDrop = new HashSet<FileToPart>();
          Queue<BackupEntry> localQ = new ConcurrentLinkedQueue<BackupEntry>();

          while (true) {
            BackupEntry be = null;

            synchronized (backupQ) {
              be = backupQ.poll();
            }
            if (be == null) {
              break;
            }
            // this is a valid entry, check if the file size is large enough
            if (be.op == BackupEntry.FOP.ADD_PART) {
              // refresh to check if the file is closed and has the proper length
              for (SFile f : be.files) {
                SFile nf;
                synchronized (rs) {
                  try {
                    nf = rs.getSFile(f.getFid());
                    if (nf != null) {
                      nf.setLocations(rs.getSFileLocations(f.getFid()));
                    } else {
                      LOG.error("Invalid SFile fid " + f.getFid() + ", not found.");
                      continue;
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                    // lately reinsert back to the queue
                    localQ.add(be);
                    break;
                  }
                }
                if (nf != null && nf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                  // this means we should wait a moment for the sfile
                  if (be.ttl + backupQTimeout >= System.currentTimeMillis()) {
                    localQ.add(be);
                  } else {
                    LOG.warn("This is a long opening file (fid " + nf.getFid() + "), might be void files.");
                  }
                  break;
                }
                if (nf != null && ((nf.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
                    nf.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
                    nf.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) &&
                    nf.getLength() >= fileSizeThreshold)) {
                  // add this file to manifest.desc
                  FileToPart ftp = new FileToPart(nf, be.part);
                  toAdd.add(ftp);
                  parts.add(be.part);
                } else {
                  LOG.warn("This file (fid " + nf.getFid() + " is ignored. (status " + nf.getStore_status() + ").");
                }
              }
            } else if (be.op == BackupEntry.FOP.DROP_PART) {
              for (SFile f : be.files) {
                // add this file to manifest.desc
                FileToPart ftp = new FileToPart(f, be.part);
                toDrop.add(ftp);
                parts.add(be.part);
              }
            } else if (be.op == BackupEntry.FOP.ADD_SUBPART) {
              // refresh to check if the file is closed and has the proper length
              for (SFile f : be.files) {
                SFile nf;
                synchronized (rs) {
                  try {
                    nf = rs.getSFile(f.getFid());
                    if (nf != null) {
                      nf.setLocations(rs.getSFileLocations(f.getFid()));
                    } else {
                      LOG.error("Invalid SFile fid " + f.getFid() + ", not found.");
                      continue;
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                    // lately reinsert back to the queue
                    localQ.add(be);
                    break;
                  }
                }
                if (nf != null && nf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                  // this means we should wait a moment for the sfile
                  if (be.ttl + backupQTimeout >= System.currentTimeMillis()) {
                    localQ.add(be);
                  } else {
                    LOG.warn("This is a long opening file (fid " + nf.getFid() + "), might be void files.");
                  }
                  break;
                }
                if (nf != null && ((nf.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
                    nf.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
                    nf.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) &&
                    nf.getLength() >= fileSizeThreshold)) {
                  // add this file to manifest.desc
                  FileToPart ftp = new FileToPart(nf, be.subpart);
                  toAdd.add(ftp);
                  subparts.add(be.subpart);
                } else {
                  LOG.warn("This file (fid " + nf.getFid() + " is ignored. (status " + nf.getStore_status() + ").");
                }
              }
            } else if (be.op == BackupEntry.FOP.DROP_SUBPART) {
              for (SFile f : be.files) {
                // add this file to manifest.desc
                FileToPart ftp = new FileToPart(f, be.subpart);
                toDrop.add(ftp);
                subparts.add(be.subpart);
              }
            }
          }
          toAdd.removeAll(toDrop);
          // generate final desc files
          if ((toAdd.size() + toDrop.size() > 0) && generateSyncFiles(parts, subparts, toAdd, toDrop)) {
            LOG.info("Generated SYNC dir around time " + System.currentTimeMillis() + ", toAdd " + toAdd.size() + ", toDrop " + toDrop.size());
          }
          last_backupTs = System.currentTimeMillis();
          synchronized (backupQ) {
            backupQ.addAll(localQ);
          }
        }
      }

    }

    public class DMDiskStatis {
      long stdev;
      long avg;
      List<Long> frees;

      public DMDiskStatis() {
        stdev = 0;
        avg = 0;
        frees = new ArrayList<Long>();
      }
    }

    public class DMTimerTask extends TimerTask {
      private RawStore trs;
      private int times = 0;
      private boolean isRunning = false;
      private final Long syncIsRunning = new Long(0);
      public long timeout = 60 * 1000; //in millisecond
      public long repDelCheck = 60 * 1000;
      public long voidFileCheck = 30 * 60 * 1000;
      public long voidFileTimeout = 12 * 3600 * 1000; // 12 hours
      public long errSFLCheck = 60 * 1000;
      public long repTimeout = 15 * 60 * 1000;
      public long delTimeout = 5 * 60 * 1000;
      public long rerepTimeout = 30 * 1000;
      public long cleanDSFStatCheck = 30 * 60 * 1000;
      public long cleanDSFStatTimeout = 24 * 3600 * 1000; // 24 hours

      public long offlineDelTimeout = 3600 * 1000; // 1 hour
      public long suspectDelTimeout = 30 * 24 * 3600 * 1000; // 30 days
      public long inactiveNodeTimeout = 3600 * 1000; // 1 hour
      public long incRepTimeout = 5 * 60 * 1000; // 5 minutes

      private long last_repTs = System.currentTimeMillis();
      private long last_rerepTs = System.currentTimeMillis();
      private long last_unspcTs = System.currentTimeMillis();
      private long last_voidTs = System.currentTimeMillis();
      private long last_inactiveNodeTs = System.currentTimeMillis();
      private long last_limitTs = System.currentTimeMillis();
      private long last_limitLeakTs = System.currentTimeMillis();
      private long last_cleanDSFStatTs = System.currentTimeMillis();
      private long last_errSFLCheckTs = System.currentTimeMillis();

      private long last_genRpt = System.currentTimeMillis();

      private long ff_start = 0;
      private long ff_range = 1000;

      private boolean useVoidCheck = false;

      public void init(HiveConf conf) throws MetaException {
        timeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_INVALIDATE_TIMEOUT);
        repDelCheck = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_REPDELCHECK_INTERVAL);
        voidFileCheck = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_VOIDFILECHECK);
        voidFileTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_VOIDFILETIMEOUT);
        repTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_REP_TIMEOUT);
        delTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_DEL_TIMEOUT);
        rerepTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_REREP_TIMEOUT);
        offlineDelTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_OFFLINE_DEL_TIMEOUT);
        suspectDelTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_SUSPECT_DEL_TIMEOUT);
        inactiveNodeTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_INACTIVE_NODE_TIMEOUT);
        cleanDSFStatTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_DSFSTAT_TIMEOUT);
        incRepTimeout = hiveConf.getLongVar(HiveConf.ConfVars.DM_CHECK_INCREP_TIMEOUT);
        ff_range = hiveConf.getLongVar(HiveConf.ConfVars.DM_FF_RANGE);
        DiskManager.identify_shared_device = hiveConf.getBoolVar(HiveConf.ConfVars.DM_IDENTIFY_SHARED_DEV);

        if (rst == RsStatus.NEWMS){
          try {
            this.trs = new RawStoreImp();
          } catch (IOException e) {
            LOG.error(e, e);
            throw new MetaException(e.getMessage());
          }
        } else {
          String rawStoreClassName = hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
          Class<? extends RawStore> rawStoreClass = (Class<? extends RawStore>) MetaStoreUtils.getClass(
              rawStoreClassName);
          this.trs = (RawStore) ReflectionUtils.newInstance(rawStoreClass, conf);
        }

      }

      private boolean __do_delete(SFileLocation loc, int flocsize) {
        NodeInfo ni = ndmap.get(loc.getNode_name());

        if (ni == null) {
          return false;
        }
        synchronized (ni.toDelete) {
          ni.toDelete.add(loc);
          LOG.info("----> Add to Node " + loc.getNode_name() + "'s toDelete " + loc.getLocation() + ", qs " + cleanQ.size() + ", " + flocsize);
        }
        // BUG-XXX: if we do NOT del or modify SFL here, we will race with reopen
        try {
          if (loc.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            loc.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.OFFLINE);
            trs.updateSFileLocation(loc);
          }
        } catch (MetaException e) {
          LOG.error(e, e);
        }
        return true;
      }

      // NOTE-XXX: for scruber Version 2.
      //
      // do_delete() try to understand how to leveling replicas. It try delete ONE highest replica
      // and many(or one) lowest replicas.
      public void do_delete(SFile f, int nr) {
        int i = 0;
        LinkedList<SFileLocation> inTypeOrder = null;
        LinkedList<Integer> types = null;

        if (nr < 0) {
          return;
        }

        synchronized (ndmap) {
          if (f.getLocationsSize() == 0 && f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
            // this means it contains non-valid locations, just delete it
            try {
              synchronized (trs) {
                LOG.info("----> Truely delete file " + f.getFid());
                trs.delSFile(f.getFid());
                return;
              }
            } catch (MetaException e) {
              LOG.error(e, e);
            }
          }
          inTypeOrder = new LinkedList<SFileLocation>();
          types = new LinkedList<Integer>();

          for (SFileLocation loc : f.getLocations()) {
            DeviceInfo di = admap.get(loc.getDevid());
            if (di != null) {
              boolean inserted = false;
              int btype = di.getType();

              for (int j = 0; j < inTypeOrder.size(); j++) {
                int atype = types.get(j);

                if (MetaStoreConst.checkDeviceOrder(btype, atype)) {
                  // insert before it
                  inTypeOrder.add(j, loc);
                  types.add(j, btype);
                  inserted = true;
                  break;
                }
              }
              if (!inserted) {
                inTypeOrder.addLast(loc);
                types.addLast(btype);
              }
            }
          }
          for (int x = 0; x < inTypeOrder.size(); x++) {
            LOG.debug(" -> do_delete() on " + DeviceInfo.getTypeStr(types.get(x)) +
                " Dev: " + inTypeOrder.get(x).getDevid());
          }

          // delete policy:
          // 1. try to remove highest L? loc;
          // 2. delete other loc randomized.
          if (types.size() - 1 < nr) {
            // this means we should change nr to types.size() - 1
            nr = types.size() - 1;
          }
          if (inTypeOrder.size() > 0) {
            if (__do_delete(inTypeOrder.get(0), f.getLocationsSize())) {
              i++;
              inTypeOrder.remove(0);
            }
          }
          Random r = new Random();
          Set<Integer> idxs = new HashSet<Integer>();

          if (nr - i > 0) {
            for (int j = 0; j < nr - i; j++) {
              idxs.add(r.nextInt(inTypeOrder.size()));
            }
            for (Integer idx : idxs) {
              // ignore any error,
              // because if ni == null, we don't known whether this loc is valid.
              __do_delete(inTypeOrder.get(idx), f.getLocationsSize());
            }
          }
        }
      }

      public void do_replicate(SFile f, int nr, int dtype, boolean isIncRep) {
        int init_size = f.getLocationsSize();
        int valid_idx = 0;
        boolean master_marked = false;
        FileLocatingPolicy flp, flp_default;
        Set<String> excludes = new TreeSet<String>();
        Set<String> excl_dev = new TreeSet<String>();
        Set<String> spec_dev = new TreeSet<String>();
        Set<String> spec_node = new TreeSet<String>();

        if (init_size <= 0) {
          LOG.error("No valid locations for file " + f.getFid());
          // FIXME: this means we should clean this file?
          if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
              f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
            LOG.warn("FID " + f.getFid() + " will be deleted(reason: no locations), however it's status is " + f.getStore_status());
            if (f.getLocationsSize() == 0) {
              synchronized (trs) {
                try {
                  // delete locations first
                  trs.delSFile(f.getFid());
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
            } else {
              do_delete(f, f.getLocationsSize());
            }
          }
          return;
        }

        // find the valid entry
        for (int i = 0; i < init_size; i++) {
          try {
            // NOTE-XXX: we allow replicate to L4 device in do_replicate. If we try to
            // replicate to L4 device, ignore node masking for L4 devices.
            if (!isSharedDevice(f.getLocations().get(i).getDevid())) {
              if (!(dtype == MetaStoreConst.MDeviceProp.SHARED &&
                  isNodeHasSD(f.getLocations().get(i).getNode_name()))) {
                excludes.add(f.getLocations().get(i).getNode_name());
              }
            }
          } catch (MetaException e1) {
            LOG.error(e1, e1);
            excludes.add(f.getLocations().get(i).getNode_name());
          } catch (NoSuchObjectException e1) {
            LOG.error(e1, e1);
            excludes.add(f.getLocations().get(i).getNode_name());
          }
          excl_dev.add(f.getLocations().get(i).getDevid());
          if (spec_dev.remove(f.getLocations().get(i).getDevid())) {
            // this backup device has already used, do not use any other backup device
            spec_dev.clear();
          }
          if (!master_marked && f.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            valid_idx = i;
            master_marked = true;
            DeviceInfo di = admap.get(f.getLocations().get(i).getDevid());
            if (di != null) {
              // FIXME: now, we do ALLOW replicate to higher level device
              if (MetaStoreConst.checkDeviceOrder(dtype, di.getType())) {
                //dtype = di.getType();
                LOG.debug("dtype <= di.getType(), might replicate to higher level.");
              }
            }
            if (f.getLocations().get(i).getNode_name().equals("")) {
              try {
                f.getLocations().get(i).setNode_name(getAnyNode(f.getLocations().get(i).getDevid()));
              } catch (MetaException e) {
                LOG.error(e, e);
                master_marked = false;
              }
            }
          }
        }

        // If master has not been marked, async replicate sfile failed.
        if (!master_marked) {
          LOG.error("Async replicate SFile " + f.getFid() + ", but no valid master FROM SFileLocations!");
          // FIXME: this means we should clean this file?
          if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
              f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
            boolean should_delete = true;

            if (f.getLocationsSize() == 0) {
              synchronized (trs) {
                try {
                  // delete locations first
                  trs.delSFile(f.getFid());
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
            } else {
              // BUG-XXX: do NOT delete a file when some SFL is in SUSPECT status
              for (SFileLocation sfl : f.getLocations()) {
                if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE ||
                    sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.SUSPECT) {
                  should_delete = false;
                }
              }
              if (should_delete) {
                do_delete(f, f.getLocationsSize());
              }
            }
            LOG.warn("FID " + f.getFid() + " deleted=" + should_delete +
                " (reason: no locations), however it's status is " + f.getStore_status());
          }
          return;
        }

        // BUG-XXX: At first, we allow replicate on nodes that didn't in current table's nodegroup.
        // This policy might be a problem, add a switch for it.
        if (hiveConf.getBoolVar(HiveConf.ConfVars.DM_FORCE_NG_POLICY) &&
            f.getDbName() != null && f.getTableName() != null) {
          try {
            Table t = null;
            synchronized (trs) {
              t = trs.getTable(f.getDbName(), f.getTableName());
            }
            if (t != null && t.getNodeGroups() != null) {
              Set<String> ngnodes = new HashSet<String>();
              Set<String> anodes = new HashSet<String>(ndmap.keySet());
              Set<String> bnodes = new HashSet<String>(ndmap.keySet());

              for (NodeGroup ng : t.getNodeGroups()) {
                for (Node n : ng.getNodes()) {
                  ngnodes.add(n.getNode_name());
                }
              }

              anodes.retainAll(ngnodes);
              bnodes.removeAll(anodes);
              excludes.addAll(bnodes);
              LOG.info("FID=" + f.getFid() + " following NGs, exclude nodes: " + bnodes);
              anodes.clear();
              bnodes.clear();
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }
        }

        flp = flp_default = new FileLocatingPolicy(excludes, excl_dev,
            FileLocatingPolicy.EXCLUDE_NODES_AND_RANDOM,
            FileLocatingPolicy.EXCLUDE_DEVS_AND_RANDOM, false);
        flp.length_hint = f.getLength();
        flp_default.length_hint = f.getLength();

        for (int i = init_size; i < (init_size + nr); i++, flp = flp_default) {
          if (i == init_size) {
            // FIXME: Use FLSelector framework here?
            String table = f.getDbName() + "." + f.getTableName();
            switch (flselector.FLSelector_switch(table)) {
            default:
            case NONE:
              break;
            case FAIR_NODES:
            case ORDERED_ALLOC_DEVS:
              flp.dev_mode = FileLocatingPolicy.ORDERED_ALLOC;
              break;
            }
            flp.accept_types = flselector.getDevTypeListAfterIncludeHint(table, dtype);
          }
          try {
            String node_name = findBestNode(flp);
            if (node_name == null) {
              LOG.warn("Could not find any best node to replicate file " + f.getFid());
              break;
            }
            // if we are selecting backup device, try to use local shortcut
            if (flp.origNode != null && flp.nodes.contains(flp.origNode)) {
              node_name = flp.origNode;
            }
            // if the valid location is a shared device, try to use local shortcut
            try {
              if (isSharedDevice(f.getLocations().get(valid_idx).getDevid()) && isSDOnNode(f.getLocations().get(valid_idx).getDevid(), node_name)) {
                // ok, reset the from loc' node name
                f.getLocations().get(valid_idx).setNode_name(node_name);
              }
            } catch (NoSuchObjectException e1) {
              LOG.error(e1, e1);
            }
            excludes.add(node_name);
            String devid = findBestDevice(node_name, flp);
            if (devid == null) {
              LOG.warn("Could not find any best device on node " + node_name + " to replicate file " + f.getFid());
              break;
            }
            excl_dev.add(devid);
            String location;
            Random rand = new Random();
            SFileLocation nloc;

            do {
              location = "/data/";
              if (f.getDbName() != null && f.getTableName() != null) {
                synchronized (trs) {
                  Table t = trs.getTable(f.getDbName(), f.getTableName());
                  if (t != null) {
                    location += t.getDbName() + "/" + t.getTableName() + "/"
                        + rand.nextInt(Integer.MAX_VALUE);
                  } else {
                    LOG.error("Inconsistent metadata detected: DB:" + f.getDbName() +
                        ", Table:" + f.getTableName());
                  }
                }
              } else {
                location += "UNNAMED-DB/UNNAMED-TABLE/" + rand.nextInt(Integer.MAX_VALUE);
              }
              nloc = new SFileLocation(node_name, f.getFid(), devid, location,
                  i, System.currentTimeMillis(),
                  isIncRep ? MetaStoreConst.MFileLocationVisitStatus.INCREP :
                    MetaStoreConst.MFileLocationVisitStatus.OFFLINE, "SFL_REP_DEFAULT");
              synchronized (trs) {
                if (trs.createFileLocation(nloc)) {
                  break;
                }
              }
            } while (true);
            f.addToLocations(nloc);

            // indicate file transfer
            JSONObject jo = new JSONObject();
            try {
              JSONObject j = new JSONObject();
              NodeInfo ni = ndmap.get(f.getLocations().get(valid_idx).getNode_name());

              if (ni == null) {
                if (nloc != null) {
                  trs.delSFileLocation(nloc.getDevid(), nloc.getLocation());
                }
                throw new IOException("Can not find Node '" + f.getLocations().get(valid_idx).getNode_name() + "' in nodemap now, is it offline?");
                                 }
              j.put("node_name", f.getLocations().get(valid_idx).getNode_name());
              j.put("devid", f.getLocations().get(valid_idx).getDevid());
              j.put("mp", ni.getMP(f.getLocations().get(valid_idx).getDevid()));
              j.put("location", f.getLocations().get(valid_idx).getLocation());
              jo.put("from", j);

              j = new JSONObject();
              ni = ndmap.get(nloc.getNode_name());
              if (ni == null) {
                throw new IOException("Can not find Node '" + nloc.getNode_name() + "' in nodemap now, is it offline?");
              }
              j.put("node_name", nloc.getNode_name());
              j.put("devid", nloc.getDevid());
              j.put("mp", ni.getMP(nloc.getDevid()));
              j.put("location", nloc.getLocation());
              jo.put("to", j);
            } catch (JSONException e) {
              LOG.error(e, e);
              continue;
            }
            synchronized (ndmap) {
              NodeInfo ni = ndmap.get(node_name);
              if (ni == null) {
                LOG.error("Can not find Node '" + node_name + "' in nodemap now, is it offline?");
              } else {
                synchronized (ni.toRep) {
                  ni.toRep.add(jo);
                  LOG.info("----> ADD " + node_name + "'s toRep " + jo);
                }
              }
            }
          } catch (IOException e) {
            LOG.error(e, e);
            break;
          } catch (MetaException e) {
            LOG.error(e, e);
          } catch (InvalidObjectException e) {
            LOG.error(e, e);
          }
        }
      }

      public void do_increplicate(SFile f, SFileLocation nloc) {
        int init_size = f.getLocationsSize();
        int valid_idx = 0;
        boolean master_marked = false;

        if (init_size <= 0) {
          LOG.error("No valid locations for file " + f.getFid());
          // FIXME: this means we should clean this file?
          if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
              f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
            LOG.warn("FID " + f.getFid() + " will be deleted(reason: no locations), however it's status is " + f.getStore_status());
            if (f.getLocationsSize() == 0) {
              synchronized (trs) {
                try {
                  // delete locations first
                  trs.delSFile(f.getFid());
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
            } else {
              do_delete(f, f.getLocationsSize());
            }
          }
          return;
        }

        // find the valid entry
        for (int i = 0; i < init_size; i++) {
          if (!master_marked &&
              f.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
            valid_idx = i;
            master_marked = true;
            if (f.getLocations().get(i).getNode_name().equals("")) {
              try {
                f.getLocations().get(i).setNode_name(getAnyNode(f.getLocations().get(i).getDevid()));
              } catch (MetaException e) {
                LOG.error(e, e);
                master_marked = false;
              }
            }
          }
        }

        // If master has not been marked, async replicate sfile failed.
        if (!master_marked) {
          LOG.error("Async replicate SFile " + f.getFid() + ", but no valid master FROM SFileLocations!");
          // FIXME: this means we should clean this file?
          if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED ||
              f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
            boolean should_delete = true;

            if (f.getLocationsSize() == 0) {
              synchronized (trs) {
                try {
                  // delete locations first
                  trs.delSFile(f.getFid());
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
            } else {
              // BUG-XXX: do NOT delete a file when some SFL is in SUSPECT status
              for (SFileLocation sfl : f.getLocations()) {
                if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE ||
                    sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.SUSPECT) {
                  should_delete = false;
                }
              }
              if (should_delete) {
                do_delete(f, f.getLocationsSize());
              }
            }
            LOG.warn("FID " + f.getFid() + " deleted=" + should_delete +
                " (reason: no locations), however it's status is " + f.getStore_status());
          }
          return;
        }

        try {
          // indicate file transfer
          JSONObject jo = new JSONObject();
          try {
            JSONObject j = new JSONObject();
            NodeInfo ni = ndmap.get(f.getLocations().get(valid_idx).getNode_name());

            if (ni == null) {
              throw new IOException("Can not find Node '" + f.getLocations().get(valid_idx).getNode_name() + "' in nodemap now, is it offline?");
            }
            j.put("node_name", f.getLocations().get(valid_idx).getNode_name());
            j.put("devid", f.getLocations().get(valid_idx).getDevid());
            j.put("mp", ni.getMP(f.getLocations().get(valid_idx).getDevid()));
            j.put("location", f.getLocations().get(valid_idx).getLocation());
            jo.put("from", j);

            j = new JSONObject();
            ni = ndmap.get(nloc.getNode_name());
            if (ni == null) {
              throw new IOException("Can not find Node '" + nloc.getNode_name() + "' in nodemap now, is it offline?");
            }
            j.put("node_name", nloc.getNode_name());
            j.put("devid", nloc.getDevid());
            j.put("mp", ni.getMP(nloc.getDevid()));
            j.put("location", nloc.getLocation());
            jo.put("to", j);
          } catch (JSONException e) {
            LOG.error(e, e);
            throw new IOException(e.getMessage());
          }
          synchronized (ndmap) {
            NodeInfo ni = ndmap.get(nloc.getNode_name());

            if (ni == null) {
              LOG.error("Can not find Node '" + nloc.getNode_name() + "' in nodemap now, is it offline?");
            } else {
              synchronized (ni.toRep) {
                ni.toRep.add(jo);
                LOG.info("----> ADD " + nloc.getNode_name() + "'s toRep " + jo);
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e, e);
        }
      }

      public void updateRunningState() {
        synchronized (syncIsRunning) {
          isRunning = false;
          LOG.debug("Timer task [" + times + "] done.");
        }
      }

      public DMDiskStatis getDMDiskStdev() {
        DMDiskStatis dds = new DMDiskStatis();
        List<Long> vals = new ArrayList<Long>();
        double avg = 0, stdev = 0;
        int nr = 0;

        synchronized (admap) {
          for (Map.Entry<String, DeviceInfo> entry : admap.entrySet()) {
            // Note: only calculate the alone and non-offline device stdev
            if ((entry.getValue().getType() == MetaStoreConst.MDeviceProp.GENERAL ||
                entry.getValue().getType() == MetaStoreConst.MDeviceProp.CACHE ||
                entry.getValue().getType() == MetaStoreConst.MDeviceProp.MASS)
                && !entry.getValue().isOffline) {
              avg += entry.getValue().free;
              nr++;
              vals.add(entry.getValue().free);
            }
          }
        }
        if (nr == 0) {
          return dds;
        }
        avg /= nr;
        for (Long free : vals) {
          stdev += (free - avg) * (free - avg);
        }
        stdev /= nr;
        stdev = Math.sqrt(stdev);

        dds.stdev = new Double(stdev).longValue();
        dds.avg = new Double(avg).longValue();
        dds.frees.addAll(vals);

        return dds;
      }

      public boolean generateReport() {
        Date d = new Date(System.currentTimeMillis());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String str = hiveConf.getVar(HiveConf.ConfVars.DM_REPORT_DIR);
        if (str == null) {
          str = System.getProperty("user.dir") + "/sotstore/reports/report-" + sdf.format(d);
        } else {
          str = str + "/sotstore/reports/report-" + sdf.format(d);
        }
        File reportFile = new File(str);
        if (!reportFile.getParentFile().exists() && !reportFile.getParentFile().mkdirs()) {
          LOG.error("Make directory " + reportFile.getParent() + " failed, can't write report data.");
          return false;
        }
        // generate report string, EX.
        //0  uptime,safemode,total_space,used_space,free_space,total_nodes,active_nodes,
        //7  total_device,active_device,
        //9  fcreate1R,fcreate1SuccR,fcreate2R,fcreate2SuccR,
        //13 freopenR,fgetR,fcloseR,freplicateR,
        //17 frmlR,frmpR,frestoreR,fdelR,
        //21 sflcreateR,sflonlineR,sflofflineR,sflsuspectR,sfldelR,
        //26 fcloseSuccR,newconn,delconn,query,
        //30 closeRepLimit,fixRepLimit,
        //32 reqQlen,cleanQlen,backupQlen,
        //35 totalReportNr,totalFileRep,totalFileDel,toRepNr,toDeleteNr,avgReportTs,
        //41 timestamp,totalVerify,totalFailRep,totalFailDel,alonediskStdev,alonediskAvg,
        //47 [alonediskFrees],
        //48 ds.qrep,ds.hrep,ds.drep,ds.qdel,ds.hdel,ds.ddel,ds.tver,ds.tvyr,
        //56 [ds.uptime],[ds.load1],truetotal,truefree,offlinefree,sharedfree,
        //62 replicate,loadstatus_bad,
        //64 l1Ttotal,l1Tfree,l1offline,
        //67 l2Ttotal,l2Tfree,l2offline,
        //70 l3Ttotal,l3Tfree,l3offline,
        //73 l4Ttotal,l4Tfree,l4offline,
        //76 localQ,MsgQ,failedQ,
        //79 l0total,l0free,l0offline,
        // {tbls},
        StringBuffer sb = new StringBuffer(2048);
        long free = 0, used = 0;
        long truetotal = 0, truefree = 0, offlinefree = 0, sharedfree = 0;
        long l0total = 0, l0free = 0, l0offline = 0;
        long l1total = 0, l1free = 0, l1offline = 0;
        long l2total = 0, l2free = 0, l2offline = 0;
        long l3total = 0, l3free = 0, l3offline = 0;
        long l4total = 0, l4free = 0, l4offline = 0;

        sb.append((System.currentTimeMillis() - startupTs) / 1000);
        sb.append("," + safeMode + ",");
        synchronized (admap) {
  	      for (Map.Entry<String, DeviceInfo> e : admap.entrySet()) {
  	        free += e.getValue().free;
  	        used += e.getValue().used;
  	        if (e.getValue().isOffline) {
  	          offlinefree += e.getValue().free;
  	        } else {
  	          truefree += e.getValue().free;
  	          truetotal += (e.getValue().free + e.getValue().used);
  	        }
  	        if (e.getValue().getType() == MetaStoreConst.MDeviceProp.SHARED ||
  	            e.getValue().getType() == MetaStoreConst.MDeviceProp.BACKUP) {
  	          sharedfree += e.getValue().free;
  	        }
  	        switch (e.getValue().getType()) {
  	        case MetaStoreConst.MDeviceProp.L0:
  	          if (e.getValue().isOffline) {
  	            l0offline += e.getValue().free;
  	          } else {
  	            l0free += e.getValue().free;
  	            l0total += (e.getValue().free + e.getValue().used);
  	          }
  	          break;
  	        case MetaStoreConst.MDeviceProp.L1:
  	          if (e.getValue().isOffline) {
                l1offline += e.getValue().free;
              } else {
                l1free += e.getValue().free;
                l1total += (e.getValue().free + e.getValue().used);
              }
  	          break;
  	        case MetaStoreConst.MDeviceProp.L2:
  	          if (e.getValue().isOffline) {
                l2offline += e.getValue().free;
              } else {
                l2free += e.getValue().free;
                l2total += (e.getValue().free + e.getValue().used);
              }
  	          break;
  	        case MetaStoreConst.MDeviceProp.L3:
  	          if (e.getValue().isOffline) {
                l3offline += e.getValue().free;
              } else {
                l3free += e.getValue().free;
                l3total += (e.getValue().free + e.getValue().used);
              }
  	          break;
  	        case MetaStoreConst.MDeviceProp.L4:
  	          if (e.getValue().isOffline) {
                l4offline += e.getValue().free;
              } else {
                l4free += e.getValue().free;
                l4total += (e.getValue().free + e.getValue().used);
              }
  	          break;
  	        }
  	      }
        }
        sb.append((used + free) + ",");
        sb.append(used + ",");
        sb.append(free + ",");
        synchronized (trs) {
          try {
            sb.append(trs.countNode() + ",");
          } catch (MetaException e) {
            sb.append("-1,");
          }
        }
        sb.append(ndmap.size() + ",");
        synchronized (trs) {
          try {
            sb.append(trs.countDevice() + ",");
          } catch (MetaException e) {
            sb.append("-1,");
          }
        }
        long totalReportNr = 0, totalFileRep = 0, totalFileDel = 0, toRepNr = 0, toDeleteNr = 0,
            avgReportTs = 0, totalVerify = 0, totalFailRep = 0, totalFailDel = 0, totalCheck = 0;
        long qrep = 0, hrep = 0, drep = 0, qdel = 0, hdel = 0, ddel = 0, tver = 0, tvyr = 0;
        List<Long> uptimes = new ArrayList<Long>();
        List<Double> load1 = new ArrayList<Double>();
        synchronized (ndmap) {
          for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
            totalReportNr += e.getValue().totalReportNr;
            totalFileRep += e.getValue().totalFileRep;
            totalFileDel += e.getValue().totalFileDel;
            totalVerify += e.getValue().totalVerify;
            totalCheck += e.getValue().totalCheck;
            toRepNr += e.getValue().toRep.size();
            toDeleteNr += e.getValue().toDelete.size();
            totalFailRep += e.getValue().totalFailRep;
            totalFailDel += e.getValue().totalFailDel;
            avgReportTs += (System.currentTimeMillis() - e.getValue().lastRptTs)/1000;
            qrep += e.getValue().qrep;
            hrep += e.getValue().hrep;
            drep += e.getValue().drep;
            qdel += e.getValue().qdel;
            hdel += e.getValue().hdel;
            ddel += e.getValue().ddel;
            tver += e.getValue().tver;
            tvyr += e.getValue().tvyr;
            uptimes.add(e.getValue().uptime);
            load1.add(e.getValue().load1);
          }
          if (ndmap.size() > 0) {
            avgReportTs /= ndmap.size();
          }
        }
        sb.append(admap.size() + ",");
        sb.append(DMProfile.fcreate1R.get() + ",");
        sb.append(DMProfile.fcreate1SuccR.get() + ",");
        sb.append(DMProfile.fcreate2R.get() + ",");
        sb.append(DMProfile.fcreate2SuccR.get() + ",");
        sb.append(DMProfile.freopenR.get() + ",");
        sb.append(DMProfile.fgetR.get() + ",");
        sb.append(DMProfile.fcloseR.get() + ",");
        sb.append(DMProfile.freplicateR.get() + ",");
        sb.append(DMProfile.frmlR.get() + ",");
        sb.append(DMProfile.frmpR.get() + ",");
        sb.append(DMProfile.frestoreR.get() + ",");
        sb.append(DMProfile.fdelR.get() + ",");
        sb.append(DMProfile.sflcreateR.get() + ",");
        sb.append(DMProfile.sflonlineR.get() + ",");
        sb.append(DMProfile.sflofflineR.get() + ",");
        sb.append(DMProfile.sflsuspectR.get() + ",");
        sb.append(DMProfile.sfldelR.get() + ",");
        sb.append(DMProfile.fcloseSuccRS.get() + ",");
        sb.append(DMProfile.newConn.get() + ",");
        sb.append(DMProfile.delConn.get() + ",");
        sb.append(DMProfile.query.get() + ",");
        sb.append(closeRepLimit.get() + ",");
        sb.append(fixRepLimit.get() + ",");
        synchronized (repQ) {
          sb.append(repQ.size() + ",");
        }
        synchronized (cleanQ) {
          sb.append(cleanQ.size() + ",");
        }
        synchronized (backupQ) {
          sb.append(backupQ.size() + ",");
        }
        sb.append(totalReportNr + ",");
        sb.append(totalFileRep + ",");
        sb.append(totalFileDel + ",");
        sb.append(toRepNr + ",");
        sb.append(toDeleteNr + ",");
        sb.append(avgReportTs + ",");
        sb.append((System.currentTimeMillis() / 1000) + ",");
        sb.append(totalVerify + ",");
        sb.append(totalFailRep + ",");
        sb.append(totalFailDel + ",");
        DMDiskStatis dds = getDMDiskStdev();
        sb.append(dds.stdev + ",");
        sb.append(dds.avg + ",");
        for (Long fnr : dds.frees) {
          sb.append(fnr + ";");
        }
        sb.append(",");
        sb.append(qrep + ",");
        sb.append(hrep + ",");
        sb.append(drep + ",");
        sb.append(qdel + ",");
        sb.append(hdel + ",");
        sb.append(ddel + ",");
        sb.append(tver + ",");
        sb.append(tvyr + ",");
        for (Long uptime : uptimes) {
          sb.append(uptime + ";");
        }
        sb.append(",");
        for (Double l : load1) {
          sb.append(l + ";");
        }
        sb.append(",");
        sb.append(truetotal + ",");
        sb.append(truefree + ",");
        sb.append(offlinefree + ",");
        sb.append(sharedfree + ",");
        sb.append(DMProfile.replicate.get() + ",");
        sb.append(DMProfile.loadStatusBad.get() + ",");
        sb.append(l1total + ",");
        sb.append(l1free + ",");
        sb.append(l1offline + ",");
        sb.append(l2total + ",");
        sb.append(l2free + ",");
        sb.append(l2offline + ",");
        sb.append(l3total + ",");
        sb.append(l3free + ",");
        sb.append(l3offline + ",");
        sb.append(l4total + ",");
        sb.append(l4free + ",");
        sb.append(l4offline + ",");
        sb.append(MsgServer.getLocalQueueSize() + ",");
        sb.append(MsgServer.getQueueSize() + ",");
        sb.append(MsgServer.getFailedQueueSize() + ",");
        sb.append(l0total + ",");
        sb.append(l0free + ",");
        sb.append(l0offline + ",");
        sb.append("\n");

        // generate report file
        FileWriter fw = null;
        try {
          if (!reportFile.exists()) {
            reportFile.createNewFile();
          }
          fw = new FileWriter(reportFile.getAbsoluteFile(), true);
          BufferedWriter bw = new BufferedWriter(fw);
          bw.write(sb.toString());
          bw.close();
        } catch (IOException e) {
          LOG.error(e, e);
          return false;
        }
        return true;
      }

      @Override
      public void run() {
        // check if we are in slave role
        switch (role) {
        default:
        case SLAVE:
          return;
        case MASTER:;
        }

        try {
          times++;
          useVoidCheck = hiveConf.getBoolVar(HiveConf.ConfVars.DM_USE_VOID_CHECK);

          // iterate the map, and invalidate the Node entry
          List<String> toInvalidate = new ArrayList<String>();

          for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
            if (entry.getValue().lastRptTs + timeout < System.currentTimeMillis()) {
              // invalid this entry
              LOG.info("TIMES[" + times + "] " + "Invalidate Entry '" + entry.getKey() + "' for timeout(" + timeout/1000 + ").");
              toInvalidate.add(entry.getKey());
            }
          }

          for (String node : toInvalidate) {
            synchronized (ndmap) {
              removeFromNDMapWTO(node, System.currentTimeMillis());
            }
          }
          // FIXME: ReplicateRequestQueue handling!!
          int xnr = 0;
          while (xnr < 50) {
            ReplicateRequest rr = rrq.poll();

            if (rr == null) {
              break;
            }
            xnr++;
            SFile sf = trs.getSFile(rr.fid);
            if (sf == null) {
              LOG.warn("Replicate request " + rr + " ERROR: file not exists.");
              continue;
            }
            // BUG-XXX: do NOT replicate open files!
            if (sf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
              if ((rr.dtype & MetaStoreConst.MDeviceProp.__INCREP_FLAG__) != 0) {
                // this means user want to do increp,
                // but we should check if we can issue a new inc replicate request
                boolean abort = false;

                if (sf.getLocationsSize() > 0) {
                  for (SFileLocation sfl : sf.getLocations()) {
                    if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
                      abort = true;
                      break;
                    }
                  }
                }
                if (abort) {
                  LOG.warn("Replicate request " + rr + " BAD: do not submit. (Duplicate INCREP)");
                  synchronized (incRepFiles) {
                    if (incRepFiles.get(sf.getFid()) == null) {
                      incRepFiles.put(sf.getFid(), System.currentTimeMillis() -
                          HiveConf.getLongVar(hiveConf, ConfVars.DM_CHECK_INCREP_TIMEOUT) / 2);
                    }
                  }
                } else {
                  do_replicate(sf, 1, rr.dtype & MetaStoreConst.MDeviceProp.__TYPE_MASK__, true);
                  synchronized (incRepFiles) {
                    incRepFiles.put(sf.getFid(), System.currentTimeMillis());
                  }
                  LOG.info("Replicate request " + rr + " OK: submitted.");
                }
              } else {
                LOG.warn("Replicate request " + rr + " BAD: do not submit. (INCREATE)");
              }
            } else {
              do_replicate(sf, 1, rr.dtype & MetaStoreConst.MDeviceProp.__TYPE_MASK__, false);
              LOG.info("Replicate request " + rr + " OK: submitted.");
            }
          }

          synchronized (syncIsRunning) {
            if (isRunning) {
              return;
            } else {
              isRunning = true;
            }
          }

          if (last_limitTs + 1800 * 1000 < System.currentTimeMillis()) {
            if (closeRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT)) {
              closeRepLimit.incrementAndGet();
            }
            if (fixRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_FIX_REP_LIMIT)){
              fixRepLimit.incrementAndGet();
            }
            last_limitTs = System.currentTimeMillis();
          }

          if (last_limitLeakTs + 300 * 1000 < System.currentTimeMillis()) {
            if (closeRepLimit.get() < 10) {
              int ds_rep = 0;
              synchronized (ndmap) {
                for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
                  ds_rep += e.getValue().qrep + e.getValue().hrep + e.getValue().drep;
                }
              }
              // BUG-XXX: if all nodes go offline, then we get ds_rep = 0.
              // In this situation, enlarge closeRepLimit might lead to many failed rep
              // attampts and delete the pending reps. It is OK.
              if (closeRepLimit.get() + ds_rep < hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT) / 2) {
                LOG.info("Enlarge closeRepLimit on low MS " + closeRepLimit.get() + " and low DS " + ds_rep);
                closeRepLimit.set(hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT));
              }
            }
            last_limitLeakTs = System.currentTimeMillis();
          }

          // check any under/over/linger/increp files
          if (last_repTs + repDelCheck < System.currentTimeMillis()) {
            // get the file list
            List<SFile> under = new ArrayList<SFile>();
            List<SFile> over = new ArrayList<SFile>();
            List<SFile> linger = new ArrayList<SFile>();
            List<SFile> increp = new ArrayList<SFile>();
            Map<SFile, Integer> munder = new TreeMap<SFile, Integer>();
            Map<SFile, Integer> mover = new TreeMap<SFile, Integer>();
            long total_file_nr = 0;

            LOG.info("Check Under/Over Replicated or Lingering Files R{" + ff_start + "," + (ff_start + ff_range) +
                "} [" + times + "]");
            synchronized (trs) {
              try {
                trs.findFiles(under, over, linger, increp, ff_start, ff_start + ff_range);
                ff_start += ff_range;
                total_file_nr = trs.countFiles();
                if (ff_start > total_file_nr) {
                  ff_start = 0;
                }
              } catch (JDOObjectNotFoundException e) {
                LOG.error(e, e);
                updateRunningState();
                return;
              } catch (MetaException e) {
                LOG.error(e, e);
                updateRunningState();
                return;
              }
            }
            LOG.info("OK, get under " + under.size() + ", over " + over.size() +
                ", increp " + increp.size() + ", linger " + linger.size() +
                " [" + String.format("%.2f%%", 100.0 * (double)ff_start / total_file_nr) + "]");

            // handle under replicated files
            for (SFile f : under) {
              // check whether we should issue a re-replicate command
              int nr = 0;

              LOG.info("check under replicated files for fid " + f.getFid());
              for (SFileLocation fl : f.getLocations()) {
                if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE ||
                    ((fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE ||
                      fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) &&
                        fl.getUpdate_time() + repTimeout > System.currentTimeMillis())) {
                  nr++;
                }
              }
              if (nr < f.getRep_nr()) {
                munder.put(f, f.getRep_nr() - nr);
              }
            }

            for (Map.Entry<SFile, Integer> entry : munder.entrySet()) {
              do_replicate(entry.getKey(), entry.getValue().intValue(),
                  MetaStoreConst.MDeviceProp.__AUTOSELECT_R3__, false);
            }

            // handle over replicated files
            for (SFile f : over) {
              // check whether we should issue a del command
              int nr = 0;

              LOG.info("check over replicated files for fid " + f.getFid());
              for (SFileLocation fl : f.getLocations()) {
                if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                  nr++;
                }
              }
              if (nr > f.getRep_nr()) {
                mover.put(f, nr - f.getRep_nr());
              }
            }
            for (Map.Entry<SFile, Integer> entry : mover.entrySet()) {
              do_delete(entry.getKey(), entry.getValue().intValue());
              // double check the file status, if it is CLOSED, change it to REPLICATED
              if (entry.getKey().getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
                synchronized (trs) {
                  try {
                    SFile saved = trs.getSFile(entry.getKey().getFid());
                    if (saved != null && saved.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
                      saved.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
                      trs.updateSFile(saved);
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                  }
                }
              }
            }

            // handle lingering files
            Set<SFileLocation> s = new TreeSet<SFileLocation>();
            Set<SFile> sd = new TreeSet<SFile>();
            for (SFile f : linger) {
              LOG.info("check lingering files for fid " + f.getFid());
              if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
                sd.add(f);
                continue;
              }
              boolean do_clean = false, ngsize_exceeded = false;

              if (f.getDbName() != null && f.getTableName() != null) {
                try {
                  Table tbl = trs.getTable(f.getDbName(), f.getTableName());
                  long ngsize = 0;

                  if (tbl == null) {
                    LOG.error("Inconsistent metadata detected: DB:" + f.getDbName() +
                        ", Table:" + f.getTableName());
                    continue;
                  }
                  if (tbl.getNodeGroupsSize() > 0) {
                    for (NodeGroup ng : tbl.getNodeGroups()) {
                      ngsize += ng.getNodesSize();
                    }
                  }
                  if (ngsize <= f.getLocationsSize()) {
                    // this means we should do cleanups
                    do_clean = true;
                    ngsize_exceeded = true;
                  }
                  if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
                    // this means we should clean up the OFFLINE/INCREP locs
                    // BUG-XXX: refer to next BUG-XXX hint
                    do_clean = true;
                  }
                } catch (MetaException e) {
                  LOG.error(e, e);
                  continue;
                }
              }

              for (SFileLocation fl : f.getLocations()) {
                if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE ||
                    fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
                  // if this OFFLINE sfl exist too long or we do exceed the ng'size limit
                  if (do_clean && f.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED) {
                    // BUG-XXX: if we want to re-replicate a sfile and it is in REPLICATED state, then
                    // we should keep it for at least half offlineDelTimeout
                    if (fl.getUpdate_time() + (offlineDelTimeout / 2) < System.currentTimeMillis()) {
                      s.add(fl);
                    }
                  } else if (do_clean || fl.getUpdate_time() + offlineDelTimeout < System.currentTimeMillis()) {
                    s.add(fl);
                  }
                }
                // NOTE-XXX: do NOT clean SUSPECT sfl, because we hope it online again. Thus,
                // we set the timeout to 30 days.
                if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.SUSPECT) {
                  // if this SUSPECT sfl exist too long or we do exceed the ng'size limit
                  // BUG-XXX: if the file had been replicated yet, do_clean always be true! thus, we
                  // should ignore do_clean flag here!
                  if (ngsize_exceeded || fl.getUpdate_time() + suspectDelTimeout < System.currentTimeMillis()) {
                    s.add(fl);
                  }
                }
              }
            }

            for (SFileLocation fl : s) {
              synchronized (trs) {
                // BUG-XXX: space leaking? we should trigger a delete to dservice; otherwise enable VERIFY to auto clean the dangling dirs.
                //asyncDelSFL(fl);
                try {
                  trs.delSFileLocation(fl.getDevid(), fl.getLocation());
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
            }
            for (SFile f : sd) {
              do_delete(f, f.getLocationsSize());
            }

            // check incr replicated files
            // Step 1: try to check existing increp files
            for (int cnt = 0; cnt <
                Math.min(HiveConf.getLongVar(hiveConf, ConfVars.DM_INCREMENT_REP_CHECK_SIZE),
                    incRepFiles.size()); cnt++) {
              synchronized (incRepFiles) {
                Long fid = incRepFiles.higherKey(incRepFid);
                if (fid == null) {
                  incRepFid = -1L;
                } else {
                  // only check this sfile when we issue last increp request at
                  // least incRepTimeout/2 miliseconds ago
                  if (System.currentTimeMillis() - incRepFiles.get(fid) >=
                      incRepTimeout / 2) {
                    synchronized (trs) {
                      try {
                        SFile f = trs.getSFile(fid);
                        if (f != null) {
                          increp.add(f);
                        }
                      } catch (Exception e) {
                        LOG.error(e, e);
                      }
                    }
                  }
                  incRepFid = fid;
                }
              }
            }
            // Step 2: iterate increp set to do inc replicate
            for (SFile f : increp) {
              // check if we should INC replicate this SFile
              SFileLocation sfl_target = null;

              if (f.getLocations() != null) {
                for (SFileLocation fl : f.getLocations()) {
                  if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
                    sfl_target = fl;
                    break;
                  }
                }
              }
              if (hiveConf.getBoolVar(ConfVars.DM_INCREMENT_REP)) {
                if (sfl_target == null) {
                  if (f.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                    // create a new INCREP state filelocation
                    do_replicate(f, 1, MetaStoreConst.MDeviceProp.__AUTOSELECT_R4__, true);
                    // add this sfile to increp list
                    synchronized (incRepFiles) {
                      incRepFiles.put(f.getFid(), System.currentTimeMillis());
                    }
                  } else {
                    // non INCREATE state sfile, just ignore it, and remove it from incRepFiles map
                    synchronized (incRepFiles) {
                      incRepFiles.remove(f.getFid());
                    }
                  }
                } else if (System.currentTimeMillis() - sfl_target.getUpdate_time() >= incRepTimeout) {
                  do_increplicate(f, sfl_target);
                  // add this sfile to increp list
                  synchronized (incRepFiles) {
                    incRepFiles.put(f.getFid(), System.currentTimeMillis());
                  }
                }
              }
            }

            last_repTs = System.currentTimeMillis();
            under.clear();
            over.clear();
            linger.clear();
            increp.clear();
          }

          // check invalid file locations on invalid devices
          if (last_rerepTs + repDelCheck < System.currentTimeMillis()) {
            for (Map.Entry<String, Long> entry : toReRep.entrySet()) {
              boolean ignore = false;
              boolean delete = true;

              if (entry.getValue() + rerepTimeout < System.currentTimeMillis()) {
                if (admap.get(entry.getKey()) != null) {
                  // found it! ignore this device and remove it now
                  toReRep.remove(entry.getKey());
                  ignore = true;
                }
                if (!ignore) {
                  List<SFileLocation> sfl;

                  synchronized (trs) {
                    try {
                      sfl = trs.getSFileLocations(entry.getKey(), System.currentTimeMillis(), 0);
                    } catch (MetaException e) {
                      LOG.error(e, e);
                      continue;
                    }
                  }
                  for (SFileLocation fl : sfl) {
                    if (toReRep.containsKey(fl.getDevid())) {
                      if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                        LOG.info("Change FileLocation " + fl.getDevid() + ":" + fl.getLocation() +
                            " to SUSPECT state! (by toReRep)");
                        synchronized (trs) {
                          fl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.SUSPECT);
                          try {
                            trs.updateSFileLocation(fl);
                          } catch (MetaException e) {
                            LOG.error(e, e);
                            delete = false;
                            continue;
                          }
                        }
                      }
                    }
                  }
                }
                if (delete) {
                  toReRep.remove(entry.getKey());
                }
              }
            }
            last_rerepTs = System.currentTimeMillis();
          }

          // check files on unspc devices, try to unspc it
          if (last_unspcTs + repDelCheck < System.currentTimeMillis()) {
            // Step 1: generate the SUSPECT file list
            List<SFileLocation> sfl;
            Set<String> hitDevs = new TreeSet<String>();
            int nr = 0;

            LOG.info("Check SUSPECT SFileLocations [" + times + "] begins ...");
            synchronized (trs) {
              try {
                sfl = trs.getSFileLocations(MetaStoreConst.MFileLocationVisitStatus.SUSPECT);
                // Step 2: TODO: try to probe the target file
                for (SFileLocation fl : sfl) {
                  // check if this device is back
                  if (toUnspc.containsKey(fl.getDevid()) || admap.containsKey(fl.getDevid())) {
                    hitDevs.add(fl.getDevid());
                    fl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
                    try {
                      trs.updateSFileLocation(fl);
                    } catch (MetaException e) {
                      LOG.error(e, e);
                      continue;
                    }
                    nr++;
                  }
                }
                if (hitDevs.size() > 0) {
                  for (String dev : hitDevs) {
                    toUnspc.remove(dev);
                  }
                }
              } catch (MetaException e) {
                LOG.error(e, e);
              } catch (Exception e) {
                LOG.error(e, e);
              }
            }

            LOG.info("Check SUSPECT SFileLocations [" + times + "] " + nr + " changed.");
            last_unspcTs = System.currentTimeMillis();
          }

          // check void files
          if (useVoidCheck && last_voidTs + voidFileCheck < System.currentTimeMillis()) {
            List<SFile> voidFiles = new ArrayList<SFile>();

            synchronized (trs) {
              try {
                trs.findVoidFiles(voidFiles);
              } catch (MetaException e) {
                LOG.error(e, e);
                voidFiles.clear();
              } catch (Exception e) {
                LOG.error(e, e);
                voidFiles.clear();
              }
            }
            for (SFile f : voidFiles) {
              boolean isVoid = true;

              if (f.getLocationsSize() > 0) {
                // check file location's update time, if it has not update in last 12 hours, then it is void!
                for (SFileLocation fl : f.getLocations()) {
                  if (fl.getUpdate_time() + voidFileTimeout > System.currentTimeMillis()) {
                    isVoid = false;
                    break;
                  }
                }
              }

              if (isVoid) {
                // ok, mark the file as deleted
                synchronized (trs) {
                  // double get the file now
                  SFile nf;
                  try {
                    nf = trs.getSFile(f.getFid());
                    if (nf == null) {
                      continue;
                    }
                    if (nf.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                      continue;
                    }
                    if (nf.getLocationsSize() > 0) {
                      boolean ok = false;
                      for (SFileLocation fl : nf.getLocations()) {
                        if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                          ok = true;
                          break;
                        }
                      }
                      if (ok) {
                        continue;
                      }
                    }

                    LOG.info("Mark file (fid " + nf.getFid() + ") as void file to physically delete.");
                    nf.setStore_status(MetaStoreConst.MFileStoreStatus.RM_PHYSICAL);
                    trs.updateSFile(nf);

                  } catch (MetaException e1) {
                    LOG.error(e1, e1);
                  }
                }
              }
            }
            last_voidTs = System.currentTimeMillis();
          }

          // check long-stand inactive nodes to handle under replicated files
          if (last_inactiveNodeTs + 120000 < System.currentTimeMillis()) {
            HashMap<String, Long> newIN = new HashMap<String, Long>();
            boolean isAdd = false;
            List<Node> lns = null;

            synchronized (trs) {
              lns = trs.getAllNodes();
            }
            if (lns != null) {
              for (Node n : lns) {
                if (!ndmap.containsKey(n.getNode_name())) {
                  Long ts = inactiveNodes.get(n.getNode_name());
                  if (ts == null) {
                    // new one
                    isAdd = true;
                    newIN.put(n.getNode_name(), System.currentTimeMillis());
                  } else {
                    newIN.put(n.getNode_name(), ts);
                  }
                }
              }
              if (isAdd || inactiveNodes.size() < newIN.size()) {
                LOG.info("Detect new inactive nodes. CUR " + newIN);
              }
              inactiveNodes.clear();
              inactiveNodes = newIN;

              // check long-stand entries
              for (Map.Entry<String, Long> entry : inactiveNodes.entrySet()) {
                if (entry.getValue() + inactiveNodeTimeout < System.currentTimeMillis()) {
                  List<String> __devs;
                  // get all devices on this node
                  synchronized (trs) {
                    __devs = trs.listDevsByNode(entry.getKey());
                  }
                  // add all devices to toReRep
                  if (__devs != null) {
                    for (String devid : __devs) {
                      boolean isQd = (toReRep.putIfAbsent(devid, System.currentTimeMillis()) == null);
                      if (isQd) {
                        removeFromADMap(devid, entry.getKey());
                        toUnspc.remove(devid);
                        LOG.info("Queue Device " + devid +
                          " on toReRep set by inactive node " + entry.getKey());
                      }
                    }
                  }
                }
              }
            }
            last_inactiveNodeTs = System.currentTimeMillis();
          }

          // check DSFStat info
          if (last_cleanDSFStatTs + cleanDSFStatCheck < System.currentTimeMillis()) {
            LOG.info("Reap DSFStat info that longer than " + (cleanDSFStatTimeout / 1000) + " seconds.");
            SysMonitor.dsfstat.cleanDSFStat(cleanDSFStatTimeout);
            last_cleanDSFStatTs = System.currentTimeMillis();
          }

          // issue SFL check cmd by errSFL
          if (last_errSFLCheckTs + errSFLCheck < System.currentTimeMillis() && errSFL.size() > 0) {
            LOG.info("Issue SFL digest checking requests for " + errSFL.size() + " SFLs.");
            HashMap<String, Long> lmap = new HashMap<String, Long>();
            lmap.putAll(errSFL);
            errSFL.clear();
            for (Map.Entry<String, Long> e : lmap.entrySet()) {
              String[] a = e.getKey().split(":");
              SFileLocation loc = null;
              if (a != null && a.length >= 2) {
                synchronized (trs) {
                  try {
                    loc = trs.getSFileLocation(a[0], a[1]);
                  } catch (Exception e2) {
                    LOG.error(e2, e2);
                  }
                  if (loc != null) {
                    NodeInfo ni = ndmap.get(loc.getNode_name());
                    if (ni != null) {
                      synchronized (ni.toCheck) {
                        ni.toCheck.add(loc);
                        LOG.info("----> Add to Node " + loc.getNode_name() + "'s toCheck " + loc.getLocation());
                      }
                    }
                  }
                }
              }
            }
            lmap.clear();
            last_errSFLCheckTs = System.currentTimeMillis();
          }

          if (last_genRpt + 60000 <= System.currentTimeMillis()) {
            generateReport();
            last_genRpt = System.currentTimeMillis();
          }

          updateRunningState();
        } catch (Exception e) {
          LOG.error(e, e);
        }
      }
    }

    public DiskManager(HiveConf conf, Log LOG) throws IOException, MetaException {
      this(conf, LOG, RsStatus.OLDMS);
    }

    public DiskManager(HiveConf conf, Log LOG, RsStatus rsType) throws IOException, MetaException {
      this.hiveConf = conf;
      this.LOG = LOG;

      String roleStr = conf.getVar(HiveConf.ConfVars.DM_ROLE);
      if (roleStr.equalsIgnoreCase("master")) {
        DiskManager.role = Role.MASTER;
      } else if (roleStr.equalsIgnoreCase("slave")) {
        DiskManager.role = Role.SLAVE;
      } else {
        DiskManager.role = Role.SLAVE;
      }

      if (rsType == RsStatus.NEWMS){
        rs = new RawStoreImp();
        rs_s1 = new RawStoreImp();
      } else if (rsType == RsStatus.OLDMS){
        String rawStoreClassName = hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
        Class<? extends RawStore> rawStoreClass = (Class<? extends RawStore>) MetaStoreUtils.getClass(
          rawStoreClassName);
        this.rs = (RawStore) ReflectionUtils.newInstance(rawStoreClass, conf);
        this.rs_s1 = (RawStore) ReflectionUtils.newInstance(rawStoreClass, conf);
      } else{
        throw new IOException("Wrong RawStore Type!");
      }
      this.rst = rsType;
      ndmap = new ConcurrentHashMap<String, NodeInfo>();
      admap = new ConcurrentHashMap<String, DeviceInfo>();
      dnmap = new ConcurrentHashMap<String, Set<String>>();
      blacklisted = new ConcurrentHashMap<String, DeviceInfo>();
      deviceTracked = new ConcurrentHashMap<String, DeviceTrack>();
      closeRepLimit.set(hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT));
      fixRepLimit.set(hiveConf.getLongVar(HiveConf.ConfVars.DM_FIX_REP_LIMIT));
      init();
    }

    private void addToDNMap(String dev, String node) {
      Set<String> s = dnmap.get(dev);
      if (s == null) {
        final Set<String> n = new ConcurrentSkipListSet<String>();
        s = dnmap.putIfAbsent(dev, n);
        if (s == null) {
          s = n;
        }
      }
      s.add(node);
    }

    private void removeFromDNMap(String dev, String node) {
      if (node == null) {
        dnmap.remove(dev);
      } else {
        Set<String> s = dnmap.get(dev);
        if (s == null) {
          final Set<String> n = new ConcurrentSkipListSet<String>();
          s = dnmap.putIfAbsent(dev, n);
          if (s == null) {
            s = n;
          }
        }
        s.remove(node);
      }
    }

    public List<String> getNodesByDevice(String dev) {
      List<String> r = new ArrayList<String>();
      Set<String> s = dnmap.get(dev);

      if (s == null) {
        return r;
      }
      r.addAll(s);

      return r;
    }

    public void init() throws IOException, MetaException {
      int listenPort = hiveConf.getIntVar(HiveConf.ConfVars.DISKMANAGERLISTENPORT);

      LOG.info("Starting DiskManager Role " + hiveConf.getVar(HiveConf.ConfVars.DM_ROLE) +
          " on port " + listenPort + ", multicast enabled=" +
          hiveConf.getVar(HiveConf.ConfVars.DM_USE_MCAST));

      if (hiveConf.getBoolVar(HiveConf.ConfVars.DM_USE_MCAST)) {
        server = new MulticastSocket(listenPort);
        ((MulticastSocket)server).setTimeToLive(5);
        ((MulticastSocket)server).joinGroup(InetAddress.getByName(
            hiveConf.getVar(HiveConf.ConfVars.DM_MCAST_GROUP_IP)));
      } else {
        server = new DatagramSocket(listenPort);
      }

      alternateURI = hiveConf.getVar(HiveConf.ConfVars.DM_ALTERNATE_URI);

      dmt = new DMThread("DiskManagerThread");
      dmct = new DMCleanThread("DiskManagerCleanThread");
      dmrt = new DMRepThread("DiskManagerRepThread");
      dmcmdt = new DMCMDThread("DiskManagerCMDThread");
      dmtt.init(hiveConf);
      timer.schedule(dmtt, 0, 5000);
      bktimer.schedule(bktt, 0, 5000);
    }

    public void release_fix_limit() {
      fixRepLimit.incrementAndGet();
    }

    public String getAnyNode(String devid) throws MetaException {
      Set<String> r = new TreeSet<String>();

      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          NodeInfo ni = e.getValue();

          if (devid == null) {
            // anyone is ok
            r.add(e.getKey());
          } else {
            if (ni.dis != null && ni.dis.size() > 0) {
              for (DeviceInfo di : ni.dis) {
                if (di.dev.equalsIgnoreCase(devid)) {
                  r.add(e.getKey());
                }
              }
            }
          }
        }
      }

      if (r.size() == 0) {
        throw new MetaException("Could not find any avaliable Node that attached device: " + devid);
      }
      Random rand = new Random();
      return r.toArray(new String[0])[rand.nextInt(r.size())];
    }

    public List<String> getActiveNodes() throws MetaException {
      List<String> r = new ArrayList<String>();

      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          r.add(e.getKey());
        }
      }

      return r;
    }

    public List<String> getActiveDevices() throws MetaException {
      List<String> r = new ArrayList<String>();

      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          for (DeviceInfo di : e.getValue().dis) {
            r.add(di.dev);
          }
        }
      }

      return r;
    }

    public boolean markSFileLocationStatus(SFile toMark) throws MetaException {
      boolean marked = false;
      Set<String> activeDevs = new HashSet<String>();

      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          for (DeviceInfo di : e.getValue().dis) {
            activeDevs.add(di.dev);
          }
        }
      }

      for (SFileLocation sfl : toMark.getLocations()) {
        if (!activeDevs.contains(sfl.getDevid()) && sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
          sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.SUSPECT);
          marked = true;
        }
      }

      return marked;
    }

    public String getNodeInfo() throws MetaException {
      String r = "", prefix = " ";

      r += "MetaStore Server Disk Manager listening @ " + hiveConf.getIntVar(HiveConf.ConfVars.DISKMANAGERLISTENPORT);
      r += "\nActive Node Infos: {\n";
      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          r += prefix + " " + e.getKey() + " -> " + "Rpt TNr: " + e.getValue().totalReportNr +
              ", TREP: " + e.getValue().totalFileRep +
              ", TDEL: " + e.getValue().totalFileDel +
              ", TVER: " + e.getValue().totalVerify +
              ", TVYR: " + e.getValue().totalVYR +
              ", QREP: " + e.getValue().toRep.size() +
              ", QDEL: " + e.getValue().toDelete.size() +
              ", Last Rpt " + (System.currentTimeMillis() - e.getValue().lastRptTs)/1000 + "s ago, {\n";
          r += prefix + e.getValue().lastReportStr + "}\n";
        }
      }
      r += "}\n";

      return r;
    }

    public String getDMStatus() throws MetaException {
      String r = "";
      Long[] devnr = new Long[MetaStoreConst.MDeviceProp.__MAX__];
      Long[] adevnr = new Long[MetaStoreConst.MDeviceProp.__MAX__];
      long free = 0, used = 0, offlinenr = 0, truetotal = 0, truefree = 0;
      long ramFree = 0, ramUsed = 0, ramTruetotal = 0, ramTruefree = 0;
      long cacheFree = 0, cacheUsed = 0, cacheTruetotal = 0, cacheTruefree = 0;
      long generalFree = 0, generalUsed = 0, generalTruetotal = 0, generalTruefree = 0;
      long massFree = 0, massUsed = 0, massTruetotal = 0, massTruefree = 0;
      long shareFree = 0, shareUsed = 0, shareTruetotal = 0, shareTruefree = 0;
      Set<String> offlinedevs = new TreeSet<String>();
      String ANSI_RESET = "\u001B[0m";
      String ANSI_RED = "\u001B[31m";
      String ANSI_GREEN = "\u001B[32m";
      String color;

      for (int i = 0; i < devnr.length; i++) {
        devnr[i] = new Long(0);
        adevnr[i] = new Long(0);
      }

      r += "Uptime " + ((System.currentTimeMillis() - startupTs) / 1000) + " s, ";
      r += "Timestamp " + System.currentTimeMillis() / 1000 + " Version 2.0.1a\n";
      r += "MetaStore Server Disk Manager listening @ " + hiveConf.getIntVar(HiveConf.ConfVars.DISKMANAGERLISTENPORT);
      r += "\nSafeMode: " + safeMode + "\n";
      r += "Multicast: " + hiveConf.getVar(HiveConf.ConfVars.DM_USE_MCAST) + "\n";
      r += "Quota: " + (hiveConf.getBoolVar(HiveConf.ConfVars.DM_USE_QUOTA) ? "enabled" : "disabled") + "\n";
      r += "IdentifySD: " + identify_shared_device + "\n";
      r += "Role: " + role + "\n";
      r += "HA SID: " + serverId + "\n";
      r += "AlterURI: " + alternateURI + "\n";
      r += "Per-IP-Connections: {\n";
      for (Map.Entry<String, AtomicLong> e : HiveMetaStoreServerEventHandler.perIPConns.entrySet()) {
        r += " " + e.getKey() + " -> " + e.getValue().get() + "\n";
      }
      r += "}\n";
      synchronized (rs) {
        r += "Total nodes " + rs.countNode() + ", active nodes " + ndmap.size() + "\n";
      }
      r += "Active Node-Device map: {\n";
      synchronized (ndmap) {
        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          // if we detect node reboot in last 24hrs, we set it to RED color
          r += " ";
          if (e.getValue().uptime > 0 && e.getValue().uptime <= 86400) {
            r += ANSI_RED + e.getKey() + ANSI_RESET;
          } else {
            r += e.getKey();
          }
          r += " -> " + "[";
          synchronized (e.getValue()) {
            if (e.getValue().dis != null) {
              for (DeviceInfo di : e.getValue().dis) {
                r += (di.isOffline ? "OF" : "ON") + ":" + di.getTypeStr() +
                    ":" + di.getUsage() + "/" + di.getQuota() + ":" + di.dev + ",";
                switch (di.getType()) {
                case MetaStoreConst.MDeviceProp.GENERAL:
                case MetaStoreConst.MDeviceProp.BACKUP:
                case MetaStoreConst.MDeviceProp.SHARED:
                case MetaStoreConst.MDeviceProp.BACKUP_ALONE:
                case MetaStoreConst.MDeviceProp.MASS:
                case MetaStoreConst.MDeviceProp.CACHE:
                case MetaStoreConst.MDeviceProp.RAM:
                  devnr[di.getType()]++;
                case -1:
                  break;
                }
              }
            }
          }
          r += "]\n";
        }
      }
      r += "}\n";

	    synchronized (admap) {
	      for (Map.Entry<String, DeviceInfo> e : admap.entrySet()) {
	        if (dmsnr % 15 == 0) {
	          synchronized (rs) {
	            Device d;
	            try {
	              d = rs.getDevice(e.getKey());
	              e.getValue().prop = d.getProp();
	              e.getValue().status = d.getStatus();
	              if (d.getStatus() == MetaStoreConst.MDeviceStatus.OFFLINE ||
	                  d.getStatus() == MetaStoreConst.MDeviceStatus.DISABLE) {
	                e.getValue().isOffline = true;
	              }
	            } catch (NoSuchObjectException e1) {
	              LOG.error(e1, e1);
	            } catch (MetaException e1) {
	              LOG.error(e1, e1);
	            }
	          }
	        }
	        free += e.getValue().free;
	        used += e.getValue().used;
	        switch(e.getValue().getType()){
	        case MetaStoreConst.MDeviceProp.RAM:
	          ramFree += e.getValue().free;
	          ramUsed += e.getValue().used;
	          break;
	        case MetaStoreConst.MDeviceProp.CACHE:
	          cacheFree += e.getValue().free;
	          cacheUsed += e.getValue().used;
	          break;
	        case MetaStoreConst.MDeviceProp.GENERAL:
	          generalFree += e.getValue().free;
	          generalUsed += e.getValue().used;
	          break;
	        case MetaStoreConst.MDeviceProp.MASS:
	          massFree += e.getValue().free;
	          massUsed += e.getValue().used;
	          break;
	        case MetaStoreConst.MDeviceProp.SHARED:
	          shareFree += e.getValue().free;
	          shareUsed += e.getValue().used;
	          break;
	        case -1:
	          break;
	        }
	        if (e.getValue().getType() >= 0) {
	          adevnr[e.getValue().getType()]++;
	        }
	        if (e.getValue().isOffline) {
	          offlinenr++;
	          offlinedevs.add(e.getValue().dev);
	        } else {
	          truefree += e.getValue().free;
	          truetotal += (e.getValue().free + e.getValue().used);
	          switch(e.getValue().getType()){
	          case MetaStoreConst.MDeviceProp.RAM:
	            ramTruefree += e.getValue().free;
	            ramTruetotal += (e.getValue().free + e.getValue().used);
	            break;
	          case MetaStoreConst.MDeviceProp.CACHE:
	            cacheTruefree += e.getValue().free;
	            cacheTruetotal += (e.getValue().free + e.getValue().used);
	            break;
	          case MetaStoreConst.MDeviceProp.GENERAL:
	            generalTruefree += e.getValue().free;
	            generalTruetotal += (e.getValue().free + e.getValue().used);
	            break;
	          case MetaStoreConst.MDeviceProp.MASS:
	            massTruefree += e.getValue().free;
	            massTruetotal += (e.getValue().free + e.getValue().used);
	            break;
	          case MetaStoreConst.MDeviceProp.SHARED:
	            shareTruefree += e.getValue().free;
	            shareTruetotal += (e.getValue().free + e.getValue().used);
	            break;
	          }
	        }
	      }
	    }

	    if (used + free > 0) {
        if (((double)truefree / (truetotal)) < 0.2) {
          color = ANSI_RED;
        } else {
          color = ANSI_GREEN;
        }
        r += "Total space " + ((used + free) / 1000000000) + "G, used " + (used / 1000000000) +
            "G, free " + color + (free / 1000000000) + ANSI_RESET + "G, ratio " + ((double)free / (used + free)) + " \n";
        r += "True  space " + ((truetotal) / 1000000000) + "G, used " + ((truetotal - truefree) / 1000000000) +
            "G, free " + color + (truefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)truefree / (truetotal)) + " \n";

        r += "L0 Total space " + ((ramUsed + ramFree) / 1000000000) + "G, used " + (ramUsed / 1000000000) +
            "G, free " + color + (ramFree / 1000000000) + ANSI_RESET + "G, ratio " +  ((double)ramFree / (ramUsed + ramFree)) + " \n";
        r += "L0 True  space " + ((ramTruetotal) / 1000000000) + "G, used " + ((ramTruetotal - ramTruefree) / 1000000000) +
            "G, free " + color + (ramTruefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)ramTruefree / (ramTruetotal)) + " \n";

        r += "L1 Total space " + ((cacheUsed + cacheFree) / 1000000000) + "G, used " + (cacheUsed / 1000000000) +
            "G, free " + color + (cacheFree / 1000000000) + ANSI_RESET + "G, ratio " +  ((double)cacheFree / (cacheUsed + cacheFree)) + " \n";
        r += "L1 True  space " + ((cacheTruetotal) / 1000000000) + "G, used " + ((cacheTruetotal - cacheTruefree) / 1000000000) +
            "G, free " + color + (cacheTruefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)cacheTruefree / (cacheTruetotal)) + " \n";

        r += "L2 Total space " + ((generalUsed + generalFree) / 1000000000) + "G, used " + (generalUsed / 1000000000) +
            "G, free " + color + (generalFree / 1000000000) + ANSI_RESET + "G, ratio " +  ((double)generalFree / (generalUsed + generalFree)) + " \n";
        r += "L2 True  space " + ((generalTruetotal) / 1000000000) + "G, used " + ((generalTruetotal - generalTruefree) / 1000000000) +
            "G, free " + color + (generalTruefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)generalTruefree / (generalTruetotal)) + " \n";

        r += "L3 Total space " + ((massUsed + massFree) / 1000000000) + "G, used " + (massUsed / 1000000000) +
            "G, free " + color + (massFree / 1000000000) + ANSI_RESET + "G, ratio " +  ((double)massFree / (massUsed + massFree)) + " \n";
        r += "L3 True  space " + ((massTruetotal) / 1000000000) + "G, used " + ((massTruetotal - massTruefree) / 1000000000) +
            "G, free " + color + (massTruefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)massTruefree / (massTruetotal)) + " \n";

        r += "L4 Total space " + ((shareUsed + shareFree) / 1000000000) + "G, used " + (shareUsed / 1000000000) +
            "G, free " + color + (shareFree / 1000000000) + ANSI_RESET + "G, ratio " +  ((double)shareFree / (shareUsed + shareFree)) + " \n";
        r += "L4 True  space " + ((shareTruetotal) / 1000000000) + "G, used " + ((shareTruetotal - shareTruefree) / 1000000000) +
            "G, free " + color + (shareTruefree / 1000000000) + ANSI_RESET + "G, ratio " + ((double)shareTruefree / (shareTruetotal)) + " \n";
	    }
      synchronized (rs) {
        r += "Total devices " + rs.countDevice() +
            ", active {offline " + offlinenr +
            ", L0 " + devnr[MetaStoreConst.MDeviceProp.RAM] +
            ", L1 " + devnr[MetaStoreConst.MDeviceProp.CACHE] +
            ", L2 " + devnr[MetaStoreConst.MDeviceProp.GENERAL] +
            ", L3 " + devnr[MetaStoreConst.MDeviceProp.MASS] +
            ", backup " + devnr[MetaStoreConst.MDeviceProp.BACKUP] + " on " +
              adevnr[MetaStoreConst.MDeviceProp.BACKUP] +
            ", shared " + devnr[MetaStoreConst.MDeviceProp.SHARED] + " on " +
              adevnr[MetaStoreConst.MDeviceProp.SHARED] +
            ", backup_alone " + devnr[MetaStoreConst.MDeviceProp.BACKUP_ALONE] +
            "}.\n";
      }
      r += "Inactive nodes list: {\n";
      synchronized (rs) {
        List<Node> lns = rs.getAllNodes();
        for (Node n : lns) {
          if (!ndmap.containsKey(n.getNode_name())) {
            r += "\t" + n.getNode_name() + ", " + n.getIps().toString() + "\n";
          }
        }
      }
      r += "}\n";
      r += "Offline Device list: {\n";
      for (String dev : offlinedevs) {
        r += dev + "\n";
      }
      r += "}\n";
      r += "toReRep Device list: {\n";
      synchronized (toReRep) {
        for (String dev : toReRep.keySet()) {
          r += "\t" + dev + "\n";
        }
      }
      r += "}\n";
      r += "toUnspc Device list: {\n";
      synchronized (toUnspc) {
        for (String dev : toUnspc.keySet()) {
          r += "\t" + dev + "\n";
        }
      }
      r += "}\n";

      r += "backupQ: {\n";
      synchronized (backupQ) {
        for (BackupEntry be : backupQ) {
          r += "\t" + be.toString() + "\n";
        }
      }
      r += "}\n";

      r += "repQ: ";
      synchronized (repQ) {
        r += repQ.size() + "{\n";
        for (DMRequest req : repQ) {
          r += "\t" + req.toString() + "\n";
        }
      }
      r += "}\n";

      r += "cleanQ: ";
      synchronized (cleanQ) {
        r += cleanQ.size() + "{\n";
        for (DMRequest req : cleanQ) {
          r += "\t" + req.toString() + "\n";
        }
      }
      r += "}\n";

      r += "RRQ: ";
      synchronized (rrq) {
        r += rrq.size() + "{\n";
        for (ReplicateRequest req : rrq) {
          r += "\t" + req.toString() + "\n";
        }
      }
      r += "}\n";

      r += "RRMAP: {\n";
      synchronized (rrmap) {
        for (Map.Entry<String, MigrateEntry> e : rrmap.entrySet()) {
          r += "\t" + e.getKey() + " -> " + e.getValue().toString();
        }
      }
      r += "}\n";
      r += flselector.printWatched();
      r += "MsgLocalQ : " + MsgServer.getLocalQueueSize() + "\n";
      r += "MsgQ      : " + MsgServer.getQueueSize() + "\n";
      r += "MsgFailedQ: " + MsgServer.getFailedQueueSize() + "\n";
      r += "IncRepQ   : " + incRepFiles.size() + "\n";
      r += "Rep Limit: closeRepLimit " + closeRepLimit.get() + ", fixRepLimit " + fixRepLimit.get() + "\n";

      dmsnr++;
      return r;
    }

    public Set<DeviceInfo> maskActiveDevice(Set<DeviceInfo> toMask) {
      Set<DeviceInfo> masked = new TreeSet<DeviceInfo>();

      for (DeviceInfo odi : toMask) {
        boolean found = false;

        for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
          NodeInfo ni = e.getValue();
          if (ni.dis != null && ni.dis.size() > 0) {
            for (DeviceInfo di : ni.dis) {
              if (odi.dev.equalsIgnoreCase(di.dev)) {
                // this means the device is active!
                found = true;
                break;
              }
            }
          }
        }
        if (!found) {
          masked.add(odi);
        }
      }
      return masked;
    }

    public DeviceInfo getDeviceInfo(String devid) {
      return admap.get(devid);
    }

    public void removeFromADMap(String devid, String node) {
      admap.remove(devid);
      // remove it from device-node map
      removeFromDNMap(devid, node);
    }

    // Return old devs
    public NodeInfo addToNDMap(long ts, Node node, List<DeviceInfo> ndi) {
      NodeInfo ni = ndmap.get(node.getNode_name());

      // FIXME: do NOT update unless CUR - lastRptTs > 1000
      if (ni != null && ((ts - ni.lastRptTs) < 1000)) {
        return ni;
      }
      // flush to database
      if (ndi != null) {
        for (DeviceInfo di : ndi) {
          try {
            synchronized (rs_s1) {
              // automatically create device here!
              // NOTE-XXX: Every 30+ seconds, we have to get up to 280 devices.
              Device d = null;
              try {
                 d = rs_s1.getDevice(di.dev);
              } catch (NoSuchObjectException e) {
              }
              if (d == null ||
                  (d.getStatus() == MetaStoreConst.MDeviceStatus.SUSPECT && di.mp != null) ||
                  (!d.getNode_name().equals(node.getNode_name()) && DeviceInfo.getType(d.getProp()) != MetaStoreConst.MDeviceProp.SHARED)) {
                rs_s1.createOrUpdateDevice(di, node, null);
                d = rs_s1.getDevice(di.dev);
              }
              di.prop = d.getProp();
              di.status = d.getStatus();
              if (d.getStatus() == MetaStoreConst.MDeviceStatus.OFFLINE ||
                  d.getStatus() == MetaStoreConst.MDeviceStatus.DISABLE) {
                di.isOffline = true;
              }
              // FIXME: set quota here!
              if (hiveConf.getBoolVar(HiveConf.ConfVars.DM_USE_QUOTA)) {
                di.free = Math.min(di.free, (di.free + di.used) * di.getQuota() / 100);
              }
            }
          } catch (InvalidObjectException e) {
            LOG.error(e, e);
          } catch (MetaException e) {
            LOG.error(e, e);
          } catch (NoSuchObjectException e) {
            LOG.error(e, e);
          }
          // ok, update it to admap and dnmap
          admap.put(di.dev, di);
          addToDNMap(di.dev, node.getNode_name());
        }
      }

      if (ni == null) {
        ni = new NodeInfo(ndi);
        ni = ndmap.put(node.getNode_name(), ni);
        // clean entry in inactiveNodes map
        if (inactiveNodes.remove(node.getNode_name()) != null) {
          LOG.info("Remove Node " + node.getNode_name() + " from inactiveNodes map.");
        }
      } else {
        Set<DeviceInfo> old, cur;
        old = new TreeSet<DeviceInfo>();
        cur = new TreeSet<DeviceInfo>();

        synchronized (ni) {
          ni.lastRptTs = ts;
          if (ni.dis != null) {
            for (DeviceInfo di : ni.dis) {
              old.add(di);
            }
          }
          if (ndi != null) {
            for (DeviceInfo di : ndi) {
              cur.add(di);
            }
          }
          ni.dis = ndi;
        }
        // check if we lost some devices
        if (cur.containsAll(old)) {
          // old is subset of cur => add in some devices, it is OK.
          cur.removeAll(old);
          old.clear();
        } else if (old.containsAll(cur)) {
          // cur is subset of old => delete some devices, check if we can do some re-replicate?
          old.removeAll(cur);
          cur.clear();
        } else {
          // neither
          Set<DeviceInfo> inter = new TreeSet<DeviceInfo>();
          inter.addAll(old);
          inter.retainAll(cur);
          old.removeAll(cur);
          cur.removeAll(inter);
        }
        // fitler active device on other node, for example, the nas device.
        old = maskActiveDevice(old);

        for (DeviceInfo di : old) {
          LOG.info("Queue Device " + di.dev + " on toReRep set by report diff.");
          synchronized (toReRep) {
            if (!toReRep.containsKey(di.dev)) {
              toReRep.putIfAbsent(di.dev, System.currentTimeMillis());
              removeFromADMap(di.dev, node.getNode_name());
              toUnspc.remove(di.dev);
            }
          }
        }
        for (DeviceInfo di : cur) {
          synchronized (toReRep) {
            if (toReRep.containsKey(di.dev)) {
              LOG.info("Devcie " + di.dev + " is back, do not make SFL SUSPECT!");
              toReRep.remove(di.dev);
            }
          }
          LOG.info("Queue Device " + di.dev + " on toUnspc set.");
          synchronized (toUnspc) {
            if (!toUnspc.containsKey(di.dev)) {
              toUnspc.put(di.dev, System.currentTimeMillis());
            }
          }
        }
      }

      // check if we can leave safe mode
      if (safeMode) {
        try {
          long cn;
          synchronized (rs_s1) {
            cn = rs_s1.countNode();
          }
          if (safeMode && ((double) ndmap.size() / (double) cn > 0)) {

            LOG.info("Nodemap size: " + ndmap.size() + ", saved size: " + cn + ", reach "
                +
                (double) ndmap.size() / (double) cn * 100 + "%, leave SafeMode.");
            safeMode = false;
          }
        } catch (MetaException e) {
          LOG.error(e, e);
        }
      }

      return ni;
    }

    public NodeInfo removeFromNDMapWTO(String node, long cts) {
      NodeInfo ni = ndmap.get(node);

      if (ni.lastRptTs + dmtt.timeout < cts) {
        if ((ni.toDelete.size() == 0 && ni.toRep.size() == 0) || (cts - ni.lastRptTs > 1800000)) {
          ni = ndmap.remove(node);
          if (ni.toDelete.size() > 0 || ni.toRep.size() > 0) {
            LOG.error("Might miss entries here ... toDelete {" + ni.toDelete.toString() + "}, toRep {" +
                ni.toRep.toString() + "}, toVYR {" +
                ni.toVerify.toString() + "}, toCheck {" +
                ni.toCheck.toString() + "}.");
          }
          // update Node status here
          try {
            synchronized (rs) {
              Node saved = rs.getNode(node);
              saved.setStatus(MetaStoreConst.MNodeStatus.SUSPECT);
              rs.updateNode(saved);
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }
        } else {
          LOG.warn("Inactive node " + node + " with pending operations: toDelete " + ni.toDelete.size() + ", toRep " +
              ni.toRep.size() + ", toVerify " + ni.toVerify.size() + ", toCheck " + ni.toCheck.size());
        }
      }
      try {
        synchronized (rs) {
          if ((double)ndmap.size() / (double)rs.countNode() < 0.1) {
            safeMode = true;
            LOG.info("Lost too many Nodes(<10%), enter into SafeMode now.");
          }
        }
      } catch (MetaException e) {
        LOG.error(e, e);
      }
      return ni;
    }

    public void SafeModeStateChange() {
      try {
        synchronized (rs) {
          if ((double)ndmap.size() / (double)rs.countNode() < 0.1) {
            safeMode = true;
            LOG.info("Lost too many Nodes(<10%), enter into SafeMode now.");
          }
        }
      } catch (MetaException e) {
        LOG.error(e,e);
      }
    }

    public boolean isSDOnNode(String devid, String nodeName) {
      NodeInfo ni = ndmap.get(nodeName);
      if (ni != null) {
        synchronized (ni) {
          if (ni.dis != null) {
            for (DeviceInfo di : ni.dis) {
              if (di.dev.equalsIgnoreCase(devid)) {
                return true;
              }
            }
          }
        }
      }
      return false;
    }

    public boolean isNodeHasSD(String nodeName) {
      NodeInfo ni = ndmap.get(nodeName);
      if (ni != null) {
        synchronized (ni) {
          if (ni.dis != null) {
            for (DeviceInfo di : ni.dis) {
              if (di.getType() == MetaStoreConst.MDeviceProp.SHARED) {
                return true;
              }
            }
          }
        }
      }
      return false;
    }

    public boolean isSharedDevice(String devid) throws MetaException, NoSuchObjectException {
      DeviceInfo di = admap.get(devid);
      if (di != null) {
        return (di.getType() == MetaStoreConst.MDeviceProp.SHARED ||
            di.getType() == MetaStoreConst.MDeviceProp.BACKUP);
      } else {
        synchronized (rs) {
          Device d = rs.getDevice(devid);
          if (DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.SHARED ||
              DeviceInfo.getType(d.getProp()) == MetaStoreConst.MDeviceProp.BACKUP) {
            return true;
          } else {
            return false;
          }
        }
      }
    }

    public List<Node> findBestNodes(Set<String> fromSet, int nr) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      if (nr <= 0) {
        return new ArrayList<Node>();
      }
      List<Node> r = new ArrayList<Node>(nr);
      SortedMap<Long, String> m = new TreeMap<Long, String>();

      for (String node : fromSet) {
        NodeInfo ni = ndmap.get(node);
        if (ni == null) {
          continue;
        }
        synchronized (ni) {
          List<DeviceInfo> dis = filterTypeDevice(null, ni.dis);
          long thisfree = 0;

          if (dis == null) {
            continue;
          }
          for (DeviceInfo di : dis) {
            thisfree += di.free;
          }
          if (thisfree > 0) {
            m.put(thisfree, node);
          }
        }
      }

      int i = 0;
      for (Map.Entry<Long, String> entry : m.entrySet()) {
        if (i >= nr) {
          break;
        }
        synchronized (rs) {
          try {
            Node n = rs.getNode(entry.getValue());
            if (n != null) {
              r.add(n);
              i++;
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }

        }
      }
      return r;
    }

    public List<Node> findBestNodes(int nr) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      if (nr <= 0) {
        return new ArrayList<Node>();
      }
      List<Node> r = new ArrayList<Node>(nr);
      SortedMap<Long, String> m = new TreeMap<Long, String>();

      for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
        NodeInfo ni = entry.getValue();
        synchronized (ni) {
          List<DeviceInfo> dis = filterTypeDevice(null, ni.dis);
          long thisfree = 0;

          if (dis == null) {
            continue;
          }
          for (DeviceInfo di : dis) {
            thisfree += di.free;
          }
          if (thisfree > 0) {
            m.put(thisfree, entry.getKey());
          }
        }
      }

      int i = 0;
      for (Map.Entry<Long, String> entry : m.entrySet()) {
        if (i >= nr) {
          break;
        }
        synchronized (rs) {
          try {
            Node n = rs.getNode(entry.getValue());
            if (n != null) {
              r.add(n);
              i++;
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }

        }
      }
      return r;
    }

    public List<Node> findBestNodesBySingleDev(int nr) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      if (nr <= 0) {
        return new ArrayList<Node>();
      }
      List<Node> r = new ArrayList<Node>(nr);
      SortedMap<Long, String> m = new TreeMap<Long, String>();
      HashSet<String> rset = new HashSet<String>();

      for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
        NodeInfo ni = entry.getValue();
        synchronized (ni) {
          List<DeviceInfo> dis = filterTypeDevice(null, ni.dis);

          if (dis == null) {
            continue;
          }
          for (DeviceInfo di : dis) {
            if (di.free > 0) {
              m.put(di.free, entry.getKey());
            }
          }
        }
      }

      int i = 0;
      for (Map.Entry<Long, String> entry : m.entrySet()) {
        if (i >= nr) {
          break;
        }
        synchronized (rs) {
          try {
            Node n = rs.getNode(entry.getValue());
            if (n != null && !rset.contains(n.getNode_name())) {
              r.add(n);
              rset.add(n.getNode_name());
              i++;
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }
        }
      }
      return r;
    }

    public String getMP(String node_name, String devid) throws MetaException {
      String mp;
      if (node_name == null || node_name.equals("")) {
        node_name = getAnyNode(devid);
      } else if (node_name.contains(";")) {
        List<String> nodes = getNodesByDevice(devid);
        if (nodes.size() > 0) {
          Random r = new Random();
          node_name = (String) nodes.toArray()[r.nextInt(nodes.size())];
        } else {
          throw new MetaException("Can't find DEV '" + devid + "' in Node '" + node_name + "'.");
        }
      }
      NodeInfo ni = ndmap.get(node_name);
      if (ni == null) {
        throw new MetaException("Can't find Node '" + node_name + "' in ndmap.");
      }
      synchronized (ni) {
        mp = ni.getMP(devid);
        if (mp == null) {
          throw new MetaException("Can't find DEV '" + devid + "' in Node '" + node_name + "'.");
        }
      }
      return mp;
    }

    public void updateDeviceTracking(List<DMReply> dmrs) {
      if (dmrs != null) {
        for (DMReply dmr : dmrs) {
          if (dmr.type == DMReply.DMReplyType.TRACK_DEV) {
            if (dmr.args != null) {
              String[] fs = dmr.args.split(",");
              if (fs.length == 7) {
                // +B:dev,rep_nr,rep_err,rep_lat,del_nr,del_err,del_lat
                DeviceTrack dt = deviceTracked.get(fs[0]);
                DeviceTrack ndt = new DeviceTrack(fs[0]);

                try {
                  ndt.rep_nr = Long.parseLong(fs[1]);
                } catch (NumberFormatException nfe) {}
                try {
                  ndt.rep_err = Long.parseLong(fs[2]);
                } catch (NumberFormatException nfe) {}
                try {
                  ndt.rep_lat = Long.parseLong(fs[3]);
                } catch (NumberFormatException nfe) {}
                try {
                  ndt.del_nr = Long.parseLong(fs[4]);
                } catch (NumberFormatException nfe) {}
                try {
                  ndt.del_err = Long.parseLong(fs[5]);
                } catch (NumberFormatException nfe) {}
                try {
                  ndt.del_lat = Long.parseLong(fs[6]);
                } catch (NumberFormatException nfe) {}

                if (dt == null) {
                  deviceTracked.put(fs[0], ndt);
                  LOG.info("Add device " + fs[0] + " to track list");
                } else {
                  if (ndt.rep_nr < dt.rep_nr) {
                    LOG.info("Detect device " + fs[0] + " re-join to track list");
                  }
                  if (ndt.rep_err > dt.rep_err) {
                    LOG.info("Detect device " + fs[0] + " generate REP errors: " +
                        (ndt.rep_err - dt.rep_err));
                  }
                  if (ndt.rep_nr > 0 && dt.rep_nr > 0 &&
                      (ndt.rep_lat / ndt.rep_nr > 2 * dt.rep_lat / dt.rep_nr)) {
                    LOG.info("Detect device " + fs[0] + " induce REP latency: " +
                        (ndt.rep_lat / ndt.rep_nr));
                  }
                  if (ndt.del_err > dt.del_err) {
                    LOG.info("Detect device " + fs[0] + " generate DEL errors: " +
                        (ndt.del_err - dt.del_err));
                  }
                  if (ndt.del_nr > 0 && dt.del_nr > 0 &&
                    (ndt.del_lat / ndt.del_nr > 2 * dt.del_lat / dt.del_nr)) {
                    LOG.info("Detect device " + fs[0] + " induce DEL latency: " +
                        (ndt.del_lat / ndt.del_nr));
                  }
                  dt.rep_nr = ndt.rep_nr;
                  dt.rep_err = ndt.rep_err;
                  dt.rep_lat = ndt.rep_lat;
                  dt.del_nr = ndt.del_nr;
                  dt.del_err = ndt.del_err;
                  dt.del_lat = ndt.del_lat;
                }
              }
            }
          }
        }
      }
    }

    public String getTrackedDevice() {
      String r = "";

      for (Map.Entry<String, DeviceTrack> e : deviceTracked.entrySet()) {
        r += e.getValue() + "\n";
      }

      return r;
    }

    static public class FileLocatingPolicy {
      public static final int SPECIFY_NODES = 0;
      public static final int EXCLUDE_NODES = 1;
      public static final int SPECIFY_DEVS = 2;
      public static final int EXCLUDE_DEVS = 3;
      public static final int EXCLUDE_DEVS_SHARED = 4;
      public static final int RANDOM_NODES = 5;
      public static final int RANDOM_DEVS = 6;
      public static final int EXCLUDE_DEVS_AND_RANDOM = 7;
      public static final int EXCLUDE_NODES_AND_RANDOM = 8;
      public static final int ORDERED_ALLOC = 9;  // active both on node and dev mode

      Set<String> nodes;
      Set<String> devs;
      public int node_mode;
      public int dev_mode;
      public List<Integer> accept_types; // try to alloc in list order
      boolean canIgnore;

      public String origNode;

      public long length_hint;

      public FileLocatingPolicy(Set<String> nodes, Set<String> devs, int node_mode, int dev_mode, boolean canIgnore) {
        this.nodes = nodes;
        this.devs = devs;
        this.node_mode = node_mode;
        this.dev_mode = dev_mode;
        this.canIgnore = canIgnore;
        this.origNode = null;
        this.accept_types = null;
        this.length_hint = 0;
      }

      @Override
      public String toString() {
        return "Node {" + (nodes == null ? "null" : nodes.toString()) + "} -> {" +
            node_mode + "}, Dev {" +
            (devs == null ? "null" : devs.toString()) + "} -> {" +
            dev_mode + "}, accept_types {" + accept_types +
            "}, canIgnore " + canIgnore + ", origNode " +
            (origNode == null ? "null" : origNode) + ".";
      }
    }

    private boolean canFindDevices(NodeInfo ni, Set<String> devs) {
      boolean canFind = false;

      if (devs == null || devs.size() == 0) {
        return true;
      }
      if (ni != null) {
        List<DeviceInfo> dis = ni.dis;
        if (dis != null) {
          for (DeviceInfo di : dis) {
            if (!devs.contains(di.dev)) {
              canFind = true;
              break;
            }
          }
        }
      }

      return canFind;
    }

    public Set<String> filterSharedDevs(List<DeviceInfo> devs) {
      Set<String> r = new TreeSet<String>();

      for (DeviceInfo di : devs) {
        if (di.getType() == MetaStoreConst.MDeviceProp.SHARED ||
            di.getType() == MetaStoreConst.MDeviceProp.BACKUP) {
          r.add(di.dev);
        }
      }
      return r;
    }

    // findBestNode()
    //
    // return largest free node based on FLP.
    public String findBestNode(FileLocatingPolicy flp) throws IOException {
      boolean isExclude = true;

      if (flp == null) {
        return findBestNode_L123(false);
      }
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      switch (flp.node_mode) {
      case FileLocatingPolicy.ORDERED_ALLOC:
      {
        TreeMap<String, Long> candidate = new TreeMap<String, Long>();

        for (Integer dtype : flp.accept_types) {
          candidate.clear();
          for (Map.Entry<String, NodeInfo> e : ndmap.entrySet()) {
            NodeInfo ni = e.getValue();
            if (ni.dis != null && ni.dis.size() > 0) {
              for (DeviceInfo di : ni.dis) {
                if (di.getType() == dtype && !di.isOffline) {
                  // ok, this node is a candidate
                  Long oldfree = candidate.get(e.getKey());
                  if (oldfree == null) {
                    candidate.put(e.getKey(), di.free);
                  } else {
                    candidate.put(e.getKey(), oldfree + di.free);
                  }
                }
              }
            }
          }
          // find the LARGEST max(10 or 20%NODES) nodes, and random select one
          if (candidate.size() == 0) {
            continue;
          } else {
            Random r = new Random();
            __trim_candidate(candidate, Math.max(10, ndmap.size() / 5));
            String[] a = candidate.keySet().toArray(new String[0]);
            if (a.length > 0) {
              return a[r.nextInt(a.length)];
            } else {
              continue;
            }
          }
        }
        return null;
      }
      case FileLocatingPolicy.EXCLUDE_NODES:
        if (flp.nodes == null || flp.nodes.size() == 0) {
          return findBestNode_L123(false);
        }
        break;
      case FileLocatingPolicy.SPECIFY_NODES:
        if (flp.nodes == null || flp.nodes.size() == 0) {
          return null;
        }
        isExclude = false;
        break;
      case FileLocatingPolicy.RANDOM_NODES:
        // random select a node in specify nodes
        if (flp.nodes == null || flp.nodes.size() == 0) {
          return null;
        } else {
          Random r = new Random();
          String[] a = flp.nodes.toArray(new String[0]);
          if (flp.nodes.size() > 0) {
            return a[r.nextInt(flp.nodes.size())];
          } else {
            return null;
          }
        }
      case FileLocatingPolicy.EXCLUDE_NODES_AND_RANDOM:
        // random select one node from largest node set or specified type list
        Random r = new Random();
        List<String> nodes = new ArrayList<String>();
        __select_some_nodes(nodes, Math.min(ndmap.size() / 5 + 1, 3),
            flp.nodes, flp.accept_types);
        LOG.debug("Random select in nodes: " + nodes + ", excl: " + flp.nodes);
        if (nodes.size() > 0) {
          return nodes.get(r.nextInt(nodes.size()));
        } else {
          return null;
        }
      }

      long largest = 0;
      String largestNode = null;

      for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
        NodeInfo ni = entry.getValue();
        synchronized (ni) {
          List<DeviceInfo> dis = ni.dis;
          long thisfree = 0;
          boolean ignore = false;

          if (isExclude) {
            if (flp.nodes.contains(entry.getKey())) {
              ignore = true;
            }
            Set<String> excludeDevs = new TreeSet<String>();
            if (flp.devs != null) {
              excludeDevs.addAll(flp.devs);
            }
            if (flp.dev_mode == FileLocatingPolicy.EXCLUDE_DEVS_SHARED ||
                flp.dev_mode == FileLocatingPolicy.RANDOM_DEVS) {
              excludeDevs.addAll(filterSharedDevs(dis));
            }
            if (!canFindDevices(ni, excludeDevs)) {
              ignore = true;
            }
          } else {
            if (!flp.nodes.contains(entry.getKey())) {
              ignore = true;
            }
          }
          if (ignore || dis == null) {
            continue;
          }
          for (DeviceInfo di : dis) {
            thisfree += di.free;
          }
          if (thisfree > largest) {
            largestNode = entry.getKey();
            largest = thisfree;
          }
        }
      }
      if (largestNode == null && flp.canIgnore) {
        // FIXME: replicas ignore NODE GROUP settings?
        return findBestNode_L123(false);
      }

      return largestNode;
    }

    // findBestNode_L123()
    //
    // return largest free nodes(count for L1,L2,L3(if false) devices).
    public String findBestNode_L123(boolean ignoreShared) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      long largest = 0;
      String largestNode = null;

      for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
        NodeInfo ni = entry.getValue();
        synchronized (ni) {
          List<DeviceInfo> dis = ni.dis;
          long thisfree = 0;

          if (dis == null) {
            continue;
          }
          for (DeviceInfo di : dis) {
            // Do not calculate cache device's space
            if (di.getType() == MetaStoreConst.MDeviceProp.CACHE) {
              continue;
            }
            if (!(ignoreShared && (di.getType() == MetaStoreConst.MDeviceProp.SHARED || di.getType() == MetaStoreConst.MDeviceProp.BACKUP))) {
              thisfree += di.free;
            }
          }
          if (thisfree > largest) {
            largestNode = entry.getKey();
            largest = thisfree;
          }
        }
      }

      return largestNode;
    }

    // filter offline device either
    private List<DeviceInfo> filterOfflineDevice(String node_name, List<DeviceInfo> orig) {
      List<DeviceInfo> r = new ArrayList<DeviceInfo>();

      if (orig != null) {
        for (DeviceInfo di : orig) {
          if (di.isOffline) {
            continue;
          } else {
            r.add(di);
          }
        }
      }

      return r;
    }

    private List<DeviceInfo> generalDeviceFilter(List<DeviceInfo> orig, Set<Integer> accept_types, boolean filter_offline) {
      List<DeviceInfo> r = new ArrayList<DeviceInfo>();

      if (orig != null) {
        for (DeviceInfo di : orig) {
          if ((filter_offline && di.isOffline) || di.free < 1024 * 1024) {
            continue;
          }
          if (accept_types.contains(di.getType())) {
            r.add(di);
          }
        }
      }
      return r;
    }

    // filter backup device and offline device both, and cache device either
    private List<DeviceInfo> filterTypeDevice(List<Integer> accept_types_list,
        List<DeviceInfo> orig) {
      Set<Integer> accept_types = new HashSet<Integer>();
      if (accept_types_list == null || accept_types_list.size() == 0) {
        accept_types.add(MetaStoreConst.MDeviceProp.GENERAL);
      } else {
        accept_types.addAll(accept_types_list);
      }
      return generalDeviceFilter(orig, accept_types, true);
    }

    public Set<String> findSharedDevs() {
      Set<Integer> accept_types = new HashSet<Integer>();
      Set<String> r = new HashSet<String>();

      accept_types.add(MetaStoreConst.MDeviceProp.SHARED);
      accept_types.add(MetaStoreConst.MDeviceProp.BACKUP);

      for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
        NodeInfo ni = entry.getValue();
        synchronized (ni) {
          List<DeviceInfo> dis = ni.dis;

          if (ni.dis == null) {
            continue;
          }
          dis = generalDeviceFilter(ni.dis, accept_types, false);
          if (dis == null || dis.size() == 0) {
            continue;
          }
          for (DeviceInfo di : dis) {
            r.add(di.dev);
          }
        }
      }
      return r;
    }

    public List<DeviceInfo> findDevices(String node) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      NodeInfo ni = ndmap.get(node);
      if (ni == null) {
        return null;
      } else {
        return ni.dis;
      }
    }

    // BUG-XXX: we have to preserve the accept types order here!
    //
    // Try to select in higher level order
    private void __select_some_nodes(List<String> nodes, int nr, Set<String> excl,
        List<Integer> accept_types) {
      if (nr <= 0) {
        return;
      }

      if (accept_types == null || accept_types.size() == 0) {
        accept_types.addAll(default_orders);
      }

      for (Integer dtype : accept_types) {
        List<Integer> stype = new ArrayList<Integer>();
        TreeMap<Long, String> m = new TreeMap<Long, String>();

        stype.add(dtype);

        for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
          if (excl != null && excl.contains(entry.getKey())) {
            continue;
          }

          NodeInfo ni = entry.getValue();
          synchronized (ni) {
            List<DeviceInfo> dis = filterTypeDevice(stype, ni.dis);
            long thisfree = 0;

            if (dis == null) {
              continue;
            }
            for (DeviceInfo di : dis) {
              thisfree += di.free;
            }
            if (thisfree > 0) {
              m.put(thisfree, entry.getKey());
            }
          }
        }
        int i = 0;
        for (Long key : m.descendingKeySet()) {
          if (i >= nr) {
            break;
          }
          // NOTE-XXX: if this node's load1 is too large, reject to select it
          NodeInfo ni = ndmap.get(m.get(key));
          if (ni != null) {
            if (ni.load1 < 50) {
              nodes.add(m.get(key));
              i++;
            }
          } else {
            nodes.add(m.get(key));
            i++;
          }
        }
        if (nodes.size() > 0) {
          break;
        }
      }
    }

    class TopKeySet {
      public LinkedList<KeySetEntry> ll;
      private final int k;

      public TopKeySet(int k) {
        ll = new LinkedList<KeySetEntry>();
        this.k = k;
      }

      class KeySetEntry {
        String key;
        Long dn;

        public KeySetEntry(String key, Long dn) {
          this.key = key;
          this.dn = dn;
        }
      }

      public void put(String key, Long value) {
        boolean isInserted = false;
        for (int i = 0; i < ll.size(); i++) {
          if (value.longValue() > ll.get(i).dn) {
            ll.add(i, new KeySetEntry(key,value));
            isInserted = true;
            break;
          }
        }
        if (ll.size() < k) {
          if (!isInserted) {
            ll.addLast(new KeySetEntry(key,value));
          }
        } else if (ll.size() > k) {
          ll.removeLast();
        }
      }
    }

    private void __trim_candidate(TreeMap<String, Long> cand, int nr) {
      if (cand.size() <= nr) {
        return;
      }
      TopKeySet topk = new TopKeySet(nr);

      for (Map.Entry<String, Long> e : cand.entrySet()) {
        topk.put(e.getKey(), e.getValue());
      }
      cand.clear();
      for (TopKeySet.KeySetEntry kse : topk.ll) {
        cand.put(kse.key, kse.dn);
      }
    }

    private void __trim_dilist(List<DeviceInfo> dilist, int nr, Set<String> excl) {
      if (dilist.size() <= nr) {
        return;
      }
      List<DeviceInfo> newlist = new ArrayList<DeviceInfo>();
      newlist.addAll(dilist);
      if (excl != null) {
        for (DeviceInfo di : dilist) {
          if (excl.contains(di.dev)) {
            newlist.remove(di);
          }
        }
      }
      dilist.clear();

      for (int i = 0; i < nr; i++) {
        long free = 0;
        DeviceInfo toDel = null;

        for (DeviceInfo di : newlist) {
          if (di.free > free) {
            free = di.free;
            toDel = di;
          }
        }
        if (toDel != null) {
          newlist.remove(toDel);
          dilist.add(toDel);
        }
      }
    }

    public String findBestDevice(String node, FileLocatingPolicy flp) throws IOException {
      if (safeMode) {
        throw new IOException("Disk Manager is in Safe Mode, waiting for disk reports ...\n");
      }
      NodeInfo ni = ndmap.get(node);
      if (ni == null) {
        throw new IOException("Node '" + node + "' does not exist in NDMap, are you sure node '" + node + "' belongs to this MetaStore?" + hiveConf.getVar(HiveConf.ConfVars.LOCAL_ATTRIBUTION) + "\n");
      }
      List<DeviceInfo> dilist = new ArrayList<DeviceInfo>();

      if (flp.dev_mode == FileLocatingPolicy.EXCLUDE_DEVS_SHARED ||
          flp.dev_mode == FileLocatingPolicy.RANDOM_DEVS) {
        // this two mode only used in selecting primary replica!
        synchronized (ni) {
          dilist = filterTypeDevice(flp.accept_types, ni.dis);
        }
      } else if (flp.dev_mode == FileLocatingPolicy.ORDERED_ALLOC) {
        if (ni.dis == null) {
          return null;
        }
        for (Integer dtype : flp.accept_types) {
          dilist.clear();
          for (DeviceInfo di : ni.dis) {
            if (di.getType() == dtype && !di.isOffline) {
              dilist.add(di);
            }
          }
          // find the LARGEST 5 devices in current type, and random select one
          __trim_dilist(dilist, 5, null);
          Random r = new Random();
          if (dilist.size() > 0) {
            return dilist.get(r.nextInt(dilist.size())).dev;
          } else {
            continue;
          }
        }
        return null;
      } else {
        // ignore offline device
        dilist = filterOfflineDevice(node, ni.dis);
      }
      String bestDev = null;
      long free = 0;

      if (dilist == null) {
        return null;
      }
      for (DeviceInfo di : dilist) {
        boolean ignore = false;

        if (flp.dev_mode == FileLocatingPolicy.EXCLUDE_DEVS ||
            flp.dev_mode == FileLocatingPolicy.EXCLUDE_DEVS_SHARED) {
          if (flp.devs != null && flp.devs.contains(di.dev)) {
            ignore = true;
            continue;
          }
        } else if (flp.dev_mode == FileLocatingPolicy.SPECIFY_DEVS) {
          if (flp.devs != null && !flp.devs.contains(di.dev)) {
            ignore = true;
            continue;
          }
        } else if (flp.dev_mode == FileLocatingPolicy.RANDOM_DEVS) {
          // random select a device in specify dilist with at most 5 device?
          if (dilist == null) {
            return null;
          } else {
            Random r = new Random();
            __trim_dilist(dilist, 5, null);
            if (dilist.size() > 0) {
              return dilist.get(r.nextInt(dilist.size())).dev;
            } else {
              return null;
            }
          }
        } else if (flp.dev_mode == FileLocatingPolicy.EXCLUDE_DEVS_AND_RANDOM) {
          // random select a device (in some types) and exclude used devs
          if (dilist == null) {
            return null;
          } else {
            Random r = new Random();
            __trim_dilist(dilist, 5, flp.devs);
            if (dilist.size() > 0) {
              return dilist.get(r.nextInt(dilist.size())).dev;
            } else {
              return null;
            }
          }
        }
        if (!ignore && di.free > free) {
          bestDev = di.dev;
          free = di.free;
        }
      }
      if (bestDev == null && flp.canIgnore) {
        for (DeviceInfo di : dilist) {
          if (di.free > free) {
            bestDev = di.dev;
          }
        }
      }

      return bestDev;
    }

    public class DMCleanThread implements Runnable {
      Thread runner;
      public DMCleanThread(String threadName) {
        runner = new Thread(this, threadName);
        runner.start();
      }

      public void run() {
        while (true) {
        try {
          // dequeue requests from the clean queue
          DMRequest r = cleanQ.poll();
          if (r == null) {
            try {
              synchronized (cleanQ) {
                cleanQ.wait();
              }
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            continue;
          }
          if (r.op == DMRequest.DMROperation.RM_PHYSICAL) {
            synchronized (ndmap) {
              if (r.file != null && r.file.getLocations() != null) {
              for (SFileLocation loc : r.file.getLocations()) {
                NodeInfo ni = ndmap.get(loc.getNode_name());
                if (ni == null) {
                  // add back to cleanQ
                  // BUG-XXX: if this node has gone, we repeat delete the other exist LOC. So, ignore it?
                  /*synchronized (cleanQ) {
                    cleanQ.add(r);
                  }*/
                  LOG.warn("RM_PHYSICAL fid " + r.file.getFid() + " DEV " + loc.getDevid() + " LOC " +
                      loc.getLocation() + " failed, NODE " + loc.getNode_name());
                  break;
                }
                synchronized (ni.toDelete) {
                  ni.toDelete.add(loc);
                  LOG.info("----> Add to Node " + loc.getNode_name() + "'s toDelete " + loc.getLocation() + ", qs " + cleanQ.size() + ", " + r.file.getLocationsSize());
                }
              }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e, e);
        }
        }
      }
    }

    // this is user provided SFL, should check on node_name
    // Caller must make sure that this SFL will NEVER be used again (e.g. not used in reopen)
    public void asyncDelSFL(SFileLocation sfl) {
      synchronized (ndmap) {
        if (sfl.getNode_name().equals("")) {
          // this is a BACKUP/SHARE device;
          try {
            try {
              String any = getAnyNode(sfl.getDevid());
              sfl.setNode_name(any);
            } catch (MetaException e) {
              synchronized (rs) {
                Device d = rs.getDevice(sfl.getDevid());
                sfl.setNode_name(d.getNode_name());
              }
            }
          } catch (MetaException e) {
            LOG.error(e, e);
            return;
          } catch (NoSuchObjectException e) {
            LOG.error(e, e);
            return;
          }
        }
        NodeInfo ni = ndmap.get(sfl.getNode_name());
        if (ni != null) {
          synchronized (ni.toDelete) {
            ni.toDelete.add(sfl);
            LOG.info("----> Add toDelete " + sfl.getLocation() + ", qs " + cleanQ.size() + ", dev " + sfl.getDevid());
          }
        } else {
          LOG.warn("SFL " + sfl.getDevid() + ":" + sfl.getLocation() + " delete leak on node " + sfl.getNode_name());
        }
      }
    }

    public class DMRepThread implements Runnable {
      private RawStore rrs = null;
      Thread runner;

      public RawStore getRS() {
        if (rrs != null) {
          return rrs;
        } else {
          return rs;
        }
      }

      public void init(HiveConf conf) throws MetaException {
        if (rst == RsStatus.NEWMS) {
          try {
            this.rrs = new RawStoreImp();
          } catch (IOException e) {
            LOG.error(e, e);
            throw new MetaException(e.getMessage());
          }
        } else {
          String rawStoreClassName = hiveConf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL);
          Class<? extends RawStore> rawStoreClass = (Class<? extends RawStore>) MetaStoreUtils.getClass(
              rawStoreClassName);
          this.rrs = (RawStore) ReflectionUtils.newInstance(rawStoreClass, conf);
        }

      }

      public DMRepThread(String threadName) {
        try {
          init(hiveConf);
        } catch (MetaException e) {
          e.printStackTrace();
          rrs = null;
        }
        runner = new Thread(this, threadName);
        runner.start();
      }

      public void release_rep_limit() {
        closeRepLimit.incrementAndGet();
      }

      public void run() {
        while (true) {
          try {
          // check limiting
          do {
            if (closeRepLimit.decrementAndGet() < 0) {
              closeRepLimit.incrementAndGet();
              try {
                  Thread.sleep(1000);
              } catch (InterruptedException e) {
              }
            } else {
              break;
            }
          } while (true);

          // dequeue requests from the rep queue
          DMRequest r = repQ.poll();
          if (r == null) {
            try {
              synchronized (repQ) {
                repQ.wait();
              }
            } catch (InterruptedException e) {
              LOG.debug(e, e);
            }
            release_rep_limit();
            continue;
          }
          // BUG-XXX: we should wait a moment to passively update the state.
          if (r.op == DMRequest.DMROperation.REPLICATE) {
            FileLocatingPolicy flp, flp_default;
            Set<String> excludes = new TreeSet<String>();
            Set<String> excl_dev = new TreeSet<String>();
            Set<String> spec_dev = new TreeSet<String>();
            Set<String> spec_node = new TreeSet<String>();
            int master = 0;
            boolean master_marked = false;

            // BUG-XXX: if r.file.getStore_status() is REPLICATED, do NOT replicate
            // otherwise, closeRepLimit leaks
            if (r.file.getStore_status() == MetaStoreConst.MFileStoreStatus.REPLICATED ||
                r.begin_idx >= r.file.getRep_nr()) {
              release_rep_limit();
              continue;
            }

            // exclude old file locations
            for (int i = 0; i < r.begin_idx; i++) {
              try {
                if (!isSharedDevice(r.file.getLocations().get(i).getDevid())) {
                  excludes.add(r.file.getLocations().get(i).getNode_name());
                }
              } catch (MetaException e1) {
                LOG.error(e1, e1);
                excludes.add(r.file.getLocations().get(i).getNode_name());
              } catch (NoSuchObjectException e1) {
                LOG.error(e1, e1);
                excludes.add(r.file.getLocations().get(i).getNode_name());
              } catch (javax.jdo.JDOException e1) {
                LOG.error(e1, e1);
                excludes.add(r.file.getLocations().get(i).getNode_name());
              }
              excl_dev.add(r.file.getLocations().get(i).getDevid());

              // mark master copy id
              if (!master_marked && r.file.getLocations().get(i).getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                master = i;
                master_marked = true;
                // BUG: if the master replica is a SHARED/BACKUP device, get any node
                if (r.file.getLocations().get(i).getNode_name().equals("")) {
                  try {
                    r.file.getLocations().get(i).setNode_name(getAnyNode(r.file.getLocations().get(i).getDevid()));
                  } catch (MetaException e) {
                    LOG.error(e, e);
                    master_marked = false;
                  }
                }
              }
              // remove if this backup device has already used
              if (spec_dev.remove(r.file.getLocations().get(i).getDevid())) {
                // this backup device has already been used, do not user any other backup device
                spec_dev.clear();
              }
              // FIXME: remove if the node is not in spec_node
              if (!spec_node.contains(r.file.getLocations().get(i).getNode_name())) {
                // this file's node is not in any backup devices' active node set, so, do NOT use backup device
                spec_dev.clear();
              }
            }
            if (!master_marked) {
              LOG.error("No active master copy for file FID " + r.file.getFid() + ". BAD FAIL!");
              // release counter here!
              release_rep_limit();
              continue;
            }

            flp = flp_default = new FileLocatingPolicy(excludes, excl_dev,
                FileLocatingPolicy.EXCLUDE_NODES_AND_RANDOM,
                FileLocatingPolicy.EXCLUDE_DEVS_AND_RANDOM, true);
            flp.length_hint = r.file.getLength();
            flp_default.length_hint = r.file.getLength();

            for (int i = r.begin_idx; i < r.file.getRep_nr(); i++, flp = flp_default) {
              if (i == r.begin_idx) {
                // TODO: Use FLSelector here to decide whether we need replicate it to CACHE device
                String table = r.file.getDbName() + "." + r.file.getTableName();
                switch (flselector.FLSelector_switch(table)) {
                default:
                case NONE:
                  break;
                case FAIR_NODES:
                case ORDERED_ALLOC_DEVS:
                  flp.dev_mode = FileLocatingPolicy.ORDERED_ALLOC;
                  break;
                }
                flp.accept_types = flselector.getDevTypeListAfterIncludeHint(table,
                    MetaStoreConst.MDeviceProp.__AUTOSELECT_R2__);
              }
              try {
                String node_name = findBestNode(flp);
                if (node_name == null) {
                  LOG.warn("Could not find any best node to replicate file " + r.file.getFid());
                  r.failnr++;
                  r.begin_idx = i;
                  if (r.failnr <= 50) {
                    // insert back to the queue;
                    synchronized (repQ) {
                      repQ.add(r);
                      repQ.notify();
                    }
                  } else {
                    LOG.error("[FLP] Drop REP request: fid " + r.file.getFid() + ", faield " + r.failnr + ", " + flp);
                  }
                  try {
                    Thread.sleep(500);
                  } catch (InterruptedException el) {
                  }
                  release_rep_limit();
                  break;
                }
                // if we are selecting backup device, try to use local shortcut
                if (flp.origNode != null && flp.nodes.contains(flp.origNode)) {
                  node_name = flp.origNode;
                }
                excludes.add(node_name);
                String devid = findBestDevice(node_name, flp);
                if (devid == null) {
                  LOG.warn("Could not find any best device on node " + node_name + " to replicate file " + r.file.getFid());
                  r.begin_idx = i;
                  // insert back to the queue;
                  synchronized (repQ) {
                    repQ.add(r);
                  }
                  release_rep_limit();
                  break;
                }
                excl_dev.add(devid);
                // if we are selecting a shared device, try to use local shortcut
                try {
                  if (isSharedDevice(devid) && isSDOnNode(devid, r.file.getLocations().get(master).getNode_name())) {
                    node_name = r.file.getLocations().get(master).getNode_name();
                  }
                } catch (NoSuchObjectException e1) {
                  LOG.error(e1, e1);
                } catch (Exception e1) {
                  LOG.error(e1, e1);
                }

                String location;
                Random rand = new Random();
                SFileLocation nloc;

                do {
                  location = "/data/";
                  if (r.file.getDbName() != null && r.file.getTableName() != null) {
                    synchronized (getRS()) {
                      Table t = getRS().getTable(r.file.getDbName(), r.file.getTableName());
                      location += t.getDbName() + "/" + t.getTableName() + "/"
                          + rand.nextInt(Integer.MAX_VALUE);
                    }
                  } else {
                    location += "UNNAMED-DB/UNNAMED-TABLE/" + rand.nextInt(Integer.MAX_VALUE);
                  }
                  nloc = new SFileLocation(node_name, r.file.getFid(), devid, location,
                      i, System.currentTimeMillis(),
                      MetaStoreConst.MFileLocationVisitStatus.OFFLINE, "SFL_REP_DEFAULT");
                  synchronized (getRS()) {
                    // FIXME: check the file status now, we might conflict with REOPEN
                    if (getRS().createFileLocation(nloc)) {
                      break;
                    }
                  }
                } while (true);
                r.file.addToLocations(nloc);

                // indicate file transfer
                JSONObject jo = new JSONObject();
                try {
                  JSONObject j = new JSONObject();
                  NodeInfo ni = ndmap.get(r.file.getLocations().get(master).getNode_name());
                  String fromMp, toMp;

                  if (ni == null) {
                    if (nloc != null) {
                      getRS().delSFileLocation(nloc.getDevid(), nloc.getLocation());
                    }
                    throw new IOException("Can not find Node '" + r.file.getLocations().get(master).getNode_name() +
                        "' in nodemap now, is it offline? fid(" + r.file.getFid() + ")");
                  }
                  fromMp = ni.getMP(r.file.getLocations().get(master).getDevid());
                  if (fromMp == null) {
                    throw new IOException("Can not find Device '" + r.file.getLocations().get(master).getDevid() +
                        "' in NodeInfo '" + r.file.getLocations().get(master).getNode_name() + "', fid(" + r.file.getFid() + ")");
                  }
                  j.put("node_name", r.file.getLocations().get(master).getNode_name());
                  j.put("devid", r.file.getLocations().get(master).getDevid());
                  j.put("mp", fromMp);
                  j.put("location", r.file.getLocations().get(master).getLocation());
                  jo.put("from", j);

                  j = new JSONObject();
                  ni = ndmap.get(nloc.getNode_name());
                  if (ni == null) {
                    if (nloc != null) {
                      getRS().delSFileLocation(nloc.getDevid(), nloc.getLocation());
                    }
                    throw new IOException("Can not find Node '" + nloc.getNode_name() + "' in nodemap now, is it offline? fid("
                        + r.file.getFid() + ")");
                  }
                  toMp = ni.getMP(nloc.getDevid());
                  if (toMp == null) {
                    throw new IOException("Can not find Device '" + nloc.getDevid() +
                        "' in NodeInfo '" + nloc.getNode_name() + "', fid(" + r.file.getFid() + ")");
                  }
                  j.put("node_name", nloc.getNode_name());
                  j.put("devid", nloc.getDevid());
                  j.put("mp", toMp);
                  j.put("location", nloc.getLocation());
                  jo.put("to", j);
                } catch (JSONException e) {
                  LOG.error(e, e);
                  release_rep_limit();
                  continue;
                }
                synchronized (ndmap) {
                  NodeInfo ni = ndmap.get(node_name);
                  if (ni == null) {
                    LOG.error("Can not find Node '" + node_name + "' in nodemap now, is it offline?");
                    release_rep_limit();
                  } else {
                    synchronized (ni.toRep) {
                      ni.toRep.add(jo);
                      LOG.info("----> ADD to Node " + node_name + "'s toRep " + jo);
                    }
                  }
                }
              } catch (IOException e) {
                LOG.error(e, e);
                r.failnr++;
                r.begin_idx = i;
                if (r.failnr <= 50) {
                  // insert back to the queue;
                  synchronized (repQ) {
                    repQ.add(r);
                    repQ.notify();
                  }
                } else {
                  LOG.error("[DEV] Drop REP request: fid " + r.file.getFid() + ", failed " + r.failnr);
                }
                try {
                  Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
                release_rep_limit();
                break;
              } catch (MetaException e) {
                LOG.error(e, e);
                release_rep_limit();
              } catch (InvalidObjectException e) {
                LOG.error(e, e);
                release_rep_limit();
              }
            }
          } else if (r.op == DMRequest.DMROperation.MIGRATE) {
            SFileLocation source = null, target = null;

            // select a source node
            if (r.file == null || r.tfile == null) {
              LOG.error("Invalid DMRequest provided, NULL SFile!");
              release_rep_limit();
              continue;
            }
            if (r.file.getLocationsSize() > 0) {
              // select the 0th location
              source = r.file.getLocations().get(0);
            }
            // determine the target node
            if (r.tfile.getLocationsSize() > 0) {
              // select the 0th location
              target = r.tfile.getLocations().get(0);
            }
            // indicate file transfer
            JSONObject jo = new JSONObject();
            try {
              JSONObject j = new JSONObject();
              NodeInfo ni = ndmap.get(source.getNode_name());

              if (ni == null) {
                throw new IOException("Can not find Node '" + source.getNode_name() + "' in ndoemap now.");
              }
              j.put("node_name", source.getNode_name());
              j.put("devid", source.getDevid());
              j.put("mp", ni.getMP(source.getDevid()));
              j.put("location", source.getLocation());
              jo.put("from", j);

              j = new JSONObject();
              if (r.devmap.get(target.getDevid()) == null) {
                throw new IOException("Can not find DEV '" + target.getDevid() + "' in pre-generated devmap.");
              }
              j.put("node_name", target.getNode_name());
              j.put("devid", target.getDevid());
              j.put("mp", r.devmap.get(target.getDevid()));
              j.put("location", target.getLocation());
              jo.put("to", j);
            } catch (JSONException e) {
              LOG.error(e, e);
              release_rep_limit();
              continue;
            } catch (IOException e) {
              LOG.error(e, e);
              release_rep_limit();
              continue;
            }
            synchronized (ndmap) {
              NodeInfo ni = ndmap.get(source.getNode_name());
              if (ni == null) {
                LOG.error("Can not find Node '" + source.getNode_name() + "' in nodemap.");
                release_rep_limit();
              } else {
                synchronized (ni.toRep) {
                  ni.toRep.add(jo);
                  LOG.info("----> ADD toRep (by migrate)" + jo);
                }
              }
            }
          }
        } catch (Exception e) {
          LOG.error(e, e);
        }
        }
      }
    }

    public class DMCMDThread implements Runnable {
      Thread runner;
      public DMCMDThread(String threadName) {
        runner = new Thread(this, threadName);
        runner.start();
      }

      @Override
      public void run() {
        while (true) {
          try {
          Set<JSONObject> toRep = null;
          Set<SFileLocation> toDelete = null;
          Set<SFileLocation> toCheck = null;
          NodeInfo ni = null;

          // wait a moment
          long wait_interval = 10 * 1000;
          try {
            Thread.sleep(wait_interval);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
            wait_interval /= 2;
          }

          synchronized (ndmap) {
            for (Map.Entry<String, NodeInfo> entry : ndmap.entrySet()) {
              if (toDelete != null && entry.getValue().toDelete.size() > 0) {
                toDelete = entry.getValue().toDelete;
              }
              if (toRep != null && entry.getValue().toRep.size() > 0) {
                toRep = entry.getValue().toRep;
              }
              if (toCheck != null && entry.getValue().toCheck.size() > 0) {
                toCheck = entry.getValue().toCheck;
              }
              if (toDelete != null || toRep != null || toCheck != null) {
                ni = entry.getValue();
                break;
              }
            }
          }

          int nr = 0;
          int nr_max = hiveConf.getIntVar(HiveConf.ConfVars.DM_APPEND_CMD_MAX);
          StringBuffer sb;

          while (true) {
            sb = new StringBuffer();
            sb.append("+OK\n");

            if (toDelete != null) {
              synchronized (toDelete) {
                Set<SFileLocation> ls = new TreeSet<SFileLocation>();
                for (SFileLocation loc : toDelete) {
                  if (nr >= nr_max) {
                    break;
                  }
                  if (ni != null) {
                    sb.append("+DEL:");
                    sb.append(loc.getNode_name());
                    sb.append(":");
                    sb.append(loc.getDevid());
                    sb.append(":");
                    sb.append(ni.getMP(loc.getDevid()));
                    sb.append(":");
                    sb.append(loc.getLocation());
                    sb.append("\n");

                    ls.add(loc);
                    nr++;
                  }
                }
                for (SFileLocation l : ls) {
                  toDelete.remove(l);
                }
              }
            } else {
              toDelete = new TreeSet<SFileLocation>();
            }

            if (toRep != null) {
              synchronized (toRep) {
                List<JSONObject> jos = new ArrayList<JSONObject>();
                for (JSONObject jo : toRep) {
                  if (nr >= nr_max) {
                    break;
                  }
                  sb.append("+REP:");
                  sb.append(jo.toString());
                  sb.append("\n");
                  jos.add(jo);
                  nr++;
                }
                for (JSONObject j : jos) {
                  toRep.remove(j);
                }
              }
            } else {
              toRep = new TreeSet<JSONObject>();
            }

            if (toCheck != null) {
              synchronized (toCheck) {
                Set<SFileLocation> ls = new TreeSet<SFileLocation>();
                for (SFileLocation loc : toCheck) {
                  if (nr >= nr_max) {
                    break;
                  }
                  if (ni != null) {
                    sb.append("+CHK:");
                    sb.append(loc.getDevid());
                    sb.append(":");
                    sb.append(ni.getMP(loc.getDevid()));
                    sb.append(":");
                    sb.append(loc.getLocation());
                    sb.append("\n");

                    ls.add(loc);
                    nr++;
                  }
                }
                for (SFileLocation l : ls) {
                  toDelete.remove(l);
                }
              }
            } else {
              toCheck = new TreeSet<SFileLocation>();
            }

            if (sb.length() > 4) {
              try {
                String sendStr = sb.toString();
                DatagramPacket sendPacket = new DatagramPacket(sendStr.getBytes(), sendStr.length(),
                    ni.address, ni.port);

                server.send(sendPacket);
              } catch (IOException e) {
                LOG.error(e, e);
              }
            }
            // check if we handles all the cmds
            if (!(toRep.size() > 0 || toDelete.size() > 0 || toCheck.size() > 0)) {
              break;
            }
          }
          } catch (Exception e) {
            LOG.error(e, e);
          }
        }
      }
    }

    public class DMThread implements Runnable {
      Thread runner;
      DMHandleReportThread dmhrt = null;

      public DMThread(String threadName) {
        dmhrt = new DMHandleReportThread("DiskManagerHandleReportThread");
        runner = new Thread(this, threadName);
        runner.start();
      }

      public class DMHandleReportThread implements Runnable {
        Thread runner;

        public DMHandleReportThread(String threadName) {
          runner = new Thread(this, threadName);
          runner.start();
        }

        // if we have detected one SLAVE cmd, just ignore other cmds.
        private boolean __handle_slave_report(DMReport report) {
          if (report.replies != null && report.replies.size() > 0) {
            for (DMReply dmr : report.replies) {
              if (dmr.type == DMReply.DMReplyType.SLAVE) {
                LOG.info("Detect one MS slave at " + dmr.args);
                alternateURI = dmr.args;
                return true;
              }
            }
          }
          return false;
        }

        @Override
        public void run() {
          while (true) {
            try {
              byte[] recvBuf = new byte[bsize];
              DatagramPacket recvPacket = new DatagramPacket(recvBuf , recvBuf.length);
              try {
                server.receive(recvPacket);
              } catch (IOException e) {
                e.printStackTrace();
                continue;
              }

              String recvStr = new String(recvPacket.getData() , 0 , recvPacket.getLength());

              DMReport report = parseReport(recvStr);

              if (report == null) {
                LOG.error("Invalid report from address: " + recvPacket.getAddress().getHostAddress());
                continue;
              }
              report.recvPacket = recvPacket;

              Node reportNode = null;

              if (report.node == null) {
                try {
                  synchronized (rs_s1) {
                    reportNode = rs_s1.findNode(recvPacket.getAddress().getHostAddress());
                  }
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              } else {
                // NOTE-XXX: handle SLAVE report here!
                if (__handle_slave_report(report)) {
                  continue;
                }
                // NOTE-XXX: handle audit report here!
                if (report.isComplexRpt) {
                  SysMonitor.dsfstat.updateDMAudit(report.replies);
                  // Track device
                  updateDeviceTracking(report.replies);
                  continue;
                }
                try {
                  synchronized (rs_s1) {
                    reportNode = rs_s1.getNode(report.node);
                  }
                } catch (MetaException e) {
                  LOG.error(e, e);
                }
              }
              report.reportNode = reportNode;

              if (reportNode == null) {
                String errStr = "Failed to find Node: " + report.node + ", IP=" + recvPacket.getAddress().getHostAddress();
                LOG.warn(errStr);
                // try to use "+NODE:node_name" to find
                report.sendStr = "+FAIL\n";
                report.sendStr += "+COMMENT:" + errStr;
              } else {
                // 0. update NodeInfo
                NodeInfo oni = null;

                oni = ndmap.get(reportNode.getNode_name());
                if (oni != null) {
                  oni.address = recvPacket.getAddress();
                  oni.port = recvPacket.getPort();
                  oni.totalReportNr++;
                  if (report.replies != null && report.replies.size() > 0) {
                    for (DMReply r : report.replies) {
                      String[] args = r.args.split(",");
                      switch (r.type) {
                      case INFO:
                        try {
                          oni.qrep = Long.parseLong(args[0]);
                          oni.hrep = Long.parseLong(args[1]);
                          oni.drep = Long.parseLong(args[2]);
                          oni.qdel = Long.parseLong(args[3]);
                          oni.hdel = Long.parseLong(args[4]);
                          oni.ddel = Long.parseLong(args[5]);
                          oni.tver = Long.parseLong(args[6]);
                          oni.tvyr = Long.parseLong(args[7]);
                          long cuptime = Long.parseLong(args[8]);
                          if (cuptime < oni.uptime && oni.uptime > 0) {
                            // FIXME: detect node reboot, we should clear all
                            // existing RAM files on this node
                            LOG.info("Detect node reboot: " + reportNode.getNode_name() +
                                " reboot at=" + (System.currentTimeMillis() / 1000 - cuptime) +
                                " last uptime=" + oni.uptime + " current uptime=" + cuptime);
                          }
                          oni.uptime = cuptime;
                          oni.load1 = Double.parseDouble(args[9]);
                          if (args.length > 10) {
                            oni.recvLatency = Long.parseLong(args[10]);
                          }
                        } catch (NumberFormatException e1) {
                          LOG.error(e1, e1);
                        } catch (IndexOutOfBoundsException e1) {
                          LOG.error(e1, e1);
                        }
                        break;
                      case VERIFY:
                        oni.totalVerify++;
                        break;
                      case REPLICATED:
                        oni.totalFileRep++;
                        break;
                      case DELETED:
                        oni.totalFileDel++;
                        break;
                      case FAILED_DEL:
                        oni.totalFailDel++;
                        // it is ok ignore any del failure
                        if (args.length == 4) {
                          LOG.info("Failed Delete on " + args[0] + " dev " + args[1] + ":" +
                              args[2] + " errcode=" + args[3]);
                        } else if (args.length == 3) {
                          LOG.info("Failed Delete on " + args[0] + " dev " + args[1] + ":" +
                              args[2]);
                        }
                        break;
                      case FAILED_REP:
                        oni.totalFailRep++;
                        // ok, we know that the SFL will be invalid parma...ly
                        SFileLocation sfl;

                        if (args.length == 4) {
                          LOG.info("Failed Replication on " + args[0] + " dev " + args[1] + ":" +
                              args[2] + " errcode=" + args[3]);
                        } else if (args.length == 3) {
                          LOG.info("Failed Replication on " + args[0] + " dev " + args[1] + ":" +
                              args[2]);
                        }
                        try {
                          synchronized (rs_s1) {
                            sfl = rs_s1.getSFileLocation(args[1], args[2]);
                            if (sfl != null) {
                              // delete this SFL right now
                              if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.OFFLINE) {
                                LOG.info("Failed Replication for file " + sfl.getFid() + " dev " + sfl.getDevid() +
                                    " loc " + sfl.getLocation() + ", delete it now.");
                                rs_s1.delSFileLocation(args[1], args[2]);
                                // BUG-XXX: shall we trigger a DELETE request to dservice?
                                synchronized (oni.toDelete) {
                                  oni.toDelete.add(sfl);
                                  LOG.info("----> Add toDelete " + sfl.getLocation() + ", qs " + cleanQ.size() +
                                      ", dev " + sfl.getDevid());
                                }
                              }
                            }
                          }
                        } catch (MetaException e) {
                          LOG.error(e, e);
                        }
                        break;
                      }
                    }
                  }
                  oni.lastReportStr = report.toString();
                }
                report.oni = oni;

                // 1. update Node status
                switch (reportNode.getStatus()) {
                default:
                case MetaStoreConst.MNodeStatus.ONLINE:
                  break;
                case MetaStoreConst.MNodeStatus.SUSPECT:
                  try {
                    reportNode.setStatus(MetaStoreConst.MNodeStatus.ONLINE);
                    synchronized (rs_s1) {
                      rs_s1.updateNode(reportNode);
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                  }
                  break;
                case MetaStoreConst.MNodeStatus.OFFLINE:
                  LOG.warn("OFFLINE node '" + reportNode.getNode_name() + "' do report?!");
                  break;
                }

                // 2. update NDMap
                synchronized (ndmap) {
                  addToNDMap(System.currentTimeMillis(), reportNode, report.dil);
                }

                // 3.0- Check if we are in slave role.
                // Only do report handling in master role.
                switch (role) {
                default:
                case SLAVE:
                  LOG.debug("SLAVE  RECV: " + recvStr);
                  continue;
                case MASTER:
                  LOG.debug("MASTER RECV: " + recvStr);
                }

                // 3. finally, add this report entry to queue
                synchronized (reportQueue) {
                  reportQueue.add(report);
                  reportQueue.notifyAll();
                }
              }
            } catch (Exception e) {
              LOG.error(e, e);
            }
          }
        }

      }

      public class DMReport {
        // Report Format:
        // +node:node_name
        // DEVMAPS
        // +CMD
        // +DEL:node,devid,location
        // +DEL:node,devid,location
        // ...
        // +REP:node,devid,location
        // +REP:node,devid,location
        // ...
        //
        // or
        //
        // +node:node_name
        // +CMD
        // +CMD
        // +A:13800000,devid,location,VFSOperation
        // +A:13800000,devid,location,VFSOperation
        // ...
        public String node = null;
        public List<DeviceInfo> dil = null;
        public List<DMReply> replies = null;
        public String sendStr = "+OK\n";

        // region for reply use
        DatagramPacket recvPacket = null;
        Node reportNode = null;
        NodeInfo oni = null;

        // is complex report?
        public boolean isComplexRpt = false;

        @Override
        public String toString() {
          String r = "";
          if (dil != null) {
            r += " DeviceInfo -> {\n";
            for (DeviceInfo di : dil) {
              r += " - " + di.dev + "," + di.mp + "," + di.used + "," + di.free + "\n";
            }
            r += "}\n";
          }
          if (replies != null) {
            r += " CMDs -> {\n";
            for (DMReply dmr : replies) {
              r += " - " + dmr.toString() + "\n";
            }
            r += "}\n";
          }
          return r;
        }
      }

      public DMReport parseReport(String recv) {
        DMReport r = new DMReport();
        String[] reports = recv.split("\\+CMD\n");

        switch (reports.length) {
        case 1:
          // only DEVMAPS
          r.node = reports[0].substring(0, reports[0].indexOf('\n')).replaceFirst("\\+node:", "");
          r.dil = parseDevices(reports[0].substring(reports[0].indexOf('\n') + 1));
          break;
        case 2:
          // contains CMDS
          r.node = reports[0].substring(0, reports[0].indexOf('\n')).replaceFirst("\\+node:", "");
          r.dil = parseDevices(reports[0].substring(reports[0].indexOf('\n') + 1));
          r.replies = parseCmds(reports[1]);
          break;
        case 3:
          // contains profile report, example format is:
          // +node:xxxxxx
          // +CMD
          // +CMD
          // +A:TAG,13800000,devid,location,VFSOperation
          r.isComplexRpt = true;
          r.node = reports[0].substring(0, reports[0].indexOf('\n')).replaceFirst("\\+node:", "");
          r.replies = parseCmds(reports[2]);
          break;
        default:
          LOG.error("parseReport '" + recv + "' error.");
          r = null;
        }

        // FIXME: do not print this info now
        if (r.replies != null && r.replies.size() > 0) {
          String infos = "----> node " + r.node + ", CMDS: {\n";
          for (DMReply reply : r.replies) {
            infos += "\t" + reply.toString() + "\n";
          }
          infos += "}";

          LOG.debug(infos);
        }
        if (r.dil != null) {
          String infos = "----> node " + r.node + ", DEVINFO: {\n";
          for (DeviceInfo di : r.dil) {
            infos += "----DEVINFO------>" + di.dev + "," + di.mp + "," + di.used + "," + di.free + "\n";
            // Note-XXX: update DTrace entry
            DiskManager.SysMonitor.hdt.updateDTrace(r.node, di);
          }
          LOG.debug(infos);
        }

        return r;
      }

      List<DMReply> parseCmds(String cmdStr) {
        List<DMReply> r = new ArrayList<DMReply>();
        String[] cmds = cmdStr.split("\n");

        for (int i = 0; i < cmds.length; i++) {
          DMReply dmr = new DMReply();

          if (cmds[i].startsWith("+INFO:")) {
            dmr.type = DMReply.DMReplyType.INFO;
            dmr.args = cmds[i].substring(6);
            r.add(dmr);
          } else if (cmds[i].startsWith("+REP:")) {
            dmr.type = DMReply.DMReplyType.REPLICATED;
            dmr.args = cmds[i].substring(5);
            r.add(dmr);
          } else if (cmds[i].startsWith("+DEL:")) {
            dmr.type = DMReply.DMReplyType.DELETED;
            dmr.args = cmds[i].substring(5);
            r.add(dmr);
          } else if (cmds[i].startsWith("+FAIL:REP:")) {
            LOG.error("RECV ERR: " + cmds[i]);
            dmr.type = DMReply.DMReplyType.FAILED_REP;
            dmr.args = cmds[i].substring(10);
            r.add(dmr);
            // release limit
            boolean release_fix_limit = false;

            if (closeRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT)) {
              closeRepLimit.incrementAndGet();
            } else {
              release_fix_limit = true;
            }
            if (release_fix_limit) {
              if (fixRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_FIX_REP_LIMIT)) {
                fixRepLimit.incrementAndGet();
              }
            }
            //TODO: delete the SFL now
          } else if (cmds[i].startsWith("+FAIL:DEL:")) {
            LOG.error("RECV ERR: " + cmds[i]);
            dmr.type = DMReply.DMReplyType.FAILED_DEL;
            dmr.args = cmds[i].substring(10);
            r.add(dmr);
          } else if (cmds[i].startsWith("+VERIFY:")) {
            dmr.type = DMReply.DMReplyType.VERIFY;
            dmr.args = cmds[i].substring(8);
            r.add(dmr);
          } else if (cmds[i].startsWith("+SLAVE:")) {
            dmr.type = DMReply.DMReplyType.SLAVE;
            dmr.args = cmds[i].substring(7);
            r.add(dmr);
          } else if (cmds[i].startsWith("+A:")) {
            dmr.type = DMReply.DMReplyType.AUDIT;
            dmr.args = cmds[i].substring(3);
            r.add(dmr);
          } else if (cmds[i].startsWith("+B:")) {
            dmr.type = DMReply.DMReplyType.TRACK_DEV;
            dmr.args = cmds[i].substring(3);
            r.add(dmr);
          }
        }

        return r;
      }

      // report format:
      // dev-id:mount_path,readnr,writenr,errnr,usedB,freeB\n
      public List<DeviceInfo> parseDevices(String report) {
        List<DeviceInfo> dilist = new ArrayList<DeviceInfo>();
        String lines[];

        if (report == null) {
          return null;
        }

        lines = report.split("\n");
        for (int i = 0; i < lines.length; i++) {
          String kv[] = lines[i].split(":");
          if (kv == null || kv.length < 2) {
            LOG.debug("Invalid report line: " + lines[i]);
            continue;
          }
          DeviceInfo di = new DeviceInfo();
          di.dev = kv[0];
          String stats[] = kv[1].split(",");
          if (stats == null || stats.length < 6) {
            LOG.debug("Invalid report line value: " + lines[i]);
            continue;
          }
          di.mp = stats[0];
          // BUG-XXX: do NOT update device prop here now!
          di.prop = 0;
          di.read_nr = Long.parseLong(stats[1]);
          di.write_nr = Long.parseLong(stats[2]);
          di.err_nr = Long.parseLong(stats[3]);
          di.used = Long.parseLong(stats[4]);
          di.free = Long.parseLong(stats[5]);

          dilist.add(di);
        }

        if (dilist.size() > 0) {
          return dilist;
        } else {
          return null;
        }
      }

      @Override
      public void run() {
        while (true) {
          try {
            // dequeue reports from the report queue
            DMReport report = reportQueue.poll();
            if (report == null) {
              try {
                synchronized (reportQueue) {
                  reportQueue.wait();
                }
              } catch (InterruptedException e) {
                LOG.debug(e, e);
              }
              continue;
            }

            if (report.reportNode == null || report.oni == null) {
              // ok, we just return the error message now
            } else {
              // 2.NA update metadata
              Set<SFile> toCheckRep = new HashSet<SFile>();
              Set<SFile> toCheckDel = new HashSet<SFile>();
              Set<SFLTriple> toCheckMig = new HashSet<SFLTriple>();
              if (report.replies != null) {
                for (DMReply r : report.replies) {
                  String[] args = r.args.split(",");
                  switch (r.type) {
                  case REPLICATED:
                    // release limiting
                    boolean release_fix_limit = false;

                    if (closeRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_CLOSE_REP_LIMIT)) {
                      closeRepLimit.incrementAndGet();
                    } else {
                      release_fix_limit = true;
                    }
                    if (release_fix_limit) {
                      if (fixRepLimit.get() < hiveConf.getLongVar(HiveConf.ConfVars.DM_FIX_REP_LIMIT)) {
                        fixRepLimit.incrementAndGet();
                      }
                    }
                    if (args.length < 3) {
                      LOG.warn("Invalid REP report: " + r.args);
                    } else {
                      if (args.length == 5) {
                        LOG.info("REPLICATED to " + report.node + " dev " + args[0] + ":" + args[1] + ":" + args[2] + ", latency=" + args[4]);
                      } else {
                        LOG.info("REPLICATED to " + report.node + " dev " + args[0] + ":" + args[1] + ":" + args[2]);
                      }
                      SFileLocation newsfl, toDel = null;
                      try {
                        synchronized (rs) {
                          newsfl = rs.getSFileLocation(args[1], args[2]);
                          if (newsfl == null) {
                            SFLTriple t = new SFLTriple(args[0], args[1], args[2]);
                            if (rrmap.containsKey(t.toString())) {
                              // this means REP might actually MIGRATE
                              toCheckMig.add(new SFLTriple(args[0], args[1], args[2]));
                              LOG.info("----> MIGRATE to " + args[0] + ":" + args[1] + "/" + args[2] + " DONE.");
                              break;
                            }
                            toDel = new SFileLocation();
                            toDel.setNode_name(args[0]);
                            toDel.setDevid(args[1]);
                            toDel.setLocation(args[2]);
                            throw new MetaException("Can not find SFileLocation " + args[0] + "," + args[1] + "," + args[2]);
                          }
                          synchronized (MetaStoreConst.file_reopen_lock) {
                            SFile file = rs.getSFile(newsfl.getFid());
                            if (file != null) {
                              if (file.getStore_status() == MetaStoreConst.MFileStoreStatus.INCREATE) {
                                // BUG-XXX: 2 situations here
                                // 1. reopen: bad, delete current SFL
                                // 2. increp: ok, accept current SFL, but do NOT update SFL except update_time
                                if (newsfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.INCREP) {
                                  newsfl.setDigest("INCREP@" + args[3]);
                                  rs.updateSFileLocation(newsfl);
                                } else {
                                  LOG.warn("Somebody reopen the file " + file.getFid() +
                                      " and we do replicate on it, so ignore this replicate and delete it:(");
                                  toDel = newsfl;
                                }
                              } else {
                                // BUG-XXX: consider increp situation, we might issue multiple INCREP request to dservice.
                                // On close_file, we set SFile.store_status to CLOSED. Then, if we receive one +REP_R
                                // response, we can't determine whether it is the last REP_R response.
                                toCheckRep.add(file);
                                newsfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
                                // BUG-XXX: We should check the digest here, and compare it with file.getDigest().
                                newsfl.setDigest(args[3]);
                                rs.updateSFileLocation(newsfl);
                              }
                            } else {
                              LOG.warn("SFL " + newsfl.getDevid() + ":" + newsfl.getLocation() + " -> FID " + newsfl.getFid() + " nonexist, delete it.");
                              toDel = newsfl;
                            }
                          }
                        }
                      } catch (MetaException e) {
                        LOG.error(e, e);
                      } finally {
                        if (toDel != null) {
                          asyncDelSFL(toDel);
                        }
                      }
                    }
                    break;
                  case DELETED:
                    if (args.length < 3) {
                      LOG.warn("Invalid DEL report: " + r.args);
                    } else {
                      if (args.length == 4) {
                        LOG.info("DELETE from " + args[0] + " dev " + args[1] + ":" + args[2] + ", latency=" + args[3]);
                      } else {
                        LOG.info("DELETE from " + args[0] + " dev " + args[1] + ":" + args[2]);
                      }
                      try {
                        synchronized (rs) {
                          SFileLocation sfl = rs.getSFileLocation(args[1], args[2]);
                          if (sfl != null) {
                            SFile file = rs.getSFile(sfl.getFid());
                            if (file != null) {
                              toCheckDel.add(file);
                            }
                            rs.delSFileLocation(args[1], args[2]);
                          }
                        }
                      } catch (MetaException e) {
                        e.printStackTrace();
                      }
                    }
                    break;
                  case VERIFY:
                    if (args.length < 4) {
                      LOG.warn("Invalid VERIFY report: " + r.args);
                    } else {
                      LOG.debug("Verify SFL: " + r.args);
                      synchronized (rs) {
                        SFileLocation sfl = rs.getSFileLocation(args[1], args[2]);
                        if (sfl == null) {
                          // NOTE: if we can not find the specified SFL, this means there
                          // is no metadata for this 'SFL'. Thus, we notify dservice to
                          // delete this 'SFL' if needed. (Dservice delete it if these files
                          // hadn't been touched for specified seconds.)
                          synchronized (report.oni.toVerify) {
                            report.oni.toVerify.add(args[1] + ":" + report.oni.getMP(args[1]) + ":" + args[2]);
                            LOG.info("----> Add toVerify " + args[0] + " " + args[1] + "," + args[2] +
                                " level " + args[3] +
                                ", qs " + report.oni.toVerify.size());
                            report.oni.totalVYR++;
                          }
                        } else {
                          if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.SUSPECT) {
                            sfl.setVisit_status(MetaStoreConst.MFileLocationVisitStatus.ONLINE);
                            try {
                              rs.updateSFileLocation(sfl);
                            } catch (Exception e) {
                              LOG.error(e, e);
                            }
                            LOG.info("Verify change dev: " + args[1] + " loc: " + args[2] + " sfl state from SUSPECT to ONLINE.");
                          }
                          // NOTE: check file location based on level
                          try {
                            int level = Integer.parseInt(args[3]);
                            if (level == 1 && args.length >= 5) {
                              // this means we need to check MD5 of SFL, but should consider the SFL status!
                              if (sfl.getDigest() != null && args[4] != null) {
                                if (sfl.getDigest().equals(args[4])) {
                                  // ok, md5 check passed
                                  LOG.debug("Verify SFL: " + r.args + " md5 checksum passed");
                                } else if (sfl.getDigest().length() != 32) {
                                  // ignore this master copy or other specified digest?
                                  // e.g. sfl.getDigest().equals("SFL_DEFAULT")
                                  // e.g. INCREP@...
                                } else {
                                  LOG.warn("Detect MD5 mismatch for fid=" + sfl.getFid() +
                                      " expect " + sfl.getDigest() + " got SFL: " + r.args);
                                  if (r.args != null) {
                                    if (sfl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE &&
                                        System.currentTimeMillis() - sfl.getUpdate_time() > 5 * 60 * 1000 &&
                                        r.args.equalsIgnoreCase("d41d8cd98f00b204e9800998ecf8427e")) {
                                      // FIXME: target directory is empty? we can safely delete it
                                      LOG.warn("Detect fid=" + sfl.getFid() + " SFL dev " + sfl.getDevid() +
                                          " loc " + sfl.getLocation() + "vstatus=" +
                                          sfl.getVisit_status() + " is EMPTY, async del.");
                                      asyncDelSFL(sfl);
                                    }
                                  }
                                }
                              }
                            }
                          } catch (NumberFormatException nfe) {}
                        }
                      }
                    }

                    break;
                  case INFO:
                  case FAILED_REP:
                  case FAILED_DEL:
                    break;
                  default:
                    LOG.warn("Invalid DMReply type: " + r.type);
                  }
                }
              }
              if (!toCheckRep.isEmpty()) {
                for (SFile f : toCheckRep) {
                  try {
                    synchronized (rs) {
                      List<SFileLocation> sfl = rs.getSFileLocations(f.getFid());
                      int repnr = 0;
                      if (sfl != null) {
                        for (SFileLocation fl : sfl) {
                          if (fl.getVisit_status() == MetaStoreConst.MFileLocationVisitStatus.ONLINE) {
                            repnr++;
                          }
                        }
                      }
                      if (f.getRep_nr() == repnr && f.getStore_status() == MetaStoreConst.MFileStoreStatus.CLOSED) {
                        f.setStore_status(MetaStoreConst.MFileStoreStatus.REPLICATED);
                        rs.updateSFile(f);
                      }
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                  }
                }
                toCheckRep.clear();
              }
              if (!toCheckDel.isEmpty()) {
                for (SFile f : toCheckDel) {
                  try {
                    synchronized (rs) {
                      List<SFileLocation> sfl = rs.getSFileLocations(f.getFid());
                      if (sfl != null && sfl.size() == 0) {
                        // delete this file: if it's in INCREATE state, ignore it; if it's in !INCREAT && !RM_PHYSICAL state, warning it.
                        if (f.getStore_status() != MetaStoreConst.MFileStoreStatus.INCREATE) {
                          if (f.getStore_status() != MetaStoreConst.MFileStoreStatus.RM_PHYSICAL) {
                            LOG.warn("FID " + f.getFid() + " will be deleted(reason: no valid locations), however it's status is " + f.getStore_status());
                          }
                          rs.delSFile(f.getFid());
                        }
                      }
                    }
                  } catch (MetaException e) {
                    LOG.error(e, e);
                  }
                }
                toCheckDel.clear();
              }
              if (!toCheckMig.isEmpty()) {
                if (HMSHandler.topdcli == null) {
                  try {
                    HiveMetaStore.connect_to_top_attribution(hiveConf);
                  } catch (MetaException e) {
                    LOG.error(e, e);
                  }
                }
                if (HMSHandler.topdcli != null) {
                  for (SFLTriple t : toCheckMig) {
                    MigrateEntry me = rrmap.get(t.toString());
                    if (me == null) {
                      LOG.error("Invalid SFLTriple-MigrateEntry map.");
                      continue;
                    } else {
                      rrmap.remove(t);
                      // connect to remote DC, and close the file
                      try {
                        Database rdb = null;
                        synchronized (HMSHandler.topdcli) {
                          rdb = HMSHandler.topdcli.get_attribution(me.to_dc);
                        }
                        IMetaStoreClient rcli = new HiveMetaStoreClient(rdb.getParameters().get("service.metastore.uri"),
                            HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES),
                            hiveConf.getIntVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY),
                            null);
                        SFile sf = rcli.get_file_by_name(t.node, t.devid, t.location);
                        sf.setDigest("REMOTE-DIGESTED!");
                        rcli.close_file(sf);
                        LOG.info("Close remote file: " + t.node + ":" + t.devid + ":" + t.location);
                        rcli.close();
                      } catch (NoSuchObjectException e) {
                        LOG.error(e, e);
                      } catch (TException e) {
                        LOG.error(e, e);
                      }
                      // remove the partition-file relationship from metastore
                      if (me.is_part) {
                        // is partition
                        synchronized (rs) {
                          Partition np;
                          try {
                            np = rs.getPartition(me.part.getDbName(), me.part.getTableName(), me.part.getPartitionName());
                            long fid = me.timap.get(t.toString());
                            me.timap.remove(t.toString());
                            List<Long> nfiles = new ArrayList<Long>();
                            nfiles.addAll(np.getFiles());
                            nfiles.remove(fid);
                            np.setFiles(nfiles);
                            rs.updatePartition(np);
                            LOG.info("Remove file fid " + fid + " from partition " + np.getPartitionName());
                          } catch (MetaException e) {
                            LOG.error(e, e);
                          } catch (NoSuchObjectException e) {
                            LOG.error(e, e);
                          } catch (InvalidObjectException e) {
                            LOG.error(e, e);
                          }
                        }
                      } else {
                        // subpartition
                        Subpartition np;
                        try {
                          np = rs.getSubpartition(me.subpart.getDbName(), me.subpart.getTableName(), me.subpart.getPartitionName());
                          long fid = me.timap.get(t.toString());
                          me.timap.remove(t.toString());
                          List<Long> nfiles = new ArrayList<Long>();
                          nfiles.addAll(np.getFiles());
                          nfiles.remove(fid);
                          np.setFiles(nfiles);
                          rs.updateSubpartition(np);
                          LOG.info("Remove file fid " + fid + " from subpartition " + np.getPartitionName());
                        } catch (MetaException e) {
                          LOG.error(e, e);
                        } catch (NoSuchObjectException e) {
                          LOG.error(e, e);
                        } catch (InvalidObjectException e) {
                          LOG.error(e, e);
                        }
                      }
                    }
                  }
                }
              }

              // 3. append any commands
              int nr = 0;
              int nr_max = hiveConf.getIntVar(HiveConf.ConfVars.DM_APPEND_CMD_MAX);
              synchronized (ndmap) {
                NodeInfo ni = ndmap.get(report.reportNode.getNode_name());
                int nr_del_max = (ni.toRep.size() > 0 ? nr_max - 1 : nr_max);

                if (ni != null && ni.toDelete.size() > 0) {
                  synchronized (ni.toDelete) {
                    Set<SFileLocation> ls = new TreeSet<SFileLocation>();
                    for (SFileLocation loc : ni.toDelete) {
                      if (nr >= nr_del_max) {
                        break;
                      }
                      report.sendStr += "+DEL:" + loc.getNode_name() + ":" + loc.getDevid() + ":" +
                          ndmap.get(loc.getNode_name()).getMP(loc.getDevid()) + ":" +
                          loc.getLocation() + "\n";
                      ls.add(loc);
                      nr++;
                    }
                    for (SFileLocation l : ls) {
                      ni.toDelete.remove(l);
                    }
                  }
                }

                if (ni != null && ni.toRep.size() > 0) {
                  synchronized (ni.toRep) {
                    List<JSONObject> jos = new ArrayList<JSONObject>();
                    for (JSONObject jo : ni.toRep) {
                      if (nr >= nr_max) {
                        break;
                      }
                      report.sendStr += "+REP:" + jo.toString() + "\n";
                      jos.add(jo);
                      nr++;
                    }
                    for (JSONObject j : jos) {
                      ni.toRep.remove(j);
                    }
                  }
                }

                if (ni != null && ni.toVerify.size() > 0) {
                  synchronized (ni.toVerify) {
                    List<String> vs = new ArrayList<String>();
                    for (String v : ni.toVerify) {
                      if (nr >= nr_max) {
                        break;
                      }
                      report.sendStr += "+VYR:" + v + "\n";
                      vs.add(v);
                      nr++;
                    }
                    for (String v : vs) {
                      ni.toVerify.remove(v);
                    }
                  }
                }
              }
            }

            // send back the reply
            int port = report.recvPacket.getPort();
            byte[] sendBuf;
            sendBuf = report.sendStr.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendBuf , sendBuf.length ,
                report.recvPacket.getAddress() , port );
            try {
              server.send(sendPacket);
            } catch (IOException e) {
              LOG.error(e, e);
            }

          } catch (Exception e) {
            LOG.error(e, e);
          }
        }
      }
    }

    public static void main(String[] args) throws Exception {
      HiveConf hiveConf = new HiveConf();

      MulticastSocket server = new MulticastSocket(hiveConf.getIntVar(HiveConf.ConfVars.DISKMANAGERLISTENPORT));
      server.setTimeToLive(5);
      server.joinGroup(InetAddress.getByName(
            hiveConf.getVar(HiveConf.ConfVars.DM_MCAST_GROUP_IP)));

      while (true) {
        try {
          byte[] recvBuf = new byte[65536];
          DatagramPacket recvPacket = new DatagramPacket(recvBuf , recvBuf.length);
          try {
            server.receive(recvPacket);
          } catch (IOException e) {
            e.printStackTrace();
            continue;
          }

          String recvStr = new String(recvPacket.getData() , 0 , recvPacket.getLength());
          System.out.println("RECV: " + recvStr);

          String sendStr = "+OK\n";

          // send back the reply
          int port = recvPacket.getPort();
          byte[] sendBuf;
          sendBuf = sendStr.getBytes();
          DatagramPacket sendPacket = new DatagramPacket(sendBuf , sendBuf.length ,
              recvPacket.getAddress() , port );
          try {
            server.send(sendPacket);
          } catch (IOException e) {
            e.printStackTrace();
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
}
