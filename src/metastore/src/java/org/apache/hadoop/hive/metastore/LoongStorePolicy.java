package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.newms.RawStoreImp;

public class LoongStorePolicy {
  public static RawStore rs;
  public static Log LOG = LogFactory.getLog(LoongStorePolicy.class);

 static {
    LOG.info(System.getProperty("java.library.path"));
    System.loadLibrary("LoongStorePolicy");
    try {
      rs = new RawStoreImp();
    } catch (IOException e) {
      LOG.error(e, e);
    }
  }
  public LoongStorePolicy() {

  }

  public native int setAllocAffinity(String path);

  public native String getIstInfo(String path, int offset);


  public IstInfo convertToIstInfo(String str) {
    if (str.equals(null)) {
      return null;
    }
    String[] attrs = str.split("\\|");
    if (attrs.length != 4) {
      LOG.info("ztt.istinfo is not complete.");
      return null;
    }
    int istnum = Integer.parseInt(attrs[0]);
    int pad = Integer.parseInt(attrs[1]);
    String[] istids = attrs[2].split(",");
    List<Integer> istid = new ArrayList<Integer>();
    for (int i = 0; i < istids.length; i++) {
      istid.add(Integer.parseInt(istids[i]));
    }
    String[] ipsstr = attrs[3].split(",");
    List<List<String>> ips = new ArrayList<List<String>>();
    for (int i = 0; i < ipsstr.length; i++) {
      String[] ipstr = ipsstr[i].split("/");
      List<String> ip = new ArrayList<String>();
      for (int j = 0; j < ipstr.length; j++) {
        ip.add(ipstr[j]);
      }
      ips.add(ip);
    }
    return new IstInfo(istnum, pad, istid, ips);
  }

  public String getNode(String path, int offset) {
    String info = getIstInfo(path, offset);
//    String info = "1|0|5|127.0.1.1/10.61.3.26/202.106.199.36";
    if (info.startsWith("#")) {
      LOG.info("ztt.LoongStore error : " +info);
      return null;
    }
    IstInfo istInfo = convertToIstInfo(info);
    if(istInfo == null){
      return null;
    }
    LOG.info(istInfo.toString());
    for (List<String> ipList : istInfo.ips) {
      LOG.info("ztt.LoongStore provide one node to store data.");
      for (String ip : ipList) {
        Node node = null;
        synchronized (rs) {
          try {
            node = rs.findNode(ip);
            if(node != null) {
              LOG.info("node: " + node + "  ip: " + ip);
            }
          } catch (MetaException e) {
            LOG.error(e, e);
          }
        }
        if (node != null) {
          return node.getNode_name();
        }
      }
    }
    LOG.info("LoongStore does not provide one node to store data.");
    return null;
  }

  public class IstInfo {
    public int istnum; // 存储结点的个数
    public int pad;
    public List<Integer> istid; // 存储结点在文件系统里面的id,最多16个
    public List<List<String>> ips;

    public IstInfo(int istnum, int pad, List<Integer> istid, List<List<String>> ips) {
      this.istnum = istnum;
      this.pad = pad;
      this.istid = istid;
      this.ips = ips;
    }

    @Override
    public String toString() {
      return "IstInfo [istnum=" + istnum + ", pad=" + pad + ", istid=" + istid + ", ips=" + ips
          + "]";
    }


  }

}