package org.apache.hadoop.hive.metastore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class LoongStorePolicy {
  static {
    System.loadLibrary("LoongStorePolicy");
}

public static class IstInfo {
    int istnum;  //存储结点的个数
    int pad;
    List<Integer> istid; //存储结点在文件系统里面的id,最多16个
    List<List<String>> ips;

    public IstInfo(int istnum, int pad, List<Integer> istid, List<List<String>> ips) {
        this.istnum = istnum;
        this.pad = pad;
        this.istid = istid;
        this.ips = ips;
    }

    public static IstInfo convertToIstInfo(String str) {
        JSONObject jsonObject = JSONObject.fromObject(str);
        int istnum = jsonObject.getInt("istnum");
        int pad = jsonObject.getInt("pad");
        JSONArray istidArray = jsonObject.getJSONArray("istid");
        List<Integer> istid = new ArrayList<Integer>();
        for (int i = 0; i < istidArray.size(); i++) {
            JSONObject object = istidArray.getJSONObject(i);
            istid.add(object.getInt("istid"));
        }
        JSONArray ipsArray = jsonObject.getJSONArray("ips");
        List<List<String>> ips = new ArrayList<List<String>>();
        for (int i = 0; i < ipsArray.size(); i++) {
            JSONObject ipObject = ipsArray.getJSONObject(i);
            List<String> ip = new ArrayList<String>();
            String[] ipstr = ipObject.getString("ips").split(";");
            for(int j = 0; j < ipstr.length; j ++) {
                ip.add(ipstr[j]);
            }
            ips.add(ip);
        }
        return new IstInfo(istnum,pad,istid,ips);
    }
};

public static ConcurrentHashMap<String, String> ipNodeMap = new ConcurrentHashMap<String, String>();

public  native int setAllocAffinity(String path);

public  native String getIstInfo(String path, int offset);

public  native String getIpNodeMap();

public static void parseIpNodeMap(String str) {
    String[] lines = str.split("\n");
    int size = lines.length;
    int i = 0;
    while (!lines[i].startsWith("#") && i < size) {
        String[] ls = lines[i].split("\t");
        if (ls.length == 2) {
          ipNodeMap.put(ls[0], ls[1]);
        }
        i++;
    }

}

public  String getNode(String path, int offset) {
    String str = getIstInfo(path, offset);
    IstInfo istInfo = IstInfo.convertToIstInfo(str);
    int num = istInfo.istnum;
    if (num == 1) {
        System.out.println("LoongStore provide one node to store data.");
        for (String ip : istInfo.ips.get(0)) {
            if (ipNodeMap.containsKey(ip)) {
                return ipNodeMap.get(ip);
            }
        }
    } else {
        System.out.println("LoongStore provide not one node to store data.");
    }
    return null;
}

  public  LoongStorePolicy() {
  // TODO Auto-generated constructor stub
    String str = getIpNodeMap();
    parseIpNodeMap(str);
  }
}
