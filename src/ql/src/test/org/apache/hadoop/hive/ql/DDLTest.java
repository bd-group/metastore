package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;

public class DDLTest {

  /**
   * @param args
   */
  public static void main(String[] args) {
    // TODO Auto-generated method stub

    String sql = "create EQROOM('aaa',qw,'abc','sd')";
    Driver dr = new Driver(new HiveConf());
    dr.compile(sql);

  }

}
