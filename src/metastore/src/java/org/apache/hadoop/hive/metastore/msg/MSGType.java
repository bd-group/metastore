package org.apache.hadoop.hive.metastore.msg;

/**
 * For msg definition send to other sub-system.
 * MSGType.
 * @author DENNIS
 */
public class MSGType {

  /**
   * 新建库
   */
  public static final int MSG_NEW_DATABESE = 1001;
  /**
   * 修改库
   */
  public static final int MSG_ALTER_DATABESE = 1002;
  /**
   * 修改库属性
   */
  public static final int MSG_ALTER_DATABESE_PARAM = 1003;
  /**
   * 删除库
   */
  public static final int MSG_DROP_DATABESE = 1004;
  /**
   * 新建表
   */
  public static final int MSG_NEW_TALBE = 1101;
  /**
   * 修改表的名字
   */
  public static final int MSG_ALT_TALBE_NAME = 1102;
  /**
   * 修改表数据的分布
   */
  public static final int MSG_ALT_TALBE_DISTRIBUTE = 1103;          ////这个先不管
  /**
   * 修改表分区方式
   */
  public static final int MSG_ALT_TALBE_PARTITIONING = 1104;

  /**
   * 修改文件划分规则
   */
  public static final int MSG_ALT_TABLE_SPLITKEYS = 1105;
  /**
   * 修改表删除列
   */
  public static final int MSG_ALT_TALBE_DEL_COL = 1201;
  /**
   * 修改表新增列
   */
  public static final int MSG_ALT_TALBE_ADD_COL = 1202;
  /**
   * 修改表修改列名
   */
  public static final int MSG_ALT_TALBE_ALT_COL_NAME = 1203;
  /**
   * 修改表修改列类型
   */
  public static final int MSG_ALT_TALBE_ALT_COL_TYPE = 1204;
  /**
   * 表参数变化
   */
  public static final int MSG_ALT_TALBE_ALT_COL_LENGTH = 1205;        ////注意：本事件不会触发！！！
  /**
   * 修改表修改列类型长度
   */
  public static final int MSG_ALT_TABLE_PARAM = 1206;
  /**
   * 删除表
   */
  public static final int MSG_DROP_TABLE = 1207;        //删除表
  /**
   * 表语义属性变化
   */
  public static final int MSG_TABLE_BUSITYPE_CHANGED = 1208;
  /**
   * 新建分区
   */
  public static final int MSG_NEW_PARTITION =  1301;
  /**
   * 修改分区
   */
  public static final int MSG_ALT_PARTITION =  1302;
  /**
   * 删除分区
   */
  public static final int MSG_DEL_PARTITION =  1303;
  /**
   * 增加分区文件
   */
  public static final int MSG_ADD_PARTITION_FILE =  1304;
  /**
   * 修改分区文件
   */
  public static final int MSG_ALT_PARTITION_FILE =  1305;           ////分区的先不管

  /**
   * 文件副本变化：例如，文件中新增或者删除了副本
   */
  public static final int MSG_REP_FILE_CHANGE =  1306;
  /**
   * 文件状态变化：例如，store_status的状态
   */
  public static final int MSG_STA_FILE_CHANGE =  1307;
  /**
   * 文件副本上下线变化：例如，文件的副本状态变化，ONLINE or OFFLINE
   */
  public static final int MSG_REP_FILE_ONOFF =  1308;
  /**
   * 删除分区文件
   */
  public static final int MSG_DEL_PARTITION_FILE =  1309;
  /**
   * 用户设置的目标副本数目变化
   */
  public static final int MSG_FILE_USER_SET_REP_CHANGE = 1310;
  /**
    *新建列索引
    */
  public static final int MSG_NEW_INDEX =  1401;
      /**
       * 修改列索引
       */
  public static final int MSG_ALT_INDEX =  1402;
      /**
       * 修改列索引属性
       */
  public static final int MSG_ALT_INDEX_PARAM = 1403;
      /**
       * 删除列索引
       */
  public static final int MSG_DEL_INDEX = 1404;
      /**
       * 新建分区索引
       */
  public static final int MSG_NEW_PARTITION_INDEX =  1405;
      /**
       * 修改分区索引
       */
  public static final int MSG_ALT_PARTITION_INDEX = 1406;           ////
      /**
       * 删除分区索引
       */
  public static final int MSG_DEL_PARTITION_INDEX = 1407;
      /**
       * 增加分区索引文件
       */
  public static final int MSG_NEW_PARTITION_INDEX_FILE =  1408;
      /**
       * 修改分区索引文件
       */
  public static final int MSG_ALT_PARTITION_INDEX_FILE = 1409;
      /**
       * 删除分区索引文件
       */
  public static final int MSG_DEL_PARTITION_INDEX_FILE = 1413;
      /**
       * 新增节点
       */
  public static final int MSG_NEW_NODE = 1501;
      /**
       * 删除节点
       */
  public static final int MSG_DEL_NODE = 1502;
      /**
       * 节点故障
       */
  public static final int MSG_FAIL_NODE = 1503;
      /**
       * 节点恢复
       */
  public static final int MSG_BACK_NODE = 1504;

  /**
   * 新建schema
   */
  public static final int MSG_CREATE_SCHEMA = 1601;
  /**
   *修改schema名字
   */
  public static final int MSG_MODIFY_SCHEMA_NAME = 1602;
  /**
   *修改schema删除列
   */
  public static final int MSG_MODIFY_SCHEMA_DEL_COL = 1603;
  /**
   *修改schema新增列
   */
  public static final int MSG_MODIFY_SCHEMA_ADD_COL = 1604;
  /**
   *修改schema修改列名
   */
  public static final int MSG_MODIFY_SCHEMA_ALT_COL_NAME = 1605;
  /**
   *修改schema修改列类型
   */
  public static final int MSG_MODIFY_SCHEMA_ALT_COL_TYPE = 1606;
  /**
   * 修改schema参数
   */
  public static final int MSG_MODIFY_SCHEMA_PARAM = 1607;
  /**
   * 删除schema
   */
  public static final int MSG_DEL_SCHEMA = 1608;
  /**
   *修改schema修改comment
   */
  public static final int MSG_MODIFY_SCHEMA_ALT_COL_COMMENT = 1609;
  /**
   * dw1 专用DDL语句
   */
  public static final int MSG_DDL_DIRECT_DW1 = 2001;
  /**
   * dw2 专用DDL语句
   */
  public static final int MSG_DDL_DIRECT_DW2 = 2002;

  /**
   * 新增节点组
   */
  public static final int MSG_NEW_NODEGROUP = 3001;
  /**
   * 修改节点组
   */
  public static final int MSG_MODIFY_NODEGROUP = 3002;
  /**
   * 删除节点组
   */
  public static final int MSG_DEL_NODEGROUP = 3003;
  /**
   * 修改节点组
   */
  public static final int MSG_ALTER_NODEGROUP = 3004;
  /**
   * 创建新文件
   */

  public static final int MSG_CREATE_FILE = 4001;
  /**
   * 物理删除文件：后续对该fid的访问将会失败
   */
  public static final int MSG_DEL_FILE = 4002;

  //所有grant
  public static final int MSG_GRANT_GLOBAL = 5101;
  public static final int MSG_GRANT_DB = 5102;
  public static final int MSG_GRANT_TABLE = 5103;
  public static final int MSG_GRANT_SCHEMA = 5104;
  public static final int MSG_GRANT_PARTITION = 5105;
  public static final int MSG_GRANT_PARTITION_COLUMN = 5106;
  public static final int MSG_GRANT_TABLE_COLUMN = 5107;
  //所有revoke
  public static final int MSG_REVOKE_GLOBAL = 5201;
  public static final int MSG_REVOKE_DB = 5202;
  public static final int MSG_REVOKE_TABLE = 5203;
  public static final int MSG_REVOKE_SCHEMA = 5204;
  public static final int MSG_REVOKE_PARTITION = 5205;
  public static final int MSG_REVOKE_PARTITION_COLUMN = 5206;
  public static final int MSG_REVOKE_TABLE_COLUMN = 5207;

  /**
   * 创建新设备，且处于可用状态，或者更新设备
   */
  public static final int MSG_CREATE_DEVICE = 6001;
  /**
   * 删除设备
   */
  public static final int MSG_DEL_DEVICE = 6002;
  /**
   * 设备进入可读可写状态
   */
  public static final int MSG_DEVICE_RW = 6003;
  /**
   * 设备进入只读状态
   */
  public static final int MSG_DEVICE_RO = 6004;
  /**
   * 设备进入下线状态，不接受任何读写
   */
  public static final int MSG_DEVICE_OFFLINE = 6006;
  /**
   * 设备进入非稳状态，平台检测到设备不可达，但可能是网络等原因造成，
   * 检索和加载可能仍能够使用
   */
  public static final int MSG_DEVICE_SUSPECT = 6005;
}
