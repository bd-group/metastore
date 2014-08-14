package org.apache.hadoop.hive.metastore.model;

public class MetaStoreConst {
  public static Long file_reopen_lock = 0L;

  public class MFileLoadStatus {
    public static final int OK = 0;
    public static final int BAD = 1;
  }

  public class MFileStoreStatus {
    public static final int INCREATE = 0;
    public static final int CLOSED = 1;
    public static final int REPLICATED = 2;
    public static final int RM_LOGICAL = 3;
    public static final int RM_PHYSICAL = 4;
  }

  public class MFileLocationVisitStatus {
    public static final int OFFLINE = 0;
    public static final int ONLINE = 1;
    public static final int SUSPECT = 2;
  }

  public class MNodeStatus {
    public static final int ONLINE = 0;
    public static final int OFFLINE = 1;
    public static final int SUSPECT = 2;
    public static final int __MAX__ = 3;
  }

  public class MDeviceProp {
    public static final int GENERAL = 0;  // L2: e.g. SAS
    public static final int SHARED = 1; // L4: might attached to many nodes, e.g. NAS
    public static final int BACKUP = 2; // this means it's special SHARED device for backup use
    public static final int BACKUP_ALONE = 3; // special ALONE device for backup use
    public static final int CACHE = 4;  // L1: fast cache device, e.g. SSD
    public static final int MASS = 5; // L3: e.g. SATA
    public static final int __MAX__ = 6;

    public static final int __TYPE_MASK__ = 0x0f;
    public static final int __QUOTA_SHIFT__ = 4;
  }

  public class MDeviceStatus {
    public static final int ONLINE = 0;
    public static final int OFFLINE = 1;
    public static final int SUSPECT = 2;
    public static final int __MAX__ = 3;
  }

  public class MEquipRoomStatus {
//    public enum Status{
//      0,1,2;
//    }
    public static final int ONLINE = 0;
    public static final int OFFLINE = 1;
    public static final int SUSPECT = 2;

  }


}
