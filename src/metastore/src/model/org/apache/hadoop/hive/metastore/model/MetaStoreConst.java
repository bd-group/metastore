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

    public static final int __MAX__ = 5;
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
    public static final int RAM = 6; // L0: most fast device, memory
    public static final int __MAX__ = 7;
    public static final int __AUTOSELECT_R1__ = -1;
    public static final int __AUTOSELECT_R2__ = -2;
    public static final int __AUTOSELECT_R3__ = -3;

    // Leveling of devices
    public static final int L1 = 4;
    public static final int L2 = 0;
    public static final int L3 = 5;
    public static final int L4 = 1;

    public static final int __TYPE_MASK__ = 0x0f;
    public static final int __QUOTA_MASK__ = 0xf0;
    public static final int __QUOTA_SHIFT__ = 4;

    // tags of device: highest 4 bits
    public static final int __LOONGSTORE__ = 0x80000000;
  }

  // return if type1 BEFORE OR EQUAL type2
  public static boolean checkDeviceOrder(int type1, int type2) {
    // type order is 4>0>5>1
    if (type1 == type2) {
      return true;
    }
    if (type1 == MDeviceProp.CACHE && type2 == MDeviceProp.GENERAL) {
      return true;
    }
    if ((type1 == MDeviceProp.CACHE || type1 == MDeviceProp.GENERAL) &&
        (type2 == MDeviceProp.MASS)) {
      return true;
    }
    if ((type1 == MDeviceProp.CACHE || type1 == MDeviceProp.GENERAL ||
        type1 == MDeviceProp.MASS) &&
        (type2 == MDeviceProp.SHARED)) {
      return true;
    }
    return false;
  }

  public class MDeviceStatus {
    public static final int ONLINE = 0;   // Ok
    public static final int OFFLINE = 1;  // Read only
    public static final int SUSPECT = 2;  // Still allow access
    public static final int DISABLE = 3;  // No read and write
    public static final int __MAX__ = 4;
  }

  public class MEquipRoomStatus {
    public static final int ONLINE = 0;
    public static final int OFFLINE = 1;
    public static final int SUSPECT = 2;

  }


}
