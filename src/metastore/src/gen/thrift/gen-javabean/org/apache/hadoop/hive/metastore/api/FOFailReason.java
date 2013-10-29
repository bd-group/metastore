/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum FOFailReason implements org.apache.thrift.TEnum {
  INVALID_NODE(1),
  INVALID_TABLE(2),
  INVALID_FILE(3),
  INVALID_SPLIT_VALUES(4),
  INVALID_ATTRIBUTION(5),
  INVALID_NODE_GROUPS(6),
  NOSPACE(10),
  NOTEXIST(11),
  SAFEMODE(12),
  INVALID_STATE(13),
  TRY_AGAIN(14);

  private final int value;

  private FOFailReason(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static FOFailReason findByValue(int value) { 
    switch (value) {
      case 1:
        return INVALID_NODE;
      case 2:
        return INVALID_TABLE;
      case 3:
        return INVALID_FILE;
      case 4:
        return INVALID_SPLIT_VALUES;
      case 5:
        return INVALID_ATTRIBUTION;
      case 6:
        return INVALID_NODE_GROUPS;
      case 10:
        return NOSPACE;
      case 11:
        return NOTEXIST;
      case 12:
        return SAFEMODE;
      case 13:
        return INVALID_STATE;
      case 14:
        return TRY_AGAIN;
      default:
        return null;
    }
  }
}
