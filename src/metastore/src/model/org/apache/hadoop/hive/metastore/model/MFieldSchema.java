/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package org.apache.hadoop.hive.metastore.model;



/**
 * Represent a column or a type of a table or object
 */
public class MFieldSchema {
  private String name;
  private String type;
  private String comment;
  private long version;
  public MFieldSchema() {}

  /**
   * @param comment
   * @param name
   * @param type
   */
  public MFieldSchema(String name, String type, String comment, long version) {
    this.comment = comment;
    this.name = name;
    this.type = type;
    this.setVersion(version);
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }
  /**
   * @param name the name to set
   */
  public void setName(String name) {
    this.name = name;
  }
  /**
   * @return the comment
   */
  public String getComment() {
    return comment;
  }
  /**
   * @param comment the comment to set
   */
  public void setComment(String comment) {
    this.comment = comment;
  }
  /**
   * @return the type
   */
  public String getType() {
    return type;
  }
  /**
   * @param field the type to set
   */
  public void setType(String field) {
    this.type = field;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }


  @Override
  public boolean equals(Object o)         //重写equals方法,在修改表增删列中的removeall方法中要用到,列名和类型相同就认为列一样
  {
    if(this == o) {
      return true;
    }
    if(o == null) {
      return false;
    }
    if(!(o instanceof MFieldSchema)) {
      return false;
    }
    MFieldSchema m = (MFieldSchema)o;
//    return ((this.name == null && m.getName() == null) || this.name.equals(m.getName()))
//        && ((this.type == null && m.getType() == null) || this.getType().equals(m.getType()) )
//        && (this.comment == null && m.getComment() == null) ||  this.getComment().equals(m.getComment());
    return this.name.equals(m.getName()) && this.type.equals(m.getType());
  }



  @Override
  public int hashCode()
  {
    return name.hashCode()+type.hashCode();
  }

  @Override
  public String toString() {
    return "MFieldSchema [name=" + name + ", type=" + type + ", comment=" + comment + ", version="
        + version + "]";
  }


}
