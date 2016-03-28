
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

/**
 * RefreshDesc.
 *
 */
@Explain(displayName = "Refresh")
public class RefreshDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String refreshType;


  public RefreshDesc() {
  }


  public RefreshDesc(String refreshType) {
    super();
    this.refreshType = refreshType;
  }


  public String getRefreshType() {
    return refreshType;
  }


  public void setRefreshType(String refreshType) {
    this.refreshType = refreshType;
  }



}
