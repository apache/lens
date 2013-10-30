package com.inmobi.grill.client.api;

import java.util.Date;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QuerySubmitResult;

@XmlRootElement
public class PreparedQueryContext {
  @XmlElement
  private QueryPrepareHandle prepareHandle;
  @XmlElement
  private String userQuery;
  @XmlElement
  private Date preparedTime;
  @XmlElement
  private String preparedUser;
  @XmlElement
  private String selectedDriverClassName;
  @XmlElement
  private String driverQuery;
  @XmlElement
  private QueryConf conf;
  

  public PreparedQueryContext() {
    // for JAXB
  }
  public PreparedQueryContext(com.inmobi.grill.api.PreparedQueryContext pctx) {
    this.userQuery = pctx.getUserQuery();
    this.preparedTime = pctx.getPreparedTime();
    this.preparedUser = pctx.getPreparedUser();
    this.prepareHandle = pctx.getPrepareHandle();
    this.driverQuery = pctx.getDriverQuery();
    this.selectedDriverClassName = pctx.getSelectedDriver()
        .getClass().getCanonicalName();
    this.conf = new QueryConf();
    pctx.getConf().iterator();
    for (Map.Entry<String, String> entry : pctx.getConf()) {
      this.conf.addProperty(entry.getKey(), entry.getValue());
    }
  }
  /**
   * @return the prepareHandle
   */
  public QueryPrepareHandle getPrepareHandle() {
    return prepareHandle;
  }
  /**
   * @return the userQuery
   */
  public String getUserQuery() {
    return userQuery;
  }
  /**
   * @return the preparedTime
   */
  public Date getPreparedTime() {
    return preparedTime;
  }
  /**
   * @return the preparedUser
   */
  public String getPreparedUser() {
    return preparedUser;
  }
  /**
   * @return the selectedDriverClassName
   */
  public String getSelectedDriverClassName() {
    return selectedDriverClassName;
  }
  /**
   * @return the driverQuery
   */
  public String getDriverQuery() {
    return driverQuery;
  }
  /**
   * @return the conf
   */
  public QueryConf getConf() {
    return conf;
  }
}
