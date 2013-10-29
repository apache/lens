package com.inmobi.grill.client.api;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.QueryCost;
import com.inmobi.grill.api.QueryPrepareHandle;
import com.inmobi.grill.api.QuerySubmitResult;
import com.inmobi.grill.api.QueryPlan.ExecMode;
import com.inmobi.grill.api.QueryPlan.ScanMode;

@XmlRootElement
public class QueryPlan extends QuerySubmitResult {

  @XmlElement
  private int numJoins = 0;
  @XmlElement
  private int numGbys = 0;
  @XmlElement
  private int numSels = 0;
  @XmlElement
  private int numSelDi = 0;
  @XmlElement
  private int numHaving = 0;
  @XmlElement
  private int numObys = 0;
  @XmlElement
  private int numAggrExprs = 0;
  @XmlElement
  private int numFilters = 0;
  @XmlElementWrapper
  private List<String> tablesQueried;
  @XmlElement
  private boolean hasSubQuery = false;
  @XmlElement
  private ExecMode execMode;
  @XmlElement
  private ScanMode scanMode;
  @XmlElementWrapper
  private Map<String, Double> tableWeights;
  @XmlElement
  private Double joinWeight;
  @XmlElement
  private Double gbyWeight;
  @XmlElement
  private Double filterWeight;
  @XmlElement
  private Double havingWeight;
  @XmlElement
  private Double obyWeight;
  @XmlElement
  private Double selectWeight;
  @XmlElement
  private QueryPrepareHandle handle;
  @XmlElement
  private String planString;
  @XmlElement
  private QueryCost queryCost;

  public QueryPlan() {
  }
  public QueryPlan(com.inmobi.grill.api.QueryPlan p) {
    this.numJoins = p.getNumJoins();
    this.numGbys = p.getNumGbys();
    this.numSels = p.getNumSels();
    this.numSelDi = p.getNumSelDistincts();
    this.numFilters = p.getNumFilters();
    this.numHaving = p.getNumHaving();
    this.numObys = p.getNumOrderBys();
    this.numAggrExprs = p.getNumAggreagateExprs();
    this.hasSubQuery = p.hasSubQuery();
    this.tablesQueried = p.getTablesQueried();
    this.tableWeights = p.getTableWeights();
    this.planString = p.getPlan();
    this.execMode = p.getExecMode();
    this.scanMode = p.getScanMode();
    this.joinWeight = p.getJoinWeight();
    this.gbyWeight = p.getGbyWeight();
    this.filterWeight = p.getFilterWeight();
    this.havingWeight = p.getHavingWeight();
    this.obyWeight  = p.getObyWeight();
    this.selectWeight = p.getSelectWeight();
    this.handle = p.getPrepareHandle();
    this.queryCost = p.getCost();
  }

  /**
   * Get the query plan
   * 
   * @return The string representation of the plan
   */
  public String getPlan() {
    return this.planString;
  }

  /**
   * Get the cost associated with the plan
   * 
   * @return QueryCost object
   */
  public QueryCost getCost() {
    return this.queryCost;
  }

  /**
   * @return the numJoins
   */
  public int getNumJoins() {
    return numJoins;
  }

  /**
   * Get the number of group by expressions on query
   * 
   * @return the numGbys
   */
  public int getNumGbys() {
    return numGbys;
  }

  /**
   * Get the number of select expressions
   * 
   * @return the numSels
   */
  public int getNumSels() {
    return numSels;
  }

  /**
   * Get the number distinct select expressions
   * 
   * @return the numSelDi
   */
  public int getNumSelDistincts() {
    return numSelDi;
  }

  /**
   * Get the number of having expressions on query
   * 
   * @return the numHaving
   */
  public int getNumHaving() {
    return numHaving;
  }

  /**
   * Get the number of order by expressions on query
   * 
   * @return the numObys
   */
  public int getNumOrderBys() {
    return numObys;
  }

  /**
   * Get the number of non-default aggregation expressions in query
   * 
   * @return the numNonDefaultAggrExprs
   */
  public int getNumAggrExprs() {
    return numAggrExprs;
  }

  /**
   * Get the list of tables to be queried
   * 
   * @return the tablesQueried
   */
  public List<String> getTablesQueried() {
    return tablesQueried;
  }

  /**
   * Get the number of filters in query
   * 
   * @return the numFilters
   */
  public int getNumFilters() {
    return numFilters;
  }

  /**
   * Get if the query has a subquery or not
   * 
   * @return the hasSubQuery true if query has subquery, false otherwise 
   */
  public boolean hasSubQuery() {
    return hasSubQuery;
  }

  /**
   * Get the table weights
   * 
   * @return the tableWeights
   */
  public Map<String, Double> getTableWeights() {
    return tableWeights;
  }

  /**
   * Get the exec mode
   * 
   * @return the {@link ExecMode}
   */
  public ExecMode getExecMode() {
    return execMode;
  }

  /**
   * Get the scan mode.
   * 
   * @return the {@link ScanMode}
   */
  public ScanMode getScanMode() {
    return scanMode;
  }

  /**
   * Get the weight associated with joins
   * 
   * @return the joinWeight
   */
  public Double getJoinWeight() {
    return joinWeight;
  }

  /**
   * Set the weight associated with group by expressions.
   * 
   * @return the gbyWeight
   */
  public Double getGbyWeight() {
    return gbyWeight;
  }

  /**
   * Set the weight associated with filter expressions.
   * 
   * @return the filterWeight
   */
  public Double getFilterWeight() {
    return filterWeight;
  }

  /**
   * Get the weight associated with order by expressions.
   * 
   * @return the obyWeight
   */
  public Double getObyWeight() {
    return obyWeight;
  }

  /**
   * Set the weight associated with having expressions.
   * 
   * @return the havingWeight
   */
  public Double getHavingWeight() {
    return havingWeight;
  }

  /**
   * Get the weight associated with select expressions.
   * 
   * @return the selectWeight
   */
  public Double getSelectWeight() {
    return selectWeight;
  }

  /**
   * 
   * @return the prepare handle
   */
  public QueryPrepareHandle getPrepareHandle() {
    return handle;
  }

}
