package com.inmobi.grill.es.cube.join;

import java.util.List;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;


public class DimJoiner {
  public static final ESLogger LOG = Loggers.getLogger(DimJoiner.class);
  private final JoinCondition cond;
  private final JoinConfig config;
  private DimJoiner next;
  private DimLoader loader;
  private JoinType joinType;
  
  public enum JoinType {
    LEFT_OUTER,
    INNER
  }
  
  public DimJoiner(JoinConfig config, JoinCondition condition, DimLoader loader) {
    this.cond = condition;
    this.config = config;
    this.loader = loader;
    if ("inner".equalsIgnoreCase(config.joinType)) {
      joinType = JoinType.INNER;
    } else if ("left_outer".equalsIgnoreCase(config.joinType)) {
      joinType = JoinType.LEFT_OUTER;
    } else {
      throw new RuntimeException("Invalid join type: " + config.joinType);
    }
  }
  
  public void setJoinType(String joinType) {
    if ("inner".equalsIgnoreCase(config.joinType)) {
      this.joinType = JoinType.INNER;
    } else if ("left_outer".equalsIgnoreCase(config.joinType)) {
      this.joinType = JoinType.LEFT_OUTER;
    } else {
      throw new RuntimeException("Invalid join type: " + config.joinType);
    }
  }
  
  public void addNextJoiner(DimJoiner joiner) {
    next = joiner;
  }
  
  public DimJoiner getNextJoiner() {
    return next;
  }
  
  public boolean hasNext() {
    return next != null;
  }
  
  public boolean join(String column, String fkeyValue, List<Object> result) {
    boolean joined = false;
    if (!hasNext()) {
      joined = loader.load(fkeyValue, column, cond,  result);
      
      if (joinType == JoinType.INNER) {
        return joined;
      } else if (joinType == JoinType.LEFT_OUTER){
        return true;
      }
      
    } else {

      throw new UnsupportedOperationException("Multilevel join not yet supported");
    }
    
    // Ideally shouldnt reach here.
    return joined;
  }
}
