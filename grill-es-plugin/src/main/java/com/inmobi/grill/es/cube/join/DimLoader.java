package com.inmobi.grill.es.cube.join;

import java.util.List;

public abstract class DimLoader {
  protected final String field;
  
  public DimLoader(String field) {
    this.field = field;
  }
  
  public String field() {
    return field;
  }
  
  /**
   * @param key value of the field
   * @param lookupKey value of the join key
   * @param columnToLoad read values of this field and add them to the result list
   * @param cond each row should satisfy this condition. If this is null, then it is ignored
   * @param result list to be populated after loading values
   * @return true if any values were added to the result, false otherwise
   */
  public abstract boolean load(String lookupKey, String columnToLoad, JoinCondition cond, List<Object> result);
}
