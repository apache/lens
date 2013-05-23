package com.inmobi.grill.es.cube.script;

import java.util.Arrays;


public class GroupEntry {
  FieldGroup group;
  @SuppressWarnings("rawtypes")
  AggregateFunction[] functions;
  
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder("Group:").append(Arrays.toString(group.fields)).append(" Functions:").append(Arrays.toString(functions));
    return buf.toString();
  }
}
