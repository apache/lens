package com.inmobi.grill.es.cube.script;

import java.util.Arrays;

public class FieldGroup {
  int hashcode;
  boolean hasHashCode;
  
  final String[] fields;
  
  public FieldGroup(String[] fields) {
    int nfields = fields.length;
    this.fields = new String[nfields];
    System.arraycopy(fields, 0, this.fields, 0, nfields);
  }
  
  
  @Override
  public int hashCode() {
    return Arrays.hashCode(fields);
  }
  
  @Override
  public boolean equals(Object other) {
    if (! (other instanceof FieldGroup)) {
      return false;
    }
    FieldGroup g = (FieldGroup) other;
    return Arrays.equals(fields, g.fields);
  }
}
