package com.inmobi.grill.api;

import java.util.List;

public interface GrillResultSetMetadata {

  public static class Column {
    private final String name;
    private final String type;
    
    public Column(String name, String type) {
      this.name = name;
      this.type = type;
    }

    /**
     * @return the name
     */
    public String getName() {
      return name;
    }

    /**
     * @return the type
     */
    public String getType() {
      return type;
    }

    @Override
    public String toString() {
      return new StringBuilder(name).append(':').append(type).toString();
    }
  }

  public List<Column> getColumns();
}
