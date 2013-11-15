package com.inmobi.grill.client.api;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.InMemoryResultSet;
import com.inmobi.grill.exception.GrillException;

@XmlRootElement
public class InMemoryQueryResult extends QueryResult {

  public static class Row {
    @XmlElementWrapper
    List<ColumnValue> row;

    public Row() {
    }

    public Row(List<ColumnValue> row) {
      this.row = row;
    }

    public List<ColumnValue> getRow() {
      return row;
    }
    public String toString() {
      return row.toString();
    }
  }

  public static class ColumnValue {
    @XmlElement
    protected Object value;

    public ColumnValue() {
    }
    public ColumnValue(Object value) {
      this.value = value;
    }

    public Object getValue() {
      return value;
    }
    public String toString() {
      if (value != null) {
        return value.toString();
      }
      return null;
    }
  }

  @XmlElementWrapper
  private List<Row> rows = new ArrayList<Row>();

  public InMemoryQueryResult() {
  }

  public InMemoryQueryResult(InMemoryResultSet result) throws GrillException {
    while (result.hasNext()) {
      List<ColumnValue> row = new ArrayList<ColumnValue>();
      for (Object col : result.next()) {
        row.add(new ColumnValue(col));
      }
      rows.add(new Row(row));
    }
  }

  public List<Row> getRows() {
    return rows;
  }
}
