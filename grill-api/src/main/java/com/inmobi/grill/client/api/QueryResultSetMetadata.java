package com.inmobi.grill.client.api;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import com.inmobi.grill.api.GrillResultSetMetadata;
import com.inmobi.grill.api.ResultColumn;
import com.inmobi.grill.exception.GrillException;

@XmlRootElement
public class QueryResultSetMetadata {

  @XmlElementWrapper
  private List<ResultColumn> columns;

  @XmlElement
  private int columnCount;

  public QueryResultSetMetadata() {
  }

  public QueryResultSetMetadata(GrillResultSetMetadata result)
      throws GrillException {
    columns = result.getColumns();
    this.columnCount = columns.size();
  }

  public List<ResultColumn> getColumns() {
    return columns;
  }

  public int getColumnCount() {
    return columnCount;
  }
}
