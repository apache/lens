package com.inmobi.grill.jdbc;


import com.inmobi.grill.api.query.QueryResultSetMetadata;
import com.inmobi.grill.api.query.ResultColumn;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GrillResultSetMetaData implements ResultSetMetaData {

  private final QueryResultSetMetadata metadata;
  private final List<String> colNames;
  private final List<String> colTypes;


  public GrillResultSetMetaData(QueryResultSetMetadata metadata) {
    this.metadata = metadata;
    colNames = new ArrayList<String>();
    colTypes = new ArrayList<String>();
    for (ResultColumn column : metadata.getColumns()) {
      colNames.add(column.getName());
      colTypes.add(column.getType().name());
    }
  }


  @Override
  public int getColumnCount() throws SQLException {
    return metadata.getColumns().size();
  }

  @Override
  public boolean isAutoIncrement(int index) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int index) throws SQLException {
    String type = colTypes.get(toZeroIndex(index));

    if ("string".equalsIgnoreCase(type)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean isSearchable(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean isCurrency(int i) throws SQLException {
    return false;
  }

  @Override
  public int isNullable(int i) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSigned(int i) throws SQLException {
    return false;
  }

  @Override
  public int getColumnDisplaySize(int i) throws SQLException {
    return getPrecision(i);
  }

  @Override
  public String getColumnLabel(int i) throws SQLException {
    return colNames.get(toZeroIndex(i));
  }

  @Override
  public String getColumnName(int i) throws SQLException {
    return colNames.get(toZeroIndex(i));
  }

  @Override
  public String getSchemaName(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!");
  }

  @Override
  public int getPrecision(int i) throws SQLException {
    return JDBCUtils.columnPrecision(getColumnType(i));
  }

  @Override
  public int getScale(int i) throws SQLException {
    return JDBCUtils.columnScale(getColumnType(i));
  }

  @Override
  public String getTableName(int i) throws SQLException {
    throw new SQLException("Operation not supported!!!!");
  }

  @Override
  public String getCatalogName(int i) throws SQLException {
    throw new SQLException("Opertation not supported!!!");
  }

  @Override
  public int getColumnType(int index) throws SQLException {
    return JDBCUtils.getSQLType(colTypes.get(toZeroIndex(index)));
  }

  @Override
  public String getColumnTypeName(int index) throws SQLException {
    return colTypes.get(toZeroIndex(index));
  }

  @Override
  public boolean isReadOnly(int i) throws SQLException {
    return true;
  }

  @Override
  public boolean isWritable(int i) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    return false;
  }

  @Override
  public String getColumnClassName(int i) throws SQLException {
    return JDBCUtils.columnClassName(getColumnType(i));
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    return false;
  }

  protected int toZeroIndex(int column) throws SQLException {
    if (colTypes.isEmpty()) {
      throw new SQLException(
          "Could not determine column type name for ResultSet");
    }
    if (column < 1 || column > colTypes.size()) {
      throw new SQLException("Invalid column value: " + column);
    }
    return column - 1;
  }
}
