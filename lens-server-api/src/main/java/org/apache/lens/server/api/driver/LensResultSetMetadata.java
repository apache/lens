package org.apache.lens.server.api.driver;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;
import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.ResultColumn;

/**
 * The Class LensResultSetMetadata.
 */
public abstract class LensResultSetMetadata {

  public abstract List<ColumnDescriptor> getColumns();

  /**
   * To query result set metadata.
   *
   * @return the query result set metadata
   */
  public QueryResultSetMetadata toQueryResultSetMetadata() {
    List<ResultColumn> result = new ArrayList<ResultColumn>();
    for (ColumnDescriptor col : getColumns()) {
      result.add(new ResultColumn(col.getName(), col.getType().getName()));
    }
    return new QueryResultSetMetadata(result);
  }

  /**
   * Gets the qualified type name.
   *
   * @param typeDesc
   *          the type desc
   * @return the qualified type name
   */
  public static String getQualifiedTypeName(TypeDescriptor typeDesc) {
    if (typeDesc.getType().isQualifiedType()) {
      switch (typeDesc.getType()) {
      case VARCHAR_TYPE:
        return VarcharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case CHAR_TYPE:
        return CharTypeInfo.getQualifiedName(typeDesc.getTypeName(),
            typeDesc.getTypeQualifiers().getCharacterMaximumLength()).toLowerCase();
      case DECIMAL_TYPE:
        return DecimalTypeInfo.getQualifiedName(typeDesc.getTypeQualifiers().getPrecision(),
            typeDesc.getTypeQualifiers().getScale()).toLowerCase();
      }
    }
    return typeDesc.getTypeName().toLowerCase();
  }
}
