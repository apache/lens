/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.lib.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.*;

/**
 * This SerDe can be used for processing JSON data in Hive. It supports arbitrary JSON data, and can handle all Hive
 * types except for UNION. However, the JSON data is expected to be a series of discrete records, rather than a JSON
 * array of objects.
 * <p/>
 * The Hive table is expected to contain columns with names corresponding to fields in the JSON data, but it is not
 * necessary for every JSON field to have a corresponding Hive column. Those JSON fields will be ignored during queries.
 * <p/>
 * Example:
 * <p/>
 * { "a": 1, "b": [ "str1", "str2" ], "c": { "field1": "val1" } }
 * <p/>
 * Could correspond to a table:
 * <p/>
 * CREATE TABLE foo (a INT, b ARRAY<STRING>, c STRUCT<field1:STRING>);
 * <p/>
 * JSON objects can also interpreted as a Hive MAP type, so long as the keys and values in the JSON object are all of
 * the appropriate types. For example, in the JSON above, another valid table declaraction would be:
 * <p/>
 * CREATE TABLE foo (a INT, b ARRAY<STRING>, c MAP<STRING,STRING>);
 * <p/>
 * Only STRING keys are supported for Hive MAPs.
 * <p/>
 */
public class JSonSerde implements SerDe {

  /** The row type info. */
  private StructTypeInfo rowTypeInfo;

  /** The row oi. */
  private ObjectInspector rowOI;

  /** The col names. */
  private List<String> colNames;

  /** The row. */
  private List<Object> row = new ArrayList<Object>();

  /**
   * An initialization function used to gather information about the table. Typically, a SerDe implementation will be
   * interested in the list of column names and their types. That information will be used to help perform actual
   * serialization and deserialization of data.
   *
   * @param conf
   *          the conf
   * @param tbl
   *          the tbl
   * @throws SerDeException
   *           the ser de exception
   */
  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    // Get a list of the table's column names.
    String colNamesStr = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    colNames = Arrays.asList(colNamesStr.split(","));

    // Get a list of TypeInfos for the columns. This list lines up with
    // the list of column names.
    String colTypesStr = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    List<TypeInfo> colTypes = TypeInfoUtils.getTypeInfosFromTypeString(colTypesStr);

    rowTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(colNames, colTypes);
    rowOI = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(rowTypeInfo);
  }

  /**
   * This method does the work of deserializing a record into Java objects that Hive can work with via the
   * ObjectInspector interface. For this SerDe, the blob that is passed in is a JSON string, and the Jackson JSON parser
   * is being used to translate the string into Java objects.
   * <p/>
   * The JSON deserialization works by taking the column names in the Hive table, and looking up those fields in the
   * parsed JSON object. If the value of the field is not a primitive, the object is parsed further.
   *
   * @param blob
   *          the blob
   * @return the object
   * @throws SerDeException
   *           the ser de exception
   */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    Map<?, ?> root = null;
    row.clear();
    try {
      ObjectMapper mapper = new ObjectMapper();
      // This is really a Map<String, Object>. For more information about how
      // Jackson parses JSON in this example, see
      // http://wiki.fasterxml.com/JacksonDataBinding
      root = mapper.readValue(blob.toString(), Map.class);
    } catch (Exception e) {
      throw new SerDeException(e);
    }

    // Lowercase the keys as expected by hive
    Map<String, Object> lowerRoot = new HashMap();
    for (Map.Entry entry : root.entrySet()) {
      lowerRoot.put(((String) entry.getKey()).toLowerCase(), entry.getValue());
    }
    root = lowerRoot;

    Object value = null;
    for (String fieldName : rowTypeInfo.getAllStructFieldNames()) {
      try {
        TypeInfo fieldTypeInfo = rowTypeInfo.getStructFieldTypeInfo(fieldName);
        value = parseField(root.get(fieldName), fieldTypeInfo);
      } catch (Exception e) {
        value = null;
      }
      row.add(value);
    }
    return row;
  }

  /**
   * Parses a JSON object according to the Hive column's type.
   *
   * @param field
   *          - The JSON object to parse
   * @param fieldTypeInfo
   *          - Metadata about the Hive column
   * @return - The parsed value of the field
   */
  private Object parseField(Object field, TypeInfo fieldTypeInfo) {
    switch (fieldTypeInfo.getCategory()) {
    case PRIMITIVE:
      // Jackson primitive parsing in case of number is not perfect, so we
      // parse it properly.
      try {
        switch (((PrimitiveTypeInfo) fieldTypeInfo).getPrimitiveCategory()) {
        case DOUBLE:
          if (!(field instanceof Double)) {
            field = new Double(field.toString());
          }
          break;
        case LONG:
          if (!(field instanceof Long)) {
            field = new Long(field.toString());
          }
          break;
        case INT:
          if (!(field instanceof Integer)) {
            field = new Integer(field.toString());
          }
          break;
        case FLOAT:
          if (!(field instanceof Float)) {
            field = new Float(field.toString());
          }
          break;
        case STRING:
          field = field.toString().replaceAll("\n", "\\\\n");
          break;
        }
      } catch (Exception e) {
        field = null;
      }
      return field;
    case LIST:
      return parseList(field, (ListTypeInfo) fieldTypeInfo);
    case MAP:
      return parseMap(field, (MapTypeInfo) fieldTypeInfo);
    case STRUCT:
      return parseStruct(field, (StructTypeInfo) fieldTypeInfo);
    case UNION:
      // Unsupported by JSON
    default:
      return null;
    }
  }

  /**
   * Parses a JSON object and its fields. The Hive metadata is used to determine how to parse the object fields.
   *
   * @param field
   *          - The JSON object to parse
   * @param fieldTypeInfo
   *          - Metadata about the Hive column
   * @return - A map representing the object and its fields
   */
  private Object parseStruct(Object field, StructTypeInfo fieldTypeInfo) {
    Map<Object, Object> map = (Map<Object, Object>) field;
    ArrayList<TypeInfo> structTypes = fieldTypeInfo.getAllStructFieldTypeInfos();
    ArrayList<String> structNames = fieldTypeInfo.getAllStructFieldNames();

    Map<String, Object> structRow = new HashMap<String, Object>();
    if (map != null) {
      for (int i = 0; i < structNames.size(); i++) {
        structRow.put(structNames.get(i), parseField(map.get(structNames.get(i)), structTypes.get(i)));
      }
    }

    return structRow;
  }

  /**
   * Parse a JSON list and its elements. This uses the Hive metadata for the list elements to determine how to parse the
   * elements.
   *
   * @param field
   *          - The JSON list to parse
   * @param fieldTypeInfo
   *          - Metadata about the Hive column
   * @return - A list of the parsed elements
   */
  private Object parseList(Object field, ListTypeInfo fieldTypeInfo) {
    ArrayList<Object> list = (ArrayList<Object>) field;
    TypeInfo elemTypeInfo = fieldTypeInfo.getListElementTypeInfo();
    if (list != null) {
      for (int i = 0; i < list.size(); i++) {
        list.set(i, parseField(list.get(i), elemTypeInfo));
      }
    }
    return list.toArray();
  }

  /**
   * Parse a JSON object as a map. This uses the Hive metadata for the map values to determine how to parse the values.
   * The map is assumed to have a string for a key.
   *
   * @param field
   *          - The JSON list to parse
   * @param fieldTypeInfo
   *          - Metadata about the Hive column
   * @return the object
   */
  private Object parseMap(Object field, MapTypeInfo fieldTypeInfo) {
    Map<Object, Object> map = (Map<Object, Object>) field;
    TypeInfo valueTypeInfo = fieldTypeInfo.getMapValueTypeInfo();
    if (map != null) {
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        map.put(entry.getKey(), parseField(entry.getValue(), valueTypeInfo));
      }
    }
    return map;
  }

  /**
   * Return an ObjectInspector for the row of data
   */
  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  /**
   * Unimplemented
   */
  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  /**
   * JSON is just a textual representation, so our serialized class is just Text.
   */
  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.Serializer#serialize(java.lang.Object,
   * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector)
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
    throw new UnsupportedOperationException("Use Jackson to serialize object");
  }

}
