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

import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * CSVSerde uses opencsv (http://opencsv.sourceforge.net/) to serialize/deserialize columns as CSV.
 *
 */
public final class CSVSerde extends AbstractSerDe {

  /** The default null format. */
  public static String DEFAULT_NULL_FORMAT = "NULL";

  /** The default collection seperator. */
  public static char DEFAULT_COLLECTION_SEPERATOR = ',';

  /** The default struct field seperator. */
  public static char DEFAULT_STRUCT_FIELD_SEPERATOR = ':';

  /** The default union tag field seperator. */
  public static char DEFAULT_UNION_TAG_FIELD_SEPERATOR = ':';

  /** The default map key value seperator. */
  public static char DEFAULT_MAP_KEY_VALUE_SEPERATOR = '=';

  /** The inspector. */
  private ObjectInspector inspector;

  /** The output fields. */
  private String[] outputFields;

  /** The num cols. */
  private int numCols;

  /** The row. */
  private List<Object> row;

  /** The column types. */
  private List<TypeInfo> columnTypes;

  /** The column object inspectors. */
  private List<ObjectInspector> columnObjectInspectors;

  /** The separator char. */
  private char separatorChar;

  /** The quote char. */
  private char quoteChar;

  /** The escape char. */
  private char escapeChar;

  /** The collection seperator. */
  private char collectionSeperator;

  /** The struct field seperator. */
  private char structFieldSeperator;

  /** The union tag field seperator. */
  private char unionTagFieldSeperator;

  /** The map key value seperator. */
  private char mapKeyValueSeperator;

  /** The null string. */
  private String nullString;

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#initialize(org.apache.hadoop.conf.Configuration,
   * java.util.Properties)
   */
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    List<String> columnNames = new ArrayList<String>();
    String[] names = tbl.getProperty(LIST_COLUMNS).split("(?!\"),(?!\")");
    for (String name : names) {
      columnNames.add(StringEscapeUtils.unescapeCsv(name));
    }
    String columnTypeProperty = tbl.getProperty(LIST_COLUMN_TYPES);
    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    numCols = columnNames.size();

    this.outputFields = new String[numCols];
    row = new ArrayList<Object>(numCols);

    for (int i = 0; i < numCols; i++) {
      row.add(null);
    }

    ObjectInspector colObjectInspector;
    columnObjectInspectors = new ArrayList<ObjectInspector>(numCols);
    for (int col = 0; col < numCols; col++) {
      colObjectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(col));
      columnObjectInspectors.add(colObjectInspector);
    }
    this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnObjectInspectors);

    separatorChar = getProperty(tbl, "separatorChar", CSVWriter.DEFAULT_SEPARATOR);
    quoteChar = getProperty(tbl, "quoteChar", CSVWriter.DEFAULT_QUOTE_CHARACTER);
    escapeChar = getProperty(tbl, "escapeChar", CSVWriter.DEFAULT_ESCAPE_CHARACTER);
    nullString = tbl.getProperty("nullString", DEFAULT_NULL_FORMAT);
    collectionSeperator = getProperty(tbl, "collectionSeperator", DEFAULT_COLLECTION_SEPERATOR);
    structFieldSeperator = getProperty(tbl, "structFieldSeperator", DEFAULT_STRUCT_FIELD_SEPERATOR);
    unionTagFieldSeperator = getProperty(tbl, "unionTagFieldSeperator", DEFAULT_UNION_TAG_FIELD_SEPERATOR);
    mapKeyValueSeperator = getProperty(tbl, "mapKeyValueSeperator", DEFAULT_MAP_KEY_VALUE_SEPERATOR);
  }

  /**
   * Gets the property.
   *
   * @param tbl
   *          the tbl
   * @param property
   *          the property
   * @param def
   *          the def
   * @return the property
   */
  private final char getProperty(final Properties tbl, final String property, final char def) {
    final String val = tbl.getProperty(property);

    if (val != null) {
      return val.charAt(0);
    }

    return def;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#serialize(java.lang.Object,
   * org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector)
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();

    if (outputFieldRefs.size() != numCols) {
      throw new SerDeException("Cannot serialize the object because there are " + outputFieldRefs.size()
          + " fields but the table has " + numCols + " columns.");
    }

    try {
      // Get all data out.
      for (int c = 0; c < numCols; c++) {
        final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
        // Get the field objectInspector and the field object.
        ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();

        outputFields[c] = serializeField(field, fieldOI);
      }

      final StringWriter writer = new StringWriter();
      final CSVWriter csv = newWriter(writer, separatorChar, quoteChar, escapeChar);

      csv.writeNext(outputFields);
      csv.close();

      return new Text(writer.toString());
    } catch (final IOException ioe) {
      throw new SerDeException(ioe);
    }
  }

  /**
   * Serialize field.
   *
   * @param field
   *          the field
   * @param fieldOI
   *          the field oi
   * @return the string
   * @throws IOException
   *           Signals that an I/O exception has occurred.
   * @throws SerDeException
   *           the ser de exception
   */
  private String serializeField(Object field, ObjectInspector fieldOI) throws IOException, SerDeException {

    if (field == null) {
      return nullString;
    }

    List<?> list;
    switch (fieldOI.getCategory()) {
    case PRIMITIVE:
      if (fieldOI instanceof StringObjectInspector) {
        final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
        return fieldStringOI.getPrimitiveJavaObject(field);
      } else {
        return field.toString();
      }
    case LIST:
      ListObjectInspector loi = (ListObjectInspector) fieldOI;
      list = loi.getList(field);
      ObjectInspector eoi = loi.getListElementObjectInspector();
      if (list == null) {
        return nullString;
      } else {
        StringBuilder listString = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            listString.append(collectionSeperator);
          }
          listString.append(serializeField(list.get(i), eoi));
        }
        return listString.toString();
      }
    case MAP:
      MapObjectInspector moi = (MapObjectInspector) fieldOI;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();
      Map<?, ?> map = moi.getMap(field);
      if (map == null) {
        return nullString;
      } else {
        StringBuilder mapString = new StringBuilder();
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (first) {
            first = false;
          } else {
            mapString.append(collectionSeperator);
          }
          mapString.append(serializeField(entry.getKey(), koi));
          mapString.append(mapKeyValueSeperator);
          mapString.append(serializeField(entry.getValue(), voi));
        }
        return mapString.toString();
      }
    case STRUCT:
      StructObjectInspector soi = (StructObjectInspector) fieldOI;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      list = soi.getStructFieldsDataAsList(field);
      if (list == null) {
        return nullString;
      } else {
        StringBuilder structString = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            structString.append(structFieldSeperator);
          }
          structString.append(serializeField(list.get(i), fields.get(i).getFieldObjectInspector()));
        }
        return structString.toString();
      }
    case UNION:
      UnionObjectInspector uoi = (UnionObjectInspector) fieldOI;
      List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
      if (ois == null) {
        return nullString;
      } else {
        StringBuilder unionString = new StringBuilder();
        ByteArrayOutputStream tagStream = new ByteArrayOutputStream();
        LazyInteger.writeUTF8(tagStream, uoi.getTag(field));
        unionString.append(new String(tagStream.toByteArray()));
        unionString.append(unionTagFieldSeperator);
        unionString.append(serializeField(uoi.getField(field), ois.get(uoi.getTag(field))));
        return unionString.toString();
      }
    default:
      break;
    }

    throw new RuntimeException("Unknown category type: " + fieldOI.getCategory());
  }

  /**
   * Gets the Java Object corresponding to the type, represented as string.
   *
   * @param colString
   *          the col string
   * @param type
   *          the type
   * @return Standard Java Object for primitive types List of Objects for Array type Map<Object,Object> for Map type
   *         List of Objects for Struct type Object itself contained in Union type
   */
  private Object getColumnObject(String colString, TypeInfo type) {
    if (colString.equals(nullString)) {
      return null;
    }
    switch (type.getCategory()) {
    case PRIMITIVE:
      return ObjectInspectorConverters.getConverter(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(type)).convert(colString);
    case LIST:
      TypeInfo elementType = ((ListTypeInfo) type).getListElementTypeInfo();
      List<Object> olist = new ArrayList<Object>();
      List<String> inlist = Arrays.asList(StringUtils.split(colString, collectionSeperator));
      for (String ins : inlist) {
        olist.add(getColumnObject(ins, elementType));
      }
      return olist;
    case MAP:
      TypeInfo keyType = ((MapTypeInfo) type).getMapKeyTypeInfo();
      TypeInfo valueType = ((MapTypeInfo) type).getMapValueTypeInfo();
      Map<Object, Object> omap = new LinkedHashMap<Object, Object>();
      List<String> maplist = Arrays.asList(StringUtils.split(colString, collectionSeperator));
      for (String ins : maplist) {
        String[] entry = StringUtils.split(ins, mapKeyValueSeperator);
        omap.put(getColumnObject(entry[0], keyType), getColumnObject(entry[1], valueType));
      }
      return omap;
    case STRUCT:
      List<TypeInfo> elementTypes = ((StructTypeInfo) type).getAllStructFieldTypeInfos();
      List<Object> slist = new ArrayList<Object>();
      List<String> instructlist = Arrays.asList(StringUtils.split(colString, structFieldSeperator));
      for (int i = 0; i < elementTypes.size(); i++) {
        slist.add(getColumnObject(instructlist.get(i), elementTypes.get(i)));
      }
      return slist;
    case UNION:
      List<TypeInfo> unionTypes = ((UnionTypeInfo) type).getAllUnionObjectTypeInfos();
      String[] unionElements = StringUtils.split(colString, unionTagFieldSeperator);
      int tag = Integer.parseInt(unionElements[0]);
      return getColumnObject(colString, unionTypes.get(tag));
    }
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.hive.serde2.AbstractSerDe#deserialize(org.apache.hadoop.io.Writable)
   */
  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    Text rowText = (Text) blob;

    CSVReader csv = null;
    try {
      csv = newReader(new CharArrayReader(rowText.toString().toCharArray()), separatorChar, quoteChar, escapeChar);
      final String[] read = csv.readNext();

      for (int i = 0; i < numCols; i++) {
        if (read != null && i < read.length && !read[i].equals(nullString)) {
          row.set(i, getColumnObject(read[i], columnTypes.get(i)));
        } else {
          row.set(i, null);
        }
      }

      return row;
    } catch (final Exception e) {
      throw new SerDeException(e);
    } finally {
      if (csv != null) {
        try {
          csv.close();
        } catch (final Exception e) {
          // ignore
        }
      }
    }
  }

  /**
   * New reader.
   *
   * @param reader
   *          the reader
   * @param separator
   *          the separator
   * @param quote
   *          the quote
   * @param escape
   *          the escape
   * @return the CSV reader
   */
  private CSVReader newReader(final Reader reader, char separator, char quote, char escape) {
    // CSVReader will throw an exception if any of separator, quote, or escape is the same, but
    // the CSV format specifies that the escape character and quote char are the same... very weird
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVReader(reader, separator, quote);
    } else {
      return new CSVReader(reader, separator, quote, escape);
    }
  }

  /**
   * New writer.
   *
   * @param writer
   *          the writer
   * @param separator
   *          the separator
   * @param quote
   *          the quote
   * @param escape
   *          the escape
   * @return the CSV writer
   */
  private CSVWriter newWriter(final Writer writer, char separator, char quote, char escape) {
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVWriter(writer, separator, quote, "");
    } else {
      return new CSVWriter(writer, separator, quote, escape, "");
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  public SerDeStats getSerDeStats() {
    return null;
  }
}
