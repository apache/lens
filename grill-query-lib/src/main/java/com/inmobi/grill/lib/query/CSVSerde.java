package com.inmobi.grill.lib.query;

/*
 * #%L
 * Grill Query Library
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;


/**
* CSVSerde uses opencsv (http://opencsv.sourceforge.net/) 
* to serialize/deserialize columns as CSV.
*
*/
public final class CSVSerde extends AbstractSerDe {

  public static String DEFAULT_NULL_FORMAT = "NULL";
  private ObjectInspector inspector;
  private String[] outputFields;
  private int numCols;
  private List<Object> row;
  List<TypeInfo> columnTypes;
  
  private char separatorChar;
  private char quoteChar;
  private char escapeChar;
  private String nullString;
  private List<Converter> converters = new ArrayList<Converter>();
  private ObjectInspector inputColumnOI;
  
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    final List<String> columnNames = Arrays.asList(tbl.getProperty(LIST_COLUMNS).split(","));    
    String columnTypeProperty = tbl.getProperty(LIST_COLUMN_TYPES);
    columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(
        columnTypeProperty);
    numCols = columnNames.size();
    // input OI for deserialize
    inputColumnOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    
    List<ObjectInspector> columnObjectInspectors = 
        new ArrayList<ObjectInspector>(numCols);
    ObjectInspector colObjectInspector;
    for (int col = 0; col < numCols; col++) {
      colObjectInspector = TypeInfoUtils
          .getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(col));
      columnObjectInspectors.add(colObjectInspector);
      Map<ObjectInspector, Boolean> oiSettableProperties = new HashMap<ObjectInspector, Boolean>();
      ObjectInspector convertedOI = ObjectInspectorConverters.getConvertedOI(inputColumnOI,
          colObjectInspector, oiSettableProperties);
      converters.add(ObjectInspectorConverters.getConverter(inputColumnOI, convertedOI));
    }

    this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnObjectInspectors);
    this.outputFields = new String[numCols];
    row = new ArrayList<Object>(numCols);
    
    for (int i=0; i< numCols; i++) {
      row.add(null);
    }
    
    separatorChar = getProperty(tbl, "separatorChar", CSVWriter.DEFAULT_SEPARATOR);
    quoteChar = getProperty(tbl, "quoteChar", CSVWriter.DEFAULT_QUOTE_CHARACTER);
    escapeChar = getProperty(tbl, "escapeChar", CSVWriter.DEFAULT_ESCAPE_CHARACTER);
    nullString = tbl.getProperty("nullString", DEFAULT_NULL_FORMAT);
  }
  
  private final char getProperty(final Properties tbl, final String property, final char def) {
    final String val = tbl.getProperty(property);
    
    if (val != null) {
      return val.charAt(0);
    }
    
    return def;
  }
  
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();
    
    if (outputFieldRefs.size() != numCols) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has " + numCols + " columns.");
    }

    // Get all data out.
    for (int c = 0; c < numCols; c++) {
      final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
      // Get the field objectInspector and the field object.
      ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();

      if (field == null) {
        outputFields[c] = nullString;
      } else if (fieldOI instanceof StringObjectInspector) {
        final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
        outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field);
      } else {
        outputFields[c] = field.toString();
      }
    }
    
    final StringWriter writer = new StringWriter();
    final CSVWriter csv = newWriter(writer, separatorChar, quoteChar, escapeChar);
    
    try {
      csv.writeNext(outputFields);
      csv.close();
      
      return new Text(writer.toString());
    } catch (final IOException ioe) {
      throw new SerDeException(ioe);
    }
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    Text rowText = (Text) blob;
    
    CSVReader csv = null;
    try {
      csv = newReader(new CharArrayReader(rowText.toString().toCharArray()),
          separatorChar, quoteChar, escapeChar);
      final String[] read = csv.readNext();

      for (int i=0; i< numCols; i++) {
        if (read != null && i < read.length && !read[i].equals(nullString)) {
          row.set(i, converters.get(i).convert(read[i]));
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

  private CSVReader newReader(final Reader reader, char separator, char quote, char escape) {
    // CSVReader will throw an exception if any of separator, quote, or escape is the same, but
    // the CSV format specifies that the escape character and quote char are the same... very weird
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVReader(reader, separator, quote);
    } else {
      return new CSVReader(reader, separator, quote, escape);
    }
  }
  
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
