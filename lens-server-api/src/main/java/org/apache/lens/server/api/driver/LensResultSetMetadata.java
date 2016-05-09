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
package org.apache.lens.server.api.driver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lens.api.query.QueryResultSetMetadata;
import org.apache.lens.api.query.ResultColumn;

import org.apache.hadoop.hive.serde2.thrift.Type;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.hive.service.cli.TypeDescriptor;

import org.codehaus.jackson.*;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * The Class LensResultSetMetadata.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@class")
public abstract class LensResultSetMetadata {

  protected static final ObjectMapper MAPPER;

  /**
   * Registering custom serializer
   */
  static {
    MAPPER = new ObjectMapper();
    SimpleModule module = new SimpleModule("HiveColumnModule", new Version(1, 0, 0, null));
    module.addSerializer(ColumnDescriptor.class, new JsonSerializer<ColumnDescriptor>() {
      @Override
      public void serialize(ColumnDescriptor columnDescriptor, JsonGenerator jsonGenerator,
                            SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("name", columnDescriptor.getName());
        jsonGenerator.writeStringField("comment", columnDescriptor.getComment());
        jsonGenerator.writeNumberField("position", columnDescriptor.getOrdinalPosition());
        jsonGenerator.writeStringField("type", columnDescriptor.getType().getName());
        jsonGenerator.writeEndObject();
      }
    });
    module.addDeserializer(ColumnDescriptor.class, new JsonDeserializer<ColumnDescriptor>() {
      @Override
      public ColumnDescriptor deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);

        Type t = Type.getType(node.get("type").asText());
        return new ColumnDescriptor(node.get("name").asText(), node.get("comment").asText(), new TypeDescriptor(t),
          node.get("position").asInt());
      }
    });
    MAPPER.registerModule(module);
  }

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
   * @param typeDesc the type desc
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
    } else if (typeDesc.getType().isComplexType()) {
      switch (typeDesc.getType()) {
      case ARRAY_TYPE:
      case MAP_TYPE:
      case STRUCT_TYPE:
        return "string";
      }
    }
    return typeDesc.getTypeName().toLowerCase();
  }

  public static LensResultSetMetadata fromJson(String json) throws IOException {
    return MAPPER.readValue(json, LensResultSetMetadata.class);
  }

  public String toJson() throws IOException {
    return MAPPER.writeValueAsString(this);
  }
}
