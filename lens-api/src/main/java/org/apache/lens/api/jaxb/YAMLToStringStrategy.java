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

package org.apache.lens.api.jaxb;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.metastore.*;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryPrepareHandle;
import org.apache.lens.api.query.ResultColumn;

import org.jvnet.jaxb2_commons.lang.JAXBToStringStrategy;
import org.jvnet.jaxb2_commons.lang.ToString;
import org.jvnet.jaxb2_commons.lang.ToStringStrategy;
import org.jvnet.jaxb2_commons.locator.ObjectLocator;

import com.google.common.collect.Sets;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class YAMLToStringStrategy extends JAXBToStringStrategy {
  public static final ToStringStrategy INSTANCE = new YAMLToStringStrategy();
  private int indentationValue = -1;
  public static final String INDENTATION_STRING = "  ";
  private boolean insideArray = false;
  private Stack<Boolean> insideArrayStack = new Stack<>();
  public static final Set<Class<?>> WRAPPER_TYPES = Sets.newHashSet(Boolean.class, Character.class, Byte.class,
    Short.class, Integer.class, Long.class, Float.class, Double.class, Void.class, XMLGregorianCalendar.class,
    UUID.class, String.class, Enum.class, QueryHandle.class, QueryPrepareHandle.class, java.util.Date.class);

  {
    insideArrayStack.push(false);
  }

  public StringBuilder indent(StringBuilder buffer) {
    for (int i = 0; i < indentationValue; i++) {
      buffer.append(INDENTATION_STRING);
    }
    return buffer;
  }

  public static String objectToString(ToString object) {
    final StringBuilder buffer = new StringBuilder();
    object.append(null, buffer, INSTANCE);
    return buffer.toString();
  }

  @Override
  public StringBuilder appendStart(ObjectLocator parentLocator, Object object, StringBuilder buffer) {
    indentationValue++;
    return buffer;
  }

  @Override
  public StringBuilder appendEnd(ObjectLocator parentLocator, Object parent, StringBuilder buffer) {
    indentationValue--;
    return buffer;
  }

  protected void appendFieldStart(ObjectLocator parentLocator, Object parent, String fieldName, StringBuilder buffer) {
    if (fieldName != null) {
      indent(buffer).append(fieldName).append(": ");
    }
  }

  interface NameableContext {
    String getHeading();

    void close();

    String getDetails();
  }

  @Data
  class XFieldNameableContext implements NameableContext {
    final XField field;
    private String displayString;
    private String description;
    private String name;
    private String heading;

    @Override
    public String getHeading() {
      StringBuilder singleLineHeading = new StringBuilder();
      if (isNotBlank(field.getDisplayString())) {
        singleLineHeading.append(field.getDisplayString()).append("(").append(field.getName()).append(")");
      } else {
        singleLineHeading.append(field.getName());
      }
      if (isNotBlank(field.getDescription())) {
        singleLineHeading.append(" [").append(field.getDescription()).append("]");
      }
      this.name = field.getName();
      this.description = field.getDescription();
      this.displayString = field.getDisplayString();
      field.setName(null);
      field.setDescription(null);
      field.setDisplayString(null);
      this.heading = singleLineHeading.toString();
      return singleLineHeading.toString() + ":";
    }

    @Override
    public void close() {
      field.setName(this.name);
      field.setDisplayString(this.displayString);
      field.setDescription(this.description);
    }

    @Override
    public String getDetails() {
      try {
        String fieldDetails = YAMLToStringStrategy.super.appendInternal(null, new StringBuilder(), field).toString();
        if (field instanceof XJoinChain) {
          return "\n" + fieldDetails;
        }
        String singleLineDetails = fieldDetails.replaceAll("\\s+$", "").replaceAll("\n\\s+", ", ").trim();
        if (singleLineDetails.length() < 100 && !singleLineDetails.contains("-")) {
          return singleLineDetails;
        }
        return "\n" + fieldDetails;
      } finally {
        close();
      }
    }
  }

  @Data
  class ReflectiveNameableContext implements NameableContext {
    private final Object value;
    private final Method getter, setter;
    private String name;

    @Override
    public String getHeading() {
      try {
        name = (String) getter.invoke(value);
        setter.invoke(value, new Object[]{null});
      } catch (IllegalAccessException | InvocationTargetException e) {
        return "-";
      }
      if (isBlank(name)) {
        return "-";
      }
      return name + ":";
    }

    @Override
    public void close() {
      try {
        setter.invoke(value, new Object[]{name});
      } catch (IllegalAccessException | InvocationTargetException e) {
        return;
      }
    }

    @Override
    public String getDetails() {
      try {
        String fieldDetails = YAMLToStringStrategy.super.appendInternal(null, new StringBuilder(), value).toString();
        String singleLineDetails = fieldDetails.replaceAll("\\s+$", "").replaceAll("\n\\s+", ", ").trim();
        if (singleLineDetails.length() < 100 && !singleLineDetails.contains("-")) {
          return singleLineDetails;
        }
        return "\n" + fieldDetails;
      } finally {
        close();
      }
    }
  }

  public NameableContext getNameableContext(Object value) {
    if (value instanceof XField) {
      return new XFieldNameableContext((XField) value);
    }
    try {
      if (value.getClass().getMethod("getName") != null) {
        return new ReflectiveNameableContext(value, value.getClass().getMethod("getName"),
          value.getClass().getMethod("setName", String.class));
      }
    } catch (NoSuchMethodException e) {
      try {
        if (value.getClass().getMethod("getStorageName") != null) {
          return new ReflectiveNameableContext(value, value.getClass().getMethod("getStorageName"),
            value.getClass().getMethod("setStorageName", String.class));
        }
      } catch (NoSuchMethodException e1) {
        return null;
      }
    }
    return null;
  }

  @Override
  protected StringBuilder appendInternal(ObjectLocator locator, StringBuilder stringBuilder, Object value) {
    insideArray = insideArrayStack.peek();
    insideArrayStack.push(false);
    try {
      if (value instanceof String && ((String) value).isEmpty()) {
        appendNullText(stringBuilder);
        return stringBuilder;
      }
      if (!canBeInlinedWithOtherArrayElements(value)) {
        appendNewLine(stringBuilder);
      }
      if (value instanceof Map) {
        indentationValue++;
        for (Object key : ((Map) value).keySet()) {
          appendNewLine(indent(stringBuilder).append(key).append(": ").append(((Map) value).get(key)));
        }
        indentationValue--;
        return stringBuilder;
      }
      if (value instanceof XProperty || value instanceof XPartSpecElement || value instanceof XTimePartSpecElement
        || value instanceof ResultColumn) {
        removeLastArrayStart(stringBuilder);
        if (value instanceof ResultColumn) {
          ResultColumn column = (ResultColumn) value;
          indent(stringBuilder).append(column.getName()).append(": ").append(column.getType());
        }
        if (value instanceof XProperty) {
          XProperty property = (XProperty) value;
          indent(stringBuilder).append(property.getName()).append(": ").append(property.getValue());
        }
        if (value instanceof XPartSpecElement) {
          XPartSpecElement partSpecElement = (XPartSpecElement) value;
          indent(stringBuilder).append(partSpecElement.getKey()).append(": ").append(partSpecElement.getValue());
        }
        if (value instanceof XTimePartSpecElement) {
          XTimePartSpecElement partSpecElement = (XTimePartSpecElement) value;
          indent(stringBuilder).append(partSpecElement.getKey()).append(": ").append(partSpecElement.getValue());
        }
        return appendNewLine(stringBuilder);
      }

      if (value instanceof XJoinEdge) {
        XJoinEdge edge = (XJoinEdge) value;
        XTableReference from = edge.getFrom();
        XTableReference to = edge.getTo();
        stringBuilder.setLength(stringBuilder.length() - 2);
        stringBuilder
          .append(from.getTable()).append(".").append(from.getColumn()).append(from.isMapsToMany() ? "(many)" : "")
          .append("=")
          .append(to.getTable()).append(".").append(to.getColumn()).append(to.isMapsToMany() ? "(many)" : "");
        return appendNewLine(stringBuilder);
      }
      NameableContext context = getNameableContext(value);
      if (context != null) {
        String heading = context.getHeading();
        if (isBlank(heading)) {
          heading = "-";
        }
        if (insideArray) {
          stringBuilder.setLength(stringBuilder.length() - 2);
        }
        String details = context.getDetails();
        stringBuilder.append(heading);
        if (details.charAt(0) != '\n') {
          stringBuilder.append(" ");
        }
        stringBuilder.append(details);
        return appendNewLine(stringBuilder);
      }
      // some other way of getting heading
      if (value instanceof Collection) {
        Collection collection = (Collection) value;
        // try inline
        StringBuilder allElements = super.appendInternal(locator, new StringBuilder(), value);
        if (collection.size() != 0 && canBeInlinedWithOtherArrayElements((collection.iterator().next()))
          && allElements.length() < 120) {
          stringBuilder.setLength(stringBuilder.length() - 1);
          String sep = " ";
          for (Object singleElement : collection) {
            stringBuilder.append(sep);
            appendInternal(locator, stringBuilder, singleElement);
            sep = ", ";
          }
          return stringBuilder;
        } else {
          return stringBuilder.append(allElements);
        }
      }
      // If this class is just a wrapper over a another object
      Field[] fields = value.getClass().getDeclaredFields();
      int nonStaticFields = 0;
      String fieldName = null;
      for (Field field : fields) {
        if (!Modifier.isStatic(field.getModifiers())) {
          nonStaticFields++;
          fieldName = field.getName();
        }
      }
      if (nonStaticFields == 1) {
        Class<?> claz = value.getClass();
        String getterName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        try {
          Object wrappedValue = claz.getDeclaredMethod(getterName).invoke(value);
          return appendNewLine(appendInternal(locator, stringBuilder, wrappedValue));
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
          log.debug("getter access failed for {}#{}. Going the usual way", claz.getName(), getterName, e);
        }
      }
      return super.appendInternal(locator, stringBuilder, value);
    } finally {
      insideArrayStack.pop();
    }
  }

  private boolean canBeInlinedWithOtherArrayElements(Object value) {
    if (value.getClass().isPrimitive()) {
      return true;
    }
    for (Class<?> clazz : WRAPPER_TYPES) {
      if (clazz.isAssignableFrom(value.getClass())) {
        return true;
      }
    }
    return false;
  }

  private void removeLastArrayStart(StringBuilder stringBuilder) {
    stringBuilder.setLength(stringBuilder.length() - ((indentationValue + 1) * INDENTATION_STRING.length()));
  }

  @Override
  protected void appendFieldEnd(ObjectLocator parentLocator, Object parent, String fieldName, StringBuilder buffer) {
    appendNewLine(buffer);
  }

  private StringBuilder appendNewLine(StringBuilder buffer) {
    if (buffer.length() != 0 && buffer.charAt(buffer.length() - 1) != '\n') {
      while (buffer.charAt(buffer.length() - 1) == ' ') {
        buffer.setLength(buffer.length() - 1);
      }
      buffer.append("\n");
    }
    return buffer;
  }

  @Override
  protected void appendArrayStart(StringBuilder buffer) {
    indentationValue++;
    insideArrayStack.push(true);
    appendArraySeparator(buffer);
  }

  @Override
  protected void appendArrayEnd(StringBuilder buffer) {
    // start takes care
    indentationValue--;
    insideArrayStack.pop();
  }

  @Override
  protected void appendArraySeparator(StringBuilder buffer) {
    indent(buffer).append("-");
  }

  @Override
  protected void appendNullText(StringBuilder buffer) {
    buffer.setLength(buffer.lastIndexOf("\n") + 1);
  }
}
