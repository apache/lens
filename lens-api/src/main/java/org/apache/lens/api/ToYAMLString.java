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
package org.apache.lens.api;


import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

import org.apache.lens.api.jaxb.YAMLToStringStrategy;

import org.jvnet.jaxb2_commons.lang.ToString;
import org.jvnet.jaxb2_commons.lang.ToStringStrategy;
import org.jvnet.jaxb2_commons.locator.ObjectLocator;

import com.google.common.collect.Lists;
import lombok.Data;

public abstract class ToYAMLString implements ToString {
  @Data(staticConstructor = "of")
  public static class FieldNameAndValue {
    final String name;
    final Object value;
  }

  public String toString() {
    final ToStringStrategy strategy = new YAMLToStringStrategy();
    final StringBuilder buffer = new StringBuilder();
    append(null, buffer, strategy);
    return buffer.toString();
  }

  public StringBuilder append(ObjectLocator locator, StringBuilder buffer, ToStringStrategy strategy) {
    if (getFieldsToAppend().size() == 1) {
      Object field = getFieldsToAppend().iterator().next().getValue();
      strategy.append(locator, buffer, field);
      return buffer;
    }
    strategy.appendStart(locator, this, buffer);
    appendFields(locator, buffer, strategy);
    strategy.appendEnd(locator, this, buffer);
    return buffer;
  }

  Collection<FieldNameAndValue> fieldsToAppend = null;

  public Collection<FieldNameAndValue> getFieldsToAppend() {
    if (fieldsToAppend == null) {
      List<FieldNameAndValue> fieldNameAndValueList = Lists.newArrayList();
      for (Field field : this.getClass().getDeclaredFields()) {
        try {
          Method getter = getGetter(field);
          fieldNameAndValueList.add(new FieldNameAndValue(getReadableName(getter), getter.invoke(this)));
        } catch (IllegalAccessException | InvocationTargetException e) {
          throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
          //pass. The field doesn't have getter.
        }
      }
      fieldsToAppend = fieldNameAndValueList;
    }
    return fieldsToAppend;
  }

  @Override
  public StringBuilder appendFields(ObjectLocator objectLocator, StringBuilder stringBuilder, ToStringStrategy
    toStringStrategy) {
    for (FieldNameAndValue fieldNameAndValue : fieldsToAppend) {
      toStringStrategy.appendField(objectLocator, this, fieldNameAndValue.getName(), stringBuilder,
        fieldNameAndValue.getValue());
    }
    return stringBuilder;
  }

  private String getReadableName(Method method) {
    return method.getName().replaceAll("^get", "").replaceAll("^is", "Is").replaceAll("([a-z])(?=[A-Z])", "$1 ");
  }

  private Method getGetter(Field field) throws NoSuchMethodException {
    String getterName;
    if (field.getType().isAssignableFrom(Boolean.class) || field.getType().equals(Boolean.TYPE)) {
      getterName = field.getName();
      if (!getterName.startsWith("is")) {
        getterName = "is" + toPascalCase(getterName);
      }
    } else {
      getterName = "get" + toPascalCase(field.getName());
    }
    return this.getClass().getDeclaredMethod(getterName);
  }

  private String toPascalCase(String str) {
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }

  public String toXMLString() {
    return ToXMLString.toString(this);
  }

  public static <T> T fromXMLString(String xml, Class<? extends T> clazz) {
    return ToXMLString.valueOf(xml, clazz);
  }
}
