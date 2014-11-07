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
package org.apache.lens.cube.metadata;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;

public class Dimension extends AbstractBaseTable {

  private final Set<CubeDimAttribute> attributes;
  private final Map<String, CubeDimAttribute> attributeMap;

  public Dimension(String name, Set<CubeDimAttribute> attributes) {
    this(name, attributes, new HashMap<String, String>(), 0L);
  }

  public Dimension(String name, Set<CubeDimAttribute> attributes, Map<String, String> properties, double weight) {
    this(name, attributes, null, properties, weight);
  }

  public Dimension(String name, Set<CubeDimAttribute> attributes, Set<ExprColumn> expressions,
      Map<String, String> properties, double weight) {
    super(name, expressions, properties, weight);
    this.attributes = attributes;

    attributeMap = new HashMap<String, CubeDimAttribute>();
    for (CubeDimAttribute dim : attributes) {
      attributeMap.put(dim.getName().toLowerCase(), dim);
    }

    addProperties();
  }

  public Dimension(Table tbl) {
    super(tbl);
    this.attributes = getAttributes(getName(), getProperties());

    attributeMap = new HashMap<String, CubeDimAttribute>();
    for (CubeDimAttribute attr : attributes) {
      addAllAttributesToMap(attr);
    }
  }

  private void addAllAttributesToMap(CubeDimAttribute attr) {
    attributeMap.put(attr.getName().toLowerCase(), attr);
    if (attr instanceof HierarchicalDimAttribute) {
      for (CubeDimAttribute d : ((HierarchicalDimAttribute) attr).getHierarchy()) {
        addAllAttributesToMap(d);
      }
    }
  }

  public Set<CubeDimAttribute> getAttributes() {
    return attributes;
  }

  @Override
  public CubeTableType getTableType() {
    return CubeTableType.DIMENSION;
  }

  @Override
  public Set<String> getStorages() {
    return null;
  }

  @Override
  public void addProperties() {
    super.addProperties();
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getDimAttributeListKey(getName()), attributes);
    setAttributedProperties(getProperties(), attributes);
  }

  private static void setAttributedProperties(Map<String, String> props, Set<CubeDimAttribute> attributes) {
    for (CubeDimAttribute attr : attributes) {
      attr.addProperties(props);
    }
  }

  public static Set<CubeDimAttribute> getAttributes(String name, Map<String, String> props) {
    Set<CubeDimAttribute> attributes = new HashSet<CubeDimAttribute>();
    String attrStr = MetastoreUtil.getNamedStringValue(props, MetastoreUtil.getDimAttributeListKey(name));
    String[] names = attrStr.split(",");
    for (String attrName : names) {
      String className = props.get(MetastoreUtil.getDimensionClassPropertyKey(attrName));
      CubeDimAttribute attr;
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor;
        constructor = clazz.getConstructor(String.class, Map.class);
        attr = (CubeDimAttribute) constructor.newInstance(new Object[] { attrName, props });
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid dimension", e);
      }
      attributes.add(attr);
    }
    return attributes;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    Dimension other = (Dimension) obj;
    if (this.getAttributes() == null) {
      if (other.getAttributes() != null) {
        return false;
      }
    } else if (!this.getAttributes().equals(other.getAttributes())) {
      return false;
    }
    return true;
  }

  public CubeDimAttribute getAttributeByName(String attr) {
    return attributeMap.get(attr == null ? attr : attr.toLowerCase());
  }

  public CubeColumn getColumnByName(String column) {
    return getAttributeByName(column);
  }

  /**
   * Alters the attribute if already existing or just adds if it is new
   * attribute
   * 
   * @param dimension
   * @throws HiveException
   */
  public void alterAttribute(CubeDimAttribute attribute) throws HiveException {
    if (attribute == null) {
      throw new NullPointerException("Cannot add null attribute");
    }

    // Replace dimension if already existing
    if (attributeMap.containsKey(attribute.getName().toLowerCase())) {
      attributes.remove(getAttributeByName(attribute.getName()));
      LOG.info("Replacing attribute " + getAttributeByName(attribute.getName()) + " with " + attribute);
    }

    attributes.add(attribute);
    attributeMap.put(attribute.getName().toLowerCase(), attribute);
    MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getDimAttributeListKey(getName()), attributes);
    attribute.addProperties(getProperties());
  }

  /**
   * Remove the dimension with name specified
   * 
   * @param attrName
   */
  public void removeAttribute(String attrName) {
    if (attributeMap.containsKey(attrName.toLowerCase())) {
      LOG.info("Removing attribute " + getAttributeByName(attrName));
      attributes.remove(getAttributeByName(attrName));
      attributeMap.remove(attrName.toLowerCase());
      MetastoreUtil.addNameStrings(getProperties(), MetastoreUtil.getDimAttributeListKey(getName()), attributes);
    }
  }

  /**
   * @return the timedDimension
   */
  public String getTimedDimension() {
    return getProperties().get(MetastoreUtil.getDimTimedDimensionKey(getName()));
  }

  public Set<String> getAttributeNames() {
    Set<String> dimNames = new HashSet<String>();
    for (CubeDimAttribute f : getAttributes()) {
      MetastoreUtil.addColumnNames(f, dimNames);
    }
    return dimNames;
  }

  @Override
  public Set<String> getAllFieldNames() {
    Set<String> fieldNames = super.getAllFieldNames();
    fieldNames.addAll(getAttributeNames());
    return fieldNames;
  }

}
