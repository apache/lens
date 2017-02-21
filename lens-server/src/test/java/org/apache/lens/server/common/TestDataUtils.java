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

package org.apache.lens.server.common;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.lens.api.metastore.*;

import org.joda.time.DateTime;

import com.google.common.base.Optional;

public class TestDataUtils {

  public static final String MOCK_STACK_TRACE = "mock-stackTrace";

  protected TestDataUtils() {
    throw new UnsupportedOperationException("TestDataUtils only have static methods, "
        + "there is no need to create and instance.");
  }

  public static String getRandomName() {
    return java.util.UUID.randomUUID().toString().replaceAll("-", "");
  }

  public static String getRandomDbName() {
    return "db" + getRandomName();
  }

  public static String getRandomStorageName() {
    return "storage" + getRandomName();
  }

  public static String getRandomCubeName() {
    return "cube" + getRandomName();
  }

  public static String getRandomFactName() {
    return "fact" + getRandomName();
  }

  public static String getRandomDimensionField() {
    return "dimfield" + getRandomName();
  }

  public static XCube createXCubeWithDummyMeasure(final String cubeName, final Optional<String> dtColName,
      final XDimAttribute... dimensions) {

    XDimAttributes xDimAttributes = createXDimAttributes(dimensions);
    XMeasures xMeasures = createXMeasures(createDummyXMeasure());

    return createXCubeWithDummyMeasure(cubeName, dtColName, xDimAttributes, xMeasures);
  }

  public static XCube createXCubeWithDummyMeasure(final String cubeName, final Optional<String> dateColName,
      final XDimAttributes xDimAttributes, final XMeasures xMeasures) {

    XBaseCube cube = new XBaseCube();
    cube.setName(cubeName);
    cube.setDimAttributes(xDimAttributes);
    cube.setMeasures(xMeasures);

    if (dateColName.isPresent()) {

      XProperty xp = new XProperty();
      xp.setName("cube."+cubeName+".timed.dimensions.list");
      xp.setValue(dateColName.get());

      XProperties xProperties = new XProperties();
      xProperties.getProperty().add(xp);
      cube.setProperties(xProperties);
    }
    return cube;
  }
  public static XDimAttribute createXDimAttribute(final String name, final Optional<DateTime> startDate,
      final Optional<DateTime> endDate) throws DatatypeConfigurationException {

    XDimAttribute xDimAttribute = new XDimAttribute();
    xDimAttribute.setName(name);
    xDimAttribute.setType("STRING");

    if (startDate.isPresent()) {
      xDimAttribute.setStartTime(createXMLGregorianCalendar(startDate.get().toDate()));
    }
    if (endDate.isPresent()) {
      xDimAttribute.setEndTime(createXMLGregorianCalendar(endDate.get().toDate()));
    }
    return xDimAttribute;
  }

  public static XMeasure createDummyXMeasure() {

    XMeasure xm = new XMeasure();
    xm.setName("dummy");
    xm.setType(XMeasureType.DOUBLE);
    return xm;
  }

  public static XMLGregorianCalendar createXMLGregorianCalendar(final Date time) throws DatatypeConfigurationException {
    GregorianCalendar c = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    c.setTime(time);
    return DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
  }

  public static XDimAttributes createXDimAttributes(final XDimAttribute... vargs) {

    XDimAttributes xDimAttributes = new XDimAttributes();

    for (XDimAttribute xDimAttribute : vargs) {
      xDimAttributes.getDimAttribute().add(xDimAttribute);
    }
    return xDimAttributes;
  }

  public static XMeasures createXMeasures(final XMeasure... vargs) {

    XMeasures xMeasures = new XMeasures();

    for (XMeasure xMeasure : vargs) {
      xMeasures.getMeasure().add(xMeasure);
    }
    return xMeasures;
  }

  public static XFactTable createXFactTableWithColumns(final String factName, final String cubeName,
      final XColumn... xColumnList) {

    XColumns xCols = createXColumns(xColumnList);

    XFactTable xFactTable = new XFactTable();
    xFactTable.setColumns(xCols);
    xFactTable.setName(factName);
    xFactTable.setCubeName(cubeName);
    xFactTable.setWeight(0.0);

    return xFactTable;
  }

  public static XColumns createXColumns(final XColumn... xColumnList) {

    XColumns xColumns = new XColumns();

    for (XColumn xColumn : xColumnList) {
      xColumns.getColumn().add(xColumn);
    }
    return xColumns;
  }

  public static XColumn createXColumn(final String colName) {

    XColumn xCol = new XColumn();
    xCol.setName(colName);
    xCol.setType("STRING");
    return xCol;
  }
}
