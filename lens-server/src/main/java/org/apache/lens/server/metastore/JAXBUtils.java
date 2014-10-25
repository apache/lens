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
package org.apache.lens.server.metastore;

import org.apache.lens.api.metastore.*;
import org.apache.lens.cube.metadata.*;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.ws.rs.WebApplicationException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;

/**
 * Utilities for converting to and from JAXB types to hive.ql.metadata.cube types
 */
public class JAXBUtils {
  public static final Logger LOG = LogManager.getLogger(JAXBUtils.class);
  private static final ObjectFactory XCF = new ObjectFactory();

  /**
   * Create a hive ql cube obejct from corresponding JAXB object
   *
   * @param cube JAXB Cube
   *
   * @return {@link Cube}
   * @throws ParseException
   */
  public static CubeInterface hiveCubeFromXCube(XCube cube, Cube parent) throws ParseException {
    if (cube.isDerived()) {
      Set<String> dims = new LinkedHashSet<String>();
      dims.addAll(cube.getDimAttrNames().getDimAttrNames());

      Set<String> measures = new LinkedHashSet<String>();
      measures.addAll(cube.getMeasureNames().getMeasures());

      Map<String, String> properties = mapFromXProperties(cube.getProperties());
      double cubeWeight = cube.getWeight() == null ? 0d : cube.getWeight();
      return new DerivedCube(cube.getName(), measures, dims, properties, cubeWeight, parent);
    } else {
      Set<CubeDimAttribute> dims = new LinkedHashSet<CubeDimAttribute>();
      for (XDimAttribute xd : cube.getDimAttributes().getDimAttributes()) {
        dims.add(hiveDimAttrFromXDimAttr(xd));
      }

      Set<CubeMeasure> measures = new LinkedHashSet<CubeMeasure>();
      for (XMeasure xm : cube.getMeasures().getMeasures()) {
        measures.add(hiveMeasureFromXMeasure(xm));
      }

      Set<ExprColumn> expressions = new LinkedHashSet<ExprColumn>();
      if (cube.getExpressions() != null) {
        for (XExprColumn xe : cube.getExpressions().getExpressions()) {
          expressions.add(hiveExprColumnFromXExprColumn(xe));
        }
      }

      Map<String, String> properties = mapFromXProperties(cube.getProperties());
      double cubeWeight = cube.getWeight() == null ? 0d : cube.getWeight();
      return new Cube(cube.getName(), measures, dims, expressions, properties, cubeWeight);
    }
  }

  /**
   * Get XCube from hive.ql.metadata.Cube
   *
   * @param c
   *
   * @return {@link XCube}
   */
  public static XCube xCubeFromHiveCube(CubeInterface c) {

    XCube xc = XCF.createXCube();
    xc.setName(c.getName());
    xc.setWeight(((AbstractCubeTable)c).weight());
    xc.setProperties(xPropertiesFromMap(((AbstractCubeTable)c).getProperties()));

    if (c.isDerivedCube()) {
      xc.setMeasureNames(XCF.createXMeasureNames());
      xc.getMeasureNames().getMeasures().addAll(c.getMeasureNames());
      xc.setDimAttrNames(XCF.createXDimAttrNames());
      xc.getDimAttrNames().getDimAttrNames().addAll(c.getDimAttributeNames());
      xc.setParent(((DerivedCube)c).getParent().getName());
      xc.setDerived(true);
    } else {
      XMeasures xms = XCF.createXMeasures();
      List<XMeasure> xmsList = xms.getMeasures();
      for (CubeMeasure cm : c.getMeasures()) {
        xmsList.add(xMeasureFromHiveMeasure(cm));
      }
      xc.setMeasures(xms);

      XExpressions xexprs = XCF.createXExpressions();
      List<XExprColumn> xexprList = xexprs.getExpressions();
      for (ExprColumn ec : c.getExpressions()) {
        xexprList.add(xExprColumnFromHiveExprColumn(ec));
      }
      xc.setExpressions(xexprs);

      XDimAttributes xdm = XCF.createXDimAttributes();
      List<XDimAttribute> xdmList = xdm.getDimAttributes();
      for (CubeDimAttribute cd : c.getDimAttributes()) {
        xdmList.add(xDimAttrFromHiveDimAttr(cd));
      }
      xc.setDimAttributes(xdm);
    }
    return xc;
  }

  /**
   * Create a hive ql CubeDimension from JAXB counterpart
   *
   * @param xd
   *
   * @return {@link CubeDimension}
   */
  public static CubeDimAttribute hiveDimAttrFromXDimAttr(XDimAttribute xd) {
    Date startDate = getDateFromXML(xd.getStartTime());
    Date endDate = getDateFromXML(xd.getEndTime());

    CubeDimAttribute hiveDim;

    if (xd.getReferences() != null && xd.getReferences().getTableReferences() != null &&
        !xd.getReferences().getTableReferences().isEmpty()) {

      List<TableReference> dimRefs = new ArrayList<TableReference>(xd.getReferences().getTableReferences().size());

      for (XTablereference xRef : xd.getReferences().getTableReferences()) {
        dimRefs.add(new TableReference(xRef.getDestTable(), xRef.getDestColumn()));
      }

      hiveDim = new ReferencedDimAtrribute(new FieldSchema(xd.getName(), xd.getType(), xd.getDescription()),
          xd.getDisplayString(),
          dimRefs,
          startDate,
          endDate,
          xd.getCost()
          );
    } else {
      hiveDim = new BaseDimAttribute(new FieldSchema(xd.getName(), xd.getType(), xd.getDescription()),
          xd.getDisplayString(),
          startDate,
          endDate,
          xd.getCost()
          );
    }

    return hiveDim;
  }

  public static XMLGregorianCalendar getXMLGregorianCalendar(Date d) {
    if (d == null) {
      return null;
    }

    GregorianCalendar c1 = new GregorianCalendar();
    c1.setTime(d);
    try {
      return DatatypeFactory.newInstance().newXMLGregorianCalendar(c1);
    } catch (DatatypeConfigurationException e) {
      LOG.warn("Error converting date " + d, e);
      return null;
    }
  }

  public static Date getDateFromXML(XMLGregorianCalendar cal) {
    if (cal == null) {
      return null;
    }
    return cal.toGregorianCalendar().getTime();
  }

  /**
   * Create XMeasure from hive ql cube measure
   */
  public static XMeasure xMeasureFromHiveMeasure(CubeMeasure cm) {
    if (cm == null) {
      return null;
    }

    XMeasure xm = XCF.createXMeasure();
    xm.setName(cm.getName());
    xm.setDescription(cm.getDescription());
    xm.setDisplayString(cm.getDisplayString());
    xm.setDefaultAggr(cm.getAggregate());
    xm.setFormatString(cm.getFormatString());
    xm.setType(cm.getType());
    xm.setUnit(cm.getUnit());
    xm.setCost(cm.getCost());
    xm.setStartTime(getXMLGregorianCalendar(cm.getStartTime()));
    xm.setEndTime(getXMLGregorianCalendar(cm.getEndTime()));
    return xm;
  }

  /**
   * Create XExprColumn from hive ExprColum
   */
  public static XExprColumn xExprColumnFromHiveExprColumn(ExprColumn ec) {
    if (ec == null) {
      return null;
    }

    XExprColumn xe = XCF.createXExprColumn();
    xe.setName(ec.getName());
    xe.setType(ec.getType());
    xe.setDescription(ec.getDescription());
    xe.setDisplayString(ec.getDisplayString());
    xe.setExpr(ec.getExpr());
    return xe;
  }

  /**
   * Create XDimension from hive ql cube dimension
   */
  public static XDimAttribute xDimAttrFromHiveDimAttr(CubeDimAttribute cd) {
    XDimAttribute xd = XCF.createXDimAttribute();
    xd.setName(cd.getName());
    xd.setDescription(cd.getDescription());
    xd.setDisplayString(cd.getDisplayString());

    if (cd instanceof ReferencedDimAtrribute) {
      ReferencedDimAtrribute rd = (ReferencedDimAtrribute) cd;
      List<TableReference> dimRefs = rd.getReferences();
      xd.setReferences(xTabReferenceFromHiveTabReference(dimRefs));
      xd.setType(rd.getType());
      xd.setCost(rd.getCost());
    } else if (cd instanceof BaseDimAttribute) {
      BaseDimAttribute bd = (BaseDimAttribute) cd;
      xd.setType(bd.getType());
      xd.setCost(bd.getCost());
    }
    return xd;
  }

  public static XTablereferences xTabReferenceFromHiveTabReference(List<TableReference> hiveRefs) {
    XTablereferences xrefs = XCF.createXTablereferences();
    List<XTablereference> xrefList = xrefs.getTableReferences();

    for (TableReference hRef : hiveRefs) {
      XTablereference xref = XCF.createXTablereference();
      xref.setDestTable(hRef.getDestTable());
      xref.setDestColumn(hRef.getDestColumn());
      xrefList.add(xref);
    }
    return xrefs;
  }

  /**
   * Create hive ql CubeMeasure from JAXB counterpart
   *
   * @param xm
   *
   * @return {@link CubeMeasure}
   */
  public static CubeMeasure hiveMeasureFromXMeasure(XMeasure xm) {
    Date startDate = xm.getStartTime() == null ? null : xm.getStartTime().toGregorianCalendar().getTime();
    Date endDate = xm.getEndTime() == null ? null : xm.getEndTime().toGregorianCalendar().getTime();
    CubeMeasure cm = new ColumnMeasure(new FieldSchema(xm.getName(), xm.getType(), xm.getDescription()),
        xm.getDisplayString(),
        xm.getFormatString(),
        xm.getDefaultAggr(),
        "unit",
        startDate,
        endDate,
        xm.getCost()
        );
    return cm;
  }

  public static ExprColumn hiveExprColumnFromXExprColumn(XExprColumn xe) throws ParseException {
    ExprColumn ec = new ExprColumn(new FieldSchema(xe.getName(), xe.getType(), xe.getDescription()),
        xe.getDisplayString(),
        xe.getExpr());
    return ec;
  }

  /**
   * Convert JAXB properties to Map<String, String>
   *
   * @param xProperties
   *
   * @return {@link Map}
   */
  public static Map<String, String> mapFromXProperties(XProperties xProperties) {
    Map<String, String> properties = new HashMap<String, String>();
    if (xProperties != null && xProperties.getProperties() != null &&
        !xProperties.getProperties().isEmpty()) {
      for (XProperty xp : xProperties.getProperties()) {
        properties.put(xp.getName(), xp.getValue());
      }
    }
    return properties;
  }

  /**
   * Convert string map to XProperties
   */
  public static XProperties xPropertiesFromMap(Map<String, String> map) {
    if (map != null && !map.isEmpty()) {
      XProperties xp = XCF.createXProperties();
      List<XProperty> xpList = xp.getProperties();
      for (Map.Entry<String, String> e : map.entrySet()) {
        XProperty property = XCF.createXProperty();
        property.setName(e.getKey());
        property.setValue(e.getValue());
        xpList.add(property);
      }

      return xp;
    }
    return null;
  }

  public static FieldSchema fieldSchemaFromColumn(Column c) {
    if (c == null) {
      return null;
    }

    return new FieldSchema(c.getName(), c.getType(), c.getComment());
  }

  public static Column columnFromFieldSchema(FieldSchema fs) {
    if (fs == null) {
      return null;
    }
    Column c = XCF.createColumn();
    c.setName(fs.getName());
    c.setType(fs.getType());
    c.setComment(fs.getComment());
    return c;
  }

  public static ArrayList<FieldSchema> fieldSchemaListFromColumns(Columns columns) {
    if (columns != null && columns.getColumns() != null && !columns.getColumns().isEmpty()) {
      ArrayList<FieldSchema> fsList = new ArrayList<FieldSchema>(columns.getColumns().size());
      for (Column c : columns.getColumns()) {
        LOG.debug("##Column "+ c.getName() + "-" + c.getType());
        fsList.add(fieldSchemaFromColumn(c));
      }
      return fsList;
    }
    return null;
  }

  public static Columns columnsFromFieldSchemaList(List<FieldSchema> fslist) {
    if (fslist == null || fslist.isEmpty()) {
      return null;
    }

    Columns cols = XCF.createColumns();
    for (FieldSchema fs : fslist) {
      cols.getColumns().add(columnFromFieldSchema(fs));
    }
    return cols;
  }

  public static Map<String, Set<UpdatePeriod>> getFactUpdatePeriodsFromUpdatePeriods(UpdatePeriods periods) {
    if (periods != null && periods.getUpdatePeriodElement() != null && !periods.getUpdatePeriodElement().isEmpty()) {
      Map<String, Set<UpdatePeriod>> map = new LinkedHashMap<String, Set<UpdatePeriod>>();

      for (UpdatePeriodElement upd : periods.getUpdatePeriodElement()) {
        Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
        for (String updStr : upd.getUpdatePeriods()) {
          updatePeriods.add(UpdatePeriod.valueOf(updStr.toUpperCase()));
        }
        map.put(upd.getStorageName(), updatePeriods);
      }
      return map;
    }
    return null;
  }

  public static Map<String, UpdatePeriod> dumpPeriodsFromUpdatePeriods(UpdatePeriods periods) {
    if (periods != null && periods.getUpdatePeriodElement() != null && !periods.getUpdatePeriodElement().isEmpty()) {
      Map<String, UpdatePeriod> map = new LinkedHashMap<String, UpdatePeriod>();

      for (UpdatePeriodElement upd : periods.getUpdatePeriodElement()) {
        UpdatePeriod dumpPeriod = null;
        if (upd.getUpdatePeriods().size() > 0) {
          dumpPeriod =  UpdatePeriod.valueOf(upd.getUpdatePeriods().get(0).toUpperCase());
        }
        map.put(upd.getStorageName(), dumpPeriod);
      }
      return map;
    }
    return null;
  }

  public static Storage storageFromXStorage(XStorage xs) {
    if (xs == null) {
      return null;
    }

    Storage storage = null;
    try {
      Class<?> clazz = Class.forName(xs.getClassname());
      Constructor<?> constructor = clazz.getConstructor(String.class);
      storage = (Storage) constructor.newInstance(new Object[]{xs.getName()});
      storage.addProperties(mapFromXProperties(xs.getProperties()));
      return storage;
    } catch (Exception e) {
      LOG.error("Could not create storage class" + xs.getClassname() + "with name:" + xs.getName());
      throw new WebApplicationException(e);
    }
  }

  public static XStorage xstorageFromStorage(Storage storage) {
    if (storage == null) {
      return null;
    }

    XStorage xstorage = null;
    xstorage = XCF.createXStorage();
    xstorage.setName(storage.getName());
    xstorage.setClassname(storage.getClass().getCanonicalName());
    xstorage.setProperties(xPropertiesFromMap(storage.getProperties()));
    return xstorage;
  }

  public static DimensionTable dimTableFromCubeDimTable(CubeDimensionTable cubeDimTable) {
    if (cubeDimTable == null) {
      return null;
    }

    DimensionTable dimTab = XCF.createDimensionTable();
    dimTab.setDimName(cubeDimTable.getDimName());
    dimTab.setTableName(cubeDimTable.getName());
    dimTab.setWeight(cubeDimTable.weight());

    Columns cols = XCF.createColumns();

    for (FieldSchema column : cubeDimTable.getColumns()) {
      cols.getColumns().add(columnFromFieldSchema(column));
    }
    dimTab.setColumns(cols);

    dimTab.setProperties(xPropertiesFromMap(cubeDimTable.getProperties()));

    Map<String, UpdatePeriod> storageToUpdatePeriod = cubeDimTable.getSnapshotDumpPeriods();
    if (storageToUpdatePeriod != null && !storageToUpdatePeriod.isEmpty()) {
      UpdatePeriods periods = XCF.createUpdatePeriods();

      for (Entry<String, UpdatePeriod> entry : storageToUpdatePeriod.entrySet()) {
        UpdatePeriodElement e = XCF.createUpdatePeriodElement();
        e.setStorageName(entry.getKey());
        if (entry.getValue() != null) {
          e.getUpdatePeriods().add(entry.getValue().toString());
        }
        periods.getUpdatePeriodElement().add(e);
      }
      dimTab.setStorageDumpPeriods(periods);
    }

    return dimTab;
  }

  public static List<? extends XTablereference> dimRefListFromTabRefList(
      List<TableReference> tabRefs) {
    if (tabRefs != null && !tabRefs.isEmpty()) {
      List<XTablereference> xTabRefs = new ArrayList<XTablereference>(tabRefs.size());
      for (TableReference ref : tabRefs) {
        XTablereference xRef = XCF.createXTablereference();
        xRef.setDestColumn(ref.getDestColumn());
        xRef.setDestTable(ref.getDestTable());
        xTabRefs.add(xRef);
      }
      return xTabRefs;
    }

    return null;
  }

  public static CubeDimensionTable cubeDimTableFromDimTable(DimensionTable dimensionTable) {

    CubeDimensionTable cdim = new CubeDimensionTable(dimensionTable.getDimName(),
        dimensionTable.getTableName(),
        fieldSchemaListFromColumns(dimensionTable.getColumns()),
        dimensionTable.getWeight(),
        dumpPeriodsFromUpdatePeriods(dimensionTable.getStorageDumpPeriods()),
        mapFromXProperties(dimensionTable.getProperties()));

    return cdim;
  }

  public static CubeFactTable cubeFactFromFactTable(FactTable fact) {
    List<FieldSchema> columns = fieldSchemaListFromColumns(fact.getColumns());

    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = getFactUpdatePeriodsFromUpdatePeriods(fact.getStorageUpdatePeriods());

    return new CubeFactTable(fact.getCubeName(),
        fact.getName(),
        columns,
        storageUpdatePeriods,
        fact.getWeight(),
        mapFromXProperties(fact.getProperties()));
  }

  public static FactTable factTableFromCubeFactTable(CubeFactTable cFact) {
    FactTable fact = XCF.createFactTable();
    fact.setName(cFact.getName());
    fact.setProperties(xPropertiesFromMap(cFact.getProperties()));
    fact.setColumns(columnsFromFieldSchemaList(cFact.getColumns()));
    fact.setWeight(cFact.weight());
    fact.setCubeName(cFact.getCubeName());

    if (cFact.getUpdatePeriods() != null && !cFact.getUpdatePeriods().isEmpty()) {
      UpdatePeriods periods = XCF.createUpdatePeriods();
      for (Map.Entry<String, Set<UpdatePeriod>> e : cFact.getUpdatePeriods().entrySet()) {
        UpdatePeriodElement uel = XCF.createUpdatePeriodElement();
        uel.setStorageName(e.getKey());
        for (UpdatePeriod p : e.getValue()) {
          uel.getUpdatePeriods().add(p.toString());
        }
        periods.getUpdatePeriodElement().add(uel);
      }
      fact.setStorageUpdatePeriods(periods);
    }
    return fact;
  }

  public static StorageTableDesc storageTableDescFromXStorageTableDesc(
      XStorageTableDesc xtableDesc) {
    StorageTableDesc tblDesc = new StorageTableDesc();
    tblDesc.setTblProps(mapFromXProperties(xtableDesc.getTableParameters()));
    tblDesc.setSerdeProps(mapFromXProperties(xtableDesc.getSerdeParameters()));
    tblDesc.setPartCols(fieldSchemaListFromColumns(xtableDesc.getPartCols()));
    tblDesc.setTimePartCols(xtableDesc.getTimePartCols());
    tblDesc.setExternal(xtableDesc.isExternal());
    tblDesc.setLocation(xtableDesc.getTableLocation());
    tblDesc.setInputFormat(xtableDesc.getInputFormat());
    tblDesc.setOutputFormat(xtableDesc.getOutputFormat());
    tblDesc.setFieldDelim(xtableDesc.getFieldDelimiter());
    tblDesc.setFieldEscape(xtableDesc.getEscapeChar());
    tblDesc.setCollItemDelim(xtableDesc.getCollectionDelimiter());
    tblDesc.setLineDelim(xtableDesc.getLineDelimiter());
    tblDesc.setMapKeyDelim(xtableDesc.getMapKeyDelimiter());
    tblDesc.setSerName(xtableDesc.getSerdeClassName());
    tblDesc.setStorageHandler(xtableDesc.getStorageHandlerName());
    return tblDesc;
  }

  public static StorageTableDesc storageTableDescFromXStorageTableElement(
      XStorageTableElement storageTableElement) {
    return storageTableDescFromXStorageTableDesc(storageTableElement.getTableDesc());
  }

  public static XStorageTableElement getXStorageTableFromHiveTable(Table tbl) {
    XStorageTableElement tblElement = new XStorageTableElement();
    tblElement.setTableDesc(getStorageTableDescFromHiveTable(tbl));
    return tblElement;
  }

  public static XStorageTableDesc getStorageTableDescFromHiveTable(Table tbl) {
    XStorageTableDesc tblDesc = new XStorageTableDesc();
    tblDesc.setPartCols(columnsFromFieldSchemaList(tbl.getPartCols()));
    String timePartCols = tbl.getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    if (timePartCols != null) {
      tblDesc.getTimePartCols().addAll(Arrays.asList(org.apache.commons.lang.StringUtils.split(timePartCols, ",")));
    }
    tblDesc.setNumBuckets(tbl.getNumBuckets());
    tblDesc.getBucketCols().addAll(tbl.getBucketCols());
    List<String> sortCols = new ArrayList<String>();
    List<Integer> sortOrders = new ArrayList<Integer>();
    for (Order order : tbl.getSortCols()) {
      sortCols.add(order.getCol());
      sortOrders.add(order.getOrder());
    }
    tblDesc.getSortCols().addAll(sortCols);
    tblDesc.getSortColOrder().addAll(sortOrders);

    XSkewedInfo xskewinfo = new XSkewedInfo();
    xskewinfo.getColNames().addAll(tbl.getSkewedColNames());
    for (List<String> value : tbl.getSkewedColValues()) {
      XStringList slist = new XStringList();
      slist.getElements().addAll(value);
      xskewinfo.getColValues().add(slist);
      XSkewedValueLocation valueLocation = new XSkewedValueLocation();
      if (tbl.getSkewedColValueLocationMaps().get(value) != null) {
        valueLocation.setValue(slist);
        valueLocation.setLocation(tbl.getSkewedColValueLocationMaps().get(value));
        xskewinfo.getValueLocationMap().add(valueLocation);
      }
    }

    tblDesc.setTableParameters(xPropertiesFromMap(tbl.getParameters()));
    tblDesc.setSerdeParameters(xPropertiesFromMap(tbl.getTTable().getSd().getSerdeInfo().getParameters()));
    tblDesc.setExternal(tbl.getTableType().equals(TableType.EXTERNAL_TABLE));
    tblDesc.setCompressed(tbl.getTTable().getSd().isCompressed());
    tblDesc.setTableLocation(tbl.getDataLocation().toString());
    tblDesc.setInputFormat(tbl.getInputFormatClass().getCanonicalName());
    tblDesc.setOutputFormat(tbl.getOutputFormatClass().getCanonicalName());
    tblDesc.setFieldDelimiter(tbl.getSerdeParam(serdeConstants.FIELD_DELIM));
    tblDesc.setLineDelimiter(tbl.getSerdeParam(serdeConstants.LINE_DELIM));
    tblDesc.setCollectionDelimiter(tbl.getSerdeParam(serdeConstants.COLLECTION_DELIM));
    tblDesc.setMapKeyDelimiter(tbl.getSerdeParam(serdeConstants.MAPKEY_DELIM));
    tblDesc.setEscapeChar(tbl.getSerdeParam(serdeConstants.ESCAPE_CHAR));
    tblDesc.setSerdeClassName(tbl.getSerializationLib());
    tblDesc.setStorageHandlerName(tbl.getStorageHandler()!= null?
        tbl.getStorageHandler().getClass().getCanonicalName():"");
    return tblDesc;
  }

  public static Map<String, StorageTableDesc> storageTableMapFromXStorageTables(XStorageTables storageTables) {
    Map<String, StorageTableDesc> storageTableMap = new HashMap<String, StorageTableDesc>();
    for (XStorageTableElement sTbl : storageTables.getStorageTables()) {
      storageTableMap.put(sTbl.getStorageName(), storageTableDescFromXStorageTableElement(sTbl));
    }
    return storageTableMap;
  }

  public static Map<String, Date> timePartSpecfromXTimePartSpec(
      XTimePartSpec xtimePartSpec) {
    Map<String, Date> timePartSpec = new HashMap<String, Date>();
    if (xtimePartSpec != null) {
      for (XTimePartSpecElement xtimePart : xtimePartSpec.getPartSpecElement()) {
        timePartSpec.put(xtimePart.getKey(), getDateFromXML(xtimePart.getValue()));
      }
    }
    return timePartSpec;
  }

  public static Map<String, String> nonTimePartSpecfromXNonTimePartSpec(
      XPartSpec xnonTimePartSpec) {
    Map<String, String> nonTimePartSpec = new HashMap<String, String>();
    if (xnonTimePartSpec != null) {
      for (XPartSpecElement xPart : xnonTimePartSpec.getPartSpecElement()) {
        nonTimePartSpec.put(xPart.getKey(), xPart.getValue());
      }
    }
    return nonTimePartSpec;
  }

  public static XPartition xpartitionFromPartition(Partition p) throws HiveException {
    XPartition xp = new XPartition();
    xp.setName(p.getCompleteName());
    xp.setLocation(p.getLocation());
    xp.setInputFormat(p.getInputFormatClass().getCanonicalName());
    xp.setOutputFormat(p.getOutputFormatClass().getCanonicalName());
    xp.setPartitionParameters(xPropertiesFromMap(p.getParameters()));
    String timePartColsStr = p.getTable().getTTable().getParameters().get(MetastoreConstants.TIME_PART_COLUMNS);
    String upParam = p.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
    xp.setUpdatePeriod(upParam);
    UpdatePeriod period = UpdatePeriod.valueOf(upParam);
    XPartSpec xps = new XPartSpec();
    for (Map.Entry<String, String> entry : p.getSpec().entrySet()) {
      XPartSpecElement e = new XPartSpecElement();
      e.setKey(entry.getKey());
      e.setValue(entry.getValue());
      xps.getPartSpecElement().add(e);
    }
    xp.setFullPartitionSpec(xps);
    xp.setSerdeClassname(p.getTPartition().getSd().getSerdeInfo().getSerializationLib());
    xp.setSerdeParameters(xPropertiesFromMap(p.getTPartition().getSd().getSerdeInfo().getParameters()));
    return xp;
  }

  public static StoragePartitionDesc storagePartSpecFromXPartition(
      XPartition xpart) {
    StoragePartitionDesc partDesc = new StoragePartitionDesc(
        xpart.getCubeTableName(),
        timePartSpecfromXTimePartSpec(xpart.getTimePartitionSpec()),
        nonTimePartSpecfromXNonTimePartSpec(xpart.getNonTimePartitionSpec()),
        UpdatePeriod.valueOf(xpart.getUpdatePeriod().toUpperCase()));
    partDesc.setPartParams(mapFromXProperties(xpart.getPartitionParameters()));
    partDesc.setSerdeParams(mapFromXProperties(xpart.getSerdeParameters()));
    partDesc.setLocation(xpart.getLocation());
    partDesc.setInputFormat(xpart.getInputFormat());
    partDesc.setOutputFormat(xpart.getOutputFormat());
    partDesc.setSerializationLib(xpart.getSerdeClassname());
    return partDesc;
  }

  public static Dimension dimensionFromXDimension(XDimension dimension) throws ParseException {
    Set<CubeDimAttribute> dims = new LinkedHashSet<CubeDimAttribute>();
    for (XDimAttribute xd : dimension.getAttributes().getDimAttributes()) {
      dims.add(hiveDimAttrFromXDimAttr(xd));
    }

    Set<ExprColumn> expressions = new LinkedHashSet<ExprColumn>();
    if (dimension.getExpressions() != null) {
      for (XExprColumn xe : dimension.getExpressions().getExpressions()) {
        expressions.add(hiveExprColumnFromXExprColumn(xe));
      }
    }

    Map<String, String> properties = mapFromXProperties(dimension.getProperties());
    double dimWeight = dimension.getWeight() == null ? 0d : dimension.getWeight();
    return new Dimension(dimension.getName(), dims, expressions, properties, dimWeight);
  }

  public static XDimension xdimensionFromDimension(Dimension dimension) {
    XDimension xd = XCF.createXDimension();
    xd.setName(dimension.getName());
    xd.setWeight(((AbstractCubeTable)dimension).weight());
    xd.setProperties(xPropertiesFromMap(((AbstractCubeTable)dimension).getProperties()));
    XDimAttributes xdm = XCF.createXDimAttributes();
    List<XDimAttribute> xdmList = xdm.getDimAttributes();
    for (CubeDimAttribute cd : dimension.getAttributes()) {
      xdmList.add(xDimAttrFromHiveDimAttr(cd));
    }
    xd.setAttributes(xdm);

    XExpressions xexprs = XCF.createXExpressions();
    List<XExprColumn> xexprList = xexprs.getExpressions();
    for (ExprColumn ec : dimension.getExpressions()) {
      xexprList.add(xExprColumnFromHiveExprColumn(ec));
    }
    xd.setExpressions(xexprs);

    return xd;
  }

  public static NativeTable nativeTableFromMetaTable(Table table) {
    NativeTable xtable = XCF.createNativeTable();
    xtable.setName(table.getTableName());
    xtable.setDbname(table.getDbName());
    xtable.setOwner(table.getOwner());
    xtable.setCreatetime(table.getTTable().getCreateTime());
    xtable.setLastAccessTime(table.getTTable().getLastAccessTime());
    xtable.setColumns(columnsFromFieldSchemaList(table.getCols()));
    xtable.setStorageDescriptor(getStorageTableDescFromHiveTable(table));
    xtable.setType(table.getTableType().name());
    return xtable;
  }
}
