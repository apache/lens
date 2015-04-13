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

import java.lang.reflect.Constructor;
import java.util.*;

import javax.ws.rs.WebApplicationException;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

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

import com.google.common.base.Optional;

/**
 * Utilities for converting to and from JAXB types to hive.ql.metadata.cube types
 */
public final class JAXBUtils {
  private JAXBUtils() {

  }

  public static final Logger LOG = LogManager.getLogger(JAXBUtils.class);
  private static final ObjectFactory XCF = new ObjectFactory();

  /**
   * Create a hive ql cube obejct from corresponding JAXB object
   *
   * @param cube JAXB Cube
   * @return {@link Cube}
   * @throws ParseException
   */
  public static CubeInterface hiveCubeFromXCube(XCube cube, Cube parent) throws ParseException {
    if (cube instanceof XDerivedCube) {
      XDerivedCube dcube = (XDerivedCube) cube;
      Set<String> dims = new LinkedHashSet<String>();
      dims.addAll(dcube.getDimAttrNames().getAttrName());

      Set<String> measures = new LinkedHashSet<String>();
      measures.addAll(dcube.getMeasureNames().getMeasureName());

      Map<String, String> properties = mapFromXProperties(cube.getProperties());
      return new DerivedCube(cube.getName(), measures, dims, properties, 0L, parent);
    } else {
      XBaseCube bcube = (XBaseCube) cube;
      Set<CubeDimAttribute> dims = new LinkedHashSet<CubeDimAttribute>();
      if (bcube.getDimAttributes() != null && !bcube.getDimAttributes().getDimAttribute().isEmpty()) {
        for (XDimAttribute xd : bcube.getDimAttributes().getDimAttribute()) {
          dims.add(hiveDimAttrFromXDimAttr(xd));
        }
      }

      Set<CubeMeasure> measures = new LinkedHashSet<CubeMeasure>();
      for (XMeasure xm : bcube.getMeasures().getMeasure()) {
        measures.add(hiveMeasureFromXMeasure(xm));
      }

      Set<ExprColumn> expressions = new LinkedHashSet<ExprColumn>();
      if (bcube.getExpressions() != null && !bcube.getExpressions().getExpression().isEmpty()) {
        for (XExprColumn xe : bcube.getExpressions().getExpression()) {
          expressions.add(hiveExprColumnFromXExprColumn(xe));
        }
      }

      Set<JoinChain> joinchains = new LinkedHashSet<JoinChain>();
      if (bcube.getJoinChains() != null && !bcube.getJoinChains().getJoinChain().isEmpty()) {
        for (XJoinChain xj : bcube.getJoinChains().getJoinChain()) {
          joinchains.add(joinChainFromXJoinChain(xj));
        }
      }

      Map<String, String> properties = mapFromXProperties(cube.getProperties());
      return new Cube(cube.getName(), measures, dims, expressions, joinchains, properties, 0L);
    }
  }

  /**
   * Get XCube from hive.ql.metadata.Cube
   *
   * @param c
   * @return {@link XCube}
   */
  public static XCube xCubeFromHiveCube(CubeInterface c) {
    XCube xc;
    if (c.isDerivedCube()) {
      XDerivedCube xdc = XCF.createXDerivedCube();
      xdc.setMeasureNames(new XMeasureNames());
      xdc.setDimAttrNames(new XDimAttrNames());
      xc = xdc;
      xdc.getMeasureNames().getMeasureName().addAll(c.getMeasureNames());
      xdc.getDimAttrNames().getAttrName().addAll(c.getDimAttributeNames());
      xdc.setParent(((DerivedCube) c).getParent().getName());
    } else {
      XBaseCube xbc = XCF.createXBaseCube();
      xbc.setMeasures(new XMeasures());
      xbc.setDimAttributes(new XDimAttributes());
      xbc.setExpressions(new XExpressions());
      xbc.setJoinChains(new XJoinChains());
      xc = xbc;
      for (CubeMeasure cm : c.getMeasures()) {
        xbc.getMeasures().getMeasure().add(xMeasureFromHiveMeasure(cm));
      }

      for (ExprColumn ec : c.getExpressions()) {
        xbc.getExpressions().getExpression().add(xExprColumnFromHiveExprColumn(ec));
      }
      for (CubeDimAttribute cd : c.getDimAttributes()) {
        xbc.getDimAttributes().getDimAttribute().add(xDimAttrFromHiveDimAttr(cd, (Cube) c));
      }
      for (JoinChain jc : c.getJoinChains()) {
        xbc.getJoinChains().getJoinChain().add(getXJoinChainFromJoinChain(jc));
      }
    }
    xc.setName(c.getName());
    xc.setProperties(new XProperties());
    xc.getProperties().getProperty().addAll(xPropertiesFromMap(((AbstractCubeTable) c).getProperties()));
    return xc;
  }

  /**
   * Create a hive ql CubeDimension from JAXB counterpart
   *
   * @param xd
   * @return {@link org.apache.lens.cube.metadata.CubeDimAttribute}
   */
  public static CubeDimAttribute hiveDimAttrFromXDimAttr(XDimAttribute xd) {
    Date startDate = getDateFromXML(xd.getStartTime());
    Date endDate = getDateFromXML(xd.getEndTime());

    CubeDimAttribute hiveDim;

    if (xd.getRefSpec() != null && xd.getRefSpec().getTableReferences() != null
      && !xd.getRefSpec().getTableReferences().getTableReference().isEmpty()) {

      List<TableReference> dimRefs = new ArrayList<TableReference>(
        xd.getRefSpec().getTableReferences().getTableReference().size());

      for (XTableReference xRef : xd.getRefSpec().getTableReferences().getTableReference()) {
        dimRefs.add(new TableReference(xRef.getTable(), xRef.getColumn()));
      }

      hiveDim = new ReferencedDimAtrribute(new FieldSchema(xd.getName(), xd.getType().toLowerCase(),
        xd.getDescription()),
        xd.getDisplayString(),
        dimRefs,
        startDate,
        endDate,
        null,
        xd.isJoinKey(),
        xd.getNumDistinctValues()
      );
    } else if (xd.getRefSpec() != null && xd.getRefSpec().getChainRefColumn() != null) {
      hiveDim = new ReferencedDimAtrribute(new FieldSchema(xd.getName(), xd.getType().toLowerCase(),
        xd.getDescription()),
        xd.getDisplayString(),
        xd.getRefSpec().getChainRefColumn().getChainName(),
        xd.getRefSpec().getChainRefColumn().getRefCol(),
        startDate,
        endDate,
        null,
        xd.getNumDistinctValues()
      );
    } else {
      hiveDim = new BaseDimAttribute(new FieldSchema(xd.getName(), xd.getType().toLowerCase(),
        xd.getDescription()),
        xd.getDisplayString(),
        startDate,
        endDate,
        null,
        xd.getNumDistinctValues()
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
    xm.setType(XMeasureType.valueOf(cm.getType().toUpperCase()));
    xm.setUnit(cm.getUnit());
    xm.setStartTime(getXMLGregorianCalendar(cm.getStartTime()));
    xm.setEndTime(getXMLGregorianCalendar(cm.getEndTime()));
    xm.setMin(cm.getMin());
    xm.setMax(cm.getMax());
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
   * Create XDimAttribute from CubeDimAttribute
   */
  public static XDimAttribute xDimAttrFromHiveDimAttr(CubeDimAttribute cd, AbstractBaseTable baseTable) {
    XDimAttribute xd = XCF.createXDimAttribute();
    xd.setName(cd.getName());
    xd.setDescription(cd.getDescription());
    xd.setDisplayString(cd.getDisplayString());
    if (cd instanceof ReferencedDimAtrribute) {
      ReferencedDimAtrribute rd = (ReferencedDimAtrribute) cd;
      List<TableReference> dimRefs = rd.getReferences();
      XDimAttribute.RefSpec refspec = XCF.createXDimAttributeRefSpec();
      if (rd.getChainName() != null) {
        XChainColumn xcc = new XChainColumn();
        xcc.setChainName(rd.getChainName());
        xcc.setRefCol(rd.getRefColumn());
        if (baseTable.getChainByName(rd.getChainName()) == null) {
          LOG.error("Missing chain definition for " + rd.getChainName());
        } else {
          xcc.setDestTable(baseTable.getChainByName(rd.getChainName()).getDestTable());
        }
        refspec.setChainRefColumn(xcc);
        xd.setJoinKey(Boolean.valueOf(false));
      } else {
        refspec.setTableReferences(new XTableReferences());
        refspec.getTableReferences().getTableReference().addAll(xTabReferencesFromHiveTabReferences(dimRefs));
        xd.setJoinKey(rd.useAsJoinKey());
      }
      xd.setRefSpec(refspec);
      xd.setType(rd.getType());
      Optional<Long> numOfDistinctValues = rd.getNumOfDistinctValues();
      if (numOfDistinctValues.isPresent()) {
        xd.setNumDistinctValues(numOfDistinctValues.get());
      }
    } else if (cd instanceof BaseDimAttribute) {
      BaseDimAttribute bd = (BaseDimAttribute) cd;
      xd.setType(bd.getType());
      Optional<Long> numOfDistinctValues = bd.getNumOfDistinctValues();
      if (numOfDistinctValues.isPresent()) {
        xd.setNumDistinctValues(numOfDistinctValues.get());
      }
    }
    return xd;
  }

  /**
   * Create XJoinChain from cube join chain
   */
  public static XJoinChain getXJoinChainFromJoinChain(JoinChain jc) {
    XJoinChain xjc = XCF.createXJoinChain();
    xjc.setName(jc.getName());
    xjc.setDescription(jc.getDescription());
    xjc.setDisplayString(jc.getDisplayString());
    xjc.setDestTable(jc.getDestTable());
    xjc.setPaths(new XJoinPaths());

    for (JoinChain.Path path : jc.getPaths()) {
      xjc.getPaths().getPath().add(xJoinPathFromJoinPath(path));
    }
    return xjc;
  }

  public static XJoinPath xJoinPathFromJoinPath(JoinChain.Path path) {
    XJoinPath xjp = XCF.createXJoinPath();
    xjp.setEdges(new XJoinEdges());
    for (JoinChain.Edge edge : path.getLinks()) {
      XJoinEdge xje = XCF.createXJoinEdge();
      xje.setFrom(xTabReferenceFromTabReference(edge.getFrom()));
      xje.setTo(xTabReferenceFromTabReference(edge.getTo()));
      xjp.getEdges().getEdge().add(xje);
    }
    return xjp;
  }

  public static List<XTableReference> xTabReferencesFromHiveTabReferences(List<TableReference> hiveRefs) {
    List<XTableReference> xrefList = new ArrayList<XTableReference>();

    for (TableReference hRef : hiveRefs) {
      xrefList.add(xTabReferenceFromTabReference(hRef));
    }
    return xrefList;
  }

  private static XTableReference xTabReferenceFromTabReference(TableReference ref) {
    XTableReference xref = XCF.createXTableReference();
    xref.setTable(ref.getDestTable());
    xref.setColumn(ref.getDestColumn());
    return xref;
  }

  /**
   * Create hive ql CubeMeasure from JAXB counterpart
   *
   * @param xm
   * @return {@link CubeMeasure}
   */
  public static CubeMeasure hiveMeasureFromXMeasure(XMeasure xm) {
    Date startDate = xm.getStartTime() == null ? null : xm.getStartTime().toGregorianCalendar().getTime();
    Date endDate = xm.getEndTime() == null ? null : xm.getEndTime().toGregorianCalendar().getTime();
    CubeMeasure cm = new ColumnMeasure(new FieldSchema(xm.getName(), xm.getType().name().toLowerCase(),
      xm.getDescription()),
      xm.getDisplayString(),
      xm.getFormatString(),
      xm.getDefaultAggr(),
      xm.getUnit(),
      startDate,
      endDate,
      null,
      xm.getMin(),
      xm.getMax()
    );
    return cm;
  }

  /**
   * Create cube's JoinChain from JAXB counterpart
   *
   * @param xj
   * @return {@link JoinChain}
   */
  public static JoinChain joinChainFromXJoinChain(XJoinChain xj) {
    JoinChain jc = new JoinChain(xj.getName(), xj.getDisplayString(), xj.getDescription());
    for (int i = 0; i < xj.getPaths().getPath().size(); i++) {
      XJoinPath xchain = xj.getPaths().getPath().get(i);
      List<TableReference> chain = new ArrayList<TableReference>(xchain.getEdges().getEdge().size() * 2);

      for (XJoinEdge xRef : xchain.getEdges().getEdge()) {
        chain.add(new TableReference(xRef.getFrom().getTable(), xRef.getFrom().getColumn()));
        chain.add(new TableReference(xRef.getTo().getTable(), xRef.getTo().getColumn()));
      }
      jc.addPath(chain);
    }
    return jc;
  }

  public static ExprColumn hiveExprColumnFromXExprColumn(XExprColumn xe) throws ParseException {
    ExprColumn ec = new ExprColumn(new FieldSchema(xe.getName(), xe.getType().toLowerCase(),
      xe.getDescription()),
      xe.getDisplayString(),
      xe.getExpr());
    return ec;
  }

  /**
   * Convert JAXB properties to Map<String, String>
   *
   * @param xProperties
   * @return {@link Map}
   */
  public static Map<String, String> mapFromXProperties(XProperties xProperties) {
    Map<String, String> properties = new HashMap<String, String>();
    if (xProperties != null && !xProperties.getProperty().isEmpty()) {
      for (XProperty xp : xProperties.getProperty()) {
        properties.put(xp.getName(), xp.getValue());
      }
    }
    return properties;
  }

  /**
   * Convert string map to XProperties
   */
  public static List<XProperty> xPropertiesFromMap(Map<String, String> map) {
    List<XProperty> xpList = new ArrayList<XProperty>();
    if (map != null && !map.isEmpty()) {
      for (Map.Entry<String, String> e : map.entrySet()) {
        XProperty property = XCF.createXProperty();
        property.setName(e.getKey());
        property.setValue(e.getValue());
        xpList.add(property);
      }
    }
    return xpList;
  }

  public static FieldSchema fieldSchemaFromColumn(XColumn c) {
    if (c == null) {
      return null;
    }

    return new FieldSchema(c.getName(), c.getType().toLowerCase(), c.getComment());
  }

  public static XColumn columnFromFieldSchema(FieldSchema fs) {
    if (fs == null) {
      return null;
    }
    XColumn c = XCF.createXColumn();
    c.setName(fs.getName());
    c.setType(fs.getType());
    c.setComment(fs.getComment());
    return c;
  }

  public static ArrayList<FieldSchema> fieldSchemaListFromColumns(XColumns columns) {
    if (columns != null && !columns.getColumn().isEmpty()) {
      ArrayList<FieldSchema> fsList = new ArrayList<FieldSchema>(columns.getColumn().size());
      for (XColumn c : columns.getColumn()) {
        fsList.add(fieldSchemaFromColumn(c));
      }
      return fsList;
    }
    return null;
  }

  public static List<XColumn> columnsFromFieldSchemaList(List<FieldSchema> fslist) {
    List<XColumn> cols = new ArrayList<XColumn>();
    if (fslist == null || fslist.isEmpty()) {
      return cols;
    }

    for (FieldSchema fs : fslist) {
      cols.add(columnFromFieldSchema(fs));
    }
    return cols;
  }

  public static Map<String, Set<UpdatePeriod>> getFactUpdatePeriodsFromStorageTables(
    XStorageTables storageTables) {
    if (storageTables != null && !storageTables.getStorageTable().isEmpty()) {
      Map<String, Set<UpdatePeriod>> factUpdatePeriods = new LinkedHashMap<String, Set<UpdatePeriod>>();

      for (XStorageTableElement ste : storageTables.getStorageTable()) {
        Set<UpdatePeriod> updatePeriods = new TreeSet<UpdatePeriod>();
        for (XUpdatePeriod upd : ste.getUpdatePeriods().getUpdatePeriod()) {
          updatePeriods.add(UpdatePeriod.valueOf(upd.name()));
        }
        factUpdatePeriods.put(ste.getStorageName(), updatePeriods);
      }
      return factUpdatePeriods;
    }
    return null;
  }

  public static Map<String, UpdatePeriod> dumpPeriodsFromStorageTables(XStorageTables storageTables) {
    if (storageTables != null && !storageTables.getStorageTable().isEmpty()) {
      Map<String, UpdatePeriod> dumpPeriods = new LinkedHashMap<String, UpdatePeriod>();

      for (XStorageTableElement ste : storageTables.getStorageTable()) {
        UpdatePeriod dumpPeriod = null;
        if (ste.getUpdatePeriods() != null && !ste.getUpdatePeriods().getUpdatePeriod().isEmpty()) {
          dumpPeriod = UpdatePeriod.valueOf(ste.getUpdatePeriods().getUpdatePeriod().get(0).name());
        }
        dumpPeriods.put(ste.getStorageName(), dumpPeriod);
      }
      return dumpPeriods;
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
    xstorage.setProperties(new XProperties());
    xstorage.setName(storage.getName());
    xstorage.setClassname(storage.getClass().getCanonicalName());
    xstorage.getProperties().getProperty().addAll(xPropertiesFromMap(storage.getProperties()));
    return xstorage;
  }

  public static XDimensionTable dimTableFromCubeDimTable(CubeDimensionTable cubeDimTable) {
    if (cubeDimTable == null) {
      return null;
    }

    XDimensionTable dimTab = XCF.createXDimensionTable();
    dimTab.setDimensionName(cubeDimTable.getDimName());
    dimTab.setTableName(cubeDimTable.getName());
    dimTab.setWeight(cubeDimTable.weight());
    dimTab.setColumns(new XColumns());
    dimTab.setProperties(new XProperties());
    dimTab.setStorageTables(new XStorageTables());

    for (FieldSchema column : cubeDimTable.getColumns()) {
      dimTab.getColumns().getColumn().add(columnFromFieldSchema(column));
    }
    dimTab.getProperties().getProperty().addAll(xPropertiesFromMap(cubeDimTable.getProperties()));

    return dimTab;
  }

  public static List<? extends XTableReference> dimRefListFromTabRefList(
    List<TableReference> tabRefs) {
    if (tabRefs != null && !tabRefs.isEmpty()) {
      List<XTableReference> xTabRefs = new ArrayList<XTableReference>(tabRefs.size());
      for (TableReference ref : tabRefs) {
        XTableReference xRef = XCF.createXTableReference();
        xRef.setColumn(ref.getDestColumn());
        xRef.setTable(ref.getDestTable());
        xTabRefs.add(xRef);
      }
      return xTabRefs;
    }

    return null;
  }

  public static CubeDimensionTable cubeDimTableFromDimTable(XDimensionTable dimensionTable) {

    CubeDimensionTable cdim = new CubeDimensionTable(dimensionTable.getDimensionName(),
      dimensionTable.getTableName(),
      fieldSchemaListFromColumns(dimensionTable.getColumns()),
      dimensionTable.getWeight(),
      dumpPeriodsFromStorageTables(dimensionTable.getStorageTables()),
      mapFromXProperties(dimensionTable.getProperties()));

    return cdim;
  }

  public static CubeFactTable cubeFactFromFactTable(XFactTable fact) {
    List<FieldSchema> columns = fieldSchemaListFromColumns(fact.getColumns());

    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = getFactUpdatePeriodsFromStorageTables(
      fact.getStorageTables());

    return new CubeFactTable(fact.getCubeName(),
      fact.getName(),
      columns,
      storageUpdatePeriods,
      fact.getWeight(),
      mapFromXProperties(fact.getProperties()));
  }

  public static XFactTable factTableFromCubeFactTable(CubeFactTable cFact) {
    XFactTable fact = XCF.createXFactTable();
    fact.setName(cFact.getName());
    fact.setColumns(new XColumns());
    fact.setProperties(new XProperties());
    fact.setStorageTables(new XStorageTables());

    fact.getProperties().getProperty().addAll(xPropertiesFromMap(cFact.getProperties()));
    fact.getColumns().getColumn().addAll(columnsFromFieldSchemaList(cFact.getColumns()));
    fact.setWeight(cFact.weight());
    fact.setCubeName(cFact.getCubeName());
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
    tblElement.setUpdatePeriods(new XUpdatePeriods());
    tblElement.setTableDesc(getStorageTableDescFromHiveTable(tbl));
    return tblElement;
  }

  public static XStorageTableDesc getStorageTableDescFromHiveTable(Table tbl) {
    XStorageTableDesc tblDesc = new XStorageTableDesc();
    tblDesc.setPartCols(new XColumns());
    tblDesc.setTableParameters(new XProperties());
    tblDesc.setSerdeParameters(new XProperties());
    tblDesc.getPartCols().getColumn().addAll(columnsFromFieldSchemaList(tbl.getPartCols()));
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
      XSkewColList colVallist = new XSkewColList();
      colVallist.getElements().addAll(value);
      xskewinfo.getColValues().add(colVallist);
      XSkewedValueLocation valueLocation = new XSkewedValueLocation();
      if (tbl.getSkewedColValueLocationMaps().get(value) != null) {
        valueLocation.setValue(colVallist);
        valueLocation.setLocation(tbl.getSkewedColValueLocationMaps().get(value));
        xskewinfo.getValueLocationMap().add(valueLocation);
      }
    }

    tblDesc.getTableParameters().getProperty().addAll(xPropertiesFromMap(tbl.getParameters()));
    tblDesc.getSerdeParameters().getProperty().addAll(xPropertiesFromMap(
      tbl.getTTable().getSd().getSerdeInfo().getParameters()));
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
    tblDesc.setStorageHandlerName(tbl.getStorageHandler() != null
      ? tbl.getStorageHandler().getClass().getCanonicalName() : "");
    return tblDesc;
  }

  public static Map<String, StorageTableDesc> storageTableMapFromXStorageTables(XStorageTables storageTables) {
    Map<String, StorageTableDesc> storageTableMap = new HashMap<String, StorageTableDesc>();
    if (storageTables != null && !storageTables.getStorageTable().isEmpty()) {
      for (XStorageTableElement sTbl : storageTables.getStorageTable()) {
        storageTableMap.put(sTbl.getStorageName(), storageTableDescFromXStorageTableElement(sTbl));
      }
    }
    return storageTableMap;
  }

  public static Map<String, Date> timePartSpecfromXTimePartSpec(
    XTimePartSpec xtimePartSpec) {
    Map<String, Date> timePartSpec = new HashMap<String, Date>();
    if (xtimePartSpec != null && !xtimePartSpec.getPartSpecElement().isEmpty()) {
      for (XTimePartSpecElement xtimePart : xtimePartSpec.getPartSpecElement()) {
        timePartSpec.put(xtimePart.getKey(), getDateFromXML(xtimePart.getValue()));
      }
    }
    return timePartSpec;
  }

  public static Map<String, String> nonTimePartSpecfromXNonTimePartSpec(
    XPartSpec xnonTimePartSpec) {
    Map<String, String> nonTimePartSpec = new HashMap<String, String>();
    if (xnonTimePartSpec != null && !xnonTimePartSpec.getPartSpecElement().isEmpty()) {
      for (XPartSpecElement xPart : xnonTimePartSpec.getPartSpecElement()) {
        nonTimePartSpec.put(xPart.getKey(), xPart.getValue());
      }
    }
    return nonTimePartSpec;
  }

  public static XPartition xpartitionFromPartition(Partition p) throws HiveException {
    XPartition xp = new XPartition();
    xp.setFullPartitionSpec(new XPartSpec());
    xp.setPartitionParameters(new XProperties());
    xp.setSerdeParameters(new XProperties());
    xp.setName(p.getCompleteName());
    xp.setLocation(p.getLocation());
    xp.setInputFormat(p.getInputFormatClass().getCanonicalName());
    xp.setOutputFormat(p.getOutputFormatClass().getCanonicalName());
    xp.getPartitionParameters().getProperty().addAll(xPropertiesFromMap(p.getParameters()));
    String upParam = p.getParameters().get(MetastoreConstants.PARTITION_UPDATE_PERIOD);
    xp.setUpdatePeriod(XUpdatePeriod.valueOf(upParam));
    for (Map.Entry<String, String> entry : p.getSpec().entrySet()) {
      XPartSpecElement e = new XPartSpecElement();
      e.setKey(entry.getKey());
      e.setValue(entry.getValue());
      xp.getFullPartitionSpec().getPartSpecElement().add(e);
    }
    xp.setSerdeClassname(p.getTPartition().getSd().getSerdeInfo().getSerializationLib());
    xp.getSerdeParameters().getProperty().addAll(xPropertiesFromMap(
      p.getTPartition().getSd().getSerdeInfo().getParameters()));
    return xp;
  }

  public static StoragePartitionDesc storagePartSpecFromXPartition(
    XPartition xpart) {
    StoragePartitionDesc partDesc = new StoragePartitionDesc(
      xpart.getFactOrDimensionTableName(),
      timePartSpecfromXTimePartSpec(xpart.getTimePartitionSpec()),
      nonTimePartSpecfromXNonTimePartSpec(xpart.getNonTimePartitionSpec()),
      UpdatePeriod.valueOf(xpart.getUpdatePeriod().name()));
    partDesc.setPartParams(mapFromXProperties(xpart.getPartitionParameters()));
    partDesc.setSerdeParams(mapFromXProperties(xpart.getSerdeParameters()));
    partDesc.setLocation(xpart.getLocation());
    partDesc.setInputFormat(xpart.getInputFormat());
    partDesc.setOutputFormat(xpart.getOutputFormat());
    partDesc.setSerializationLib(xpart.getSerdeClassname());
    return partDesc;
  }

  public static List<StoragePartitionDesc> storagePartSpecListFromXPartitionList(
    final XPartitionList xpartList) {
    ArrayList<StoragePartitionDesc> ret = new ArrayList<StoragePartitionDesc>();
    for (XPartition xpart : xpartList.getPartition()) {
      ret.add(storagePartSpecFromXPartition(xpart));
    }
    return ret;
  }

  public static Dimension dimensionFromXDimension(XDimension dimension) throws ParseException {
    Set<CubeDimAttribute> dims = new LinkedHashSet<CubeDimAttribute>();
    for (XDimAttribute xd : dimension.getAttributes().getDimAttribute()) {
      dims.add(hiveDimAttrFromXDimAttr(xd));
    }

    Set<ExprColumn> expressions = new LinkedHashSet<ExprColumn>();
    if (dimension.getExpressions() != null && !dimension.getExpressions().getExpression().isEmpty()) {
      for (XExprColumn xe : dimension.getExpressions().getExpression()) {
        expressions.add(hiveExprColumnFromXExprColumn(xe));
      }
    }

    Set<JoinChain> joinchains = new LinkedHashSet<JoinChain>();
    if (dimension.getJoinChains() != null && !dimension.getJoinChains().getJoinChain().isEmpty()) {
      for (XJoinChain xj : dimension.getJoinChains().getJoinChain()) {
        joinchains.add(joinChainFromXJoinChain(xj));
      }
    }

    Map<String, String> properties = mapFromXProperties(dimension.getProperties());
    return new Dimension(dimension.getName(), dims, expressions, joinchains, properties, 0L);
  }

  public static XDimension xdimensionFromDimension(Dimension dimension) {
    XDimension xd = XCF.createXDimension();
    xd.setName(dimension.getName());
    xd.setAttributes(new XDimAttributes());
    xd.setExpressions(new XExpressions());
    xd.setJoinChains(new XJoinChains());
    xd.setProperties(new XProperties());

    xd.getProperties().getProperty().addAll(xPropertiesFromMap(((AbstractCubeTable) dimension).getProperties()));
    for (CubeDimAttribute cd : dimension.getAttributes()) {
      xd.getAttributes().getDimAttribute().add(xDimAttrFromHiveDimAttr(cd, dimension));
    }

    for (ExprColumn ec : dimension.getExpressions()) {
      xd.getExpressions().getExpression().add(xExprColumnFromHiveExprColumn(ec));
    }

    for (JoinChain jc : dimension.getJoinChains()) {
      xd.getJoinChains().getJoinChain().add(getXJoinChainFromJoinChain(jc));
    }

    return xd;
  }

  public static XNativeTable nativeTableFromMetaTable(Table table) {
    XNativeTable xtable = XCF.createXNativeTable();
    xtable.setColumns(new XColumns());
    xtable.setName(table.getTableName());
    xtable.setDbname(table.getDbName());
    xtable.setOwner(table.getOwner());
    xtable.setCreatetime(table.getTTable().getCreateTime());
    xtable.setLastAccessTime(table.getTTable().getLastAccessTime());
    xtable.getColumns().getColumn().addAll(columnsFromFieldSchemaList(table.getCols()));
    xtable.setStorageDescriptor(getStorageTableDescFromHiveTable(table));
    xtable.setTableType(table.getTableType().name());
    return xtable;
  }
}
