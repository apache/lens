package com.inmobi.grill.metastore.service;


import com.inmobi.grill.metastore.model.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
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
   * @param cube JAXB Cube
   * @return
   */
  public static Cube hiveCubeFromXCube(XCube cube) {
    Set<CubeDimension> dims = new LinkedHashSet<CubeDimension>();
    for (XDimension xd : cube.getDimensions().getDimensions()) {
      dims.add(hiveDimFromXDim(xd));
    }

    Set<CubeMeasure> measures = new LinkedHashSet<CubeMeasure>();
    for (XMeasure xm : cube.getMeasures().getMeasures()) {
      measures.add(hiveMeasureFromXMeasure(xm));
    }

    Map<String, String> properties = mapFromXProperties(cube.getProperties());
    double cubeWeight = cube.getWeight() == null ? 0d : cube.getWeight();
    return new Cube(cube.getName(), measures, dims, properties, cubeWeight);
  }

  /**
   * Get XCube from hive.ql.metadata.Cube
   * @param c
   * @return
   */
  public static XCube xCubeFromHiveCube(Cube c) {
    XCube xc = XCF.createXCube();
    xc.setName(c.getName());
    xc.setWeight(c.weight());

    XMeasures xms = XCF.createXMeasures();
    List<XMeasure> xmsList = xms.getMeasures();
    for (CubeMeasure cm : c.getMeasures()) {
      xmsList.add(xMeasureFromHiveMeasure(cm));
    }
    xc.setMeasures(xms);


    XDimensions xdm = XCF.createXDimensions();
    List<XDimension> xdmList = xdm.getDimensions();
    for (CubeDimension cd : c.getDimensions()) {
      xdmList.add(xDimensionFromHiveDimension(cd));
    }
    xc.setDimensions(xdm);

    xc.setProperties(xPropertiesFromMap(c.getProperties()));
    return xc;
  }

  /**
   * Create a hive ql CubeDimension from JAXB counterpart
   * @param xd
   * @return
   */
  public static CubeDimension hiveDimFromXDim(XDimension xd) {
    Date startDate = getDateFromXML(xd.getStartTime());
    Date endDate = getDateFromXML(xd.getEndTime());

    CubeDimension hiveDim;

    if (xd.getReferences() != null && xd.getReferences().getTableReferences() != null &&
        !xd.getReferences().getTableReferences().isEmpty()) {

      List<TableReference> dimRefs = new ArrayList<TableReference>(xd.getReferences().getTableReferences().size());

      for (XTablereference xRef : xd.getReferences().getTableReferences()) {
        dimRefs.add(new TableReference(xRef.getDestTable(), xRef.getDestColumn()));
      }

      hiveDim = new ReferencedDimension(new FieldSchema(xd.getName(), xd.getType(), ""),
          dimRefs,
          startDate,
          endDate,
          xd.getCost()
          );
    } else {
      hiveDim = new BaseDimension(new FieldSchema(xd.getName(), xd.getType(), ""),
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
    if (cal == null) return null;
    return cal.toGregorianCalendar().getTime();
  }

  /**
   * Create XMeasure from hive ql cube measure
   */
  public static XMeasure xMeasureFromHiveMeasure(CubeMeasure cm) {
    if (cm == null) return null;

    XMeasure xm = XCF.createXMeasure();
    xm.setName(cm.getName());
    xm.setDefaultAggr(cm.getAggregate());
    xm.setFormatString(cm.getFormatString());
    xm.setType(cm.getType());
    xm.setUnit(cm.getUnit());
    xm.setCost(cm.getCost());
    xm.setStartTime(getXMLGregorianCalendar(cm.getStartTime()));
    xm.setEndTime(getXMLGregorianCalendar(cm.getEndTime()));

    if (cm instanceof ExprMeasure) {
      xm.setExpr(((ExprMeasure) cm).getExpr());
    }
    return xm;
  }

  /**
   * Create XDimension from hive ql cube dimension
   */
  public static XDimension xDimensionFromHiveDimension(CubeDimension cd) {
    XDimension xd = XCF.createXDimension();
    xd.setName(cd.getName());

    if (cd instanceof ReferencedDimension) {
      ReferencedDimension rd = (ReferencedDimension) cd;
      List<TableReference> dimRefs = rd.getReferences();
      xd.setReferences(xTabReferenceFromHiveTabReference(dimRefs));
      xd.setType(rd.getType());
      xd.setCost(rd.getCost());
    } else if (cd instanceof BaseDimension) {
      BaseDimension bd = (BaseDimension) cd;
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
   * @param xm
   * @return
   */
  public static CubeMeasure hiveMeasureFromXMeasure(XMeasure xm) {
    Date startDate = xm.getStartTime() == null ? null : xm.getStartTime().toGregorianCalendar().getTime();
    Date endDate = xm.getEndTime() == null ? null : xm.getEndTime().toGregorianCalendar().getTime();
    CubeMeasure cm;
    if (xm.getExpr() != null && !xm.getExpr().isEmpty()) {
      cm = new ExprMeasure(new FieldSchema(xm.getName(), xm.getType(), ""),
          xm.getExpr(),
          xm.getFormatString(),
          xm.getDefaultAggr(),
          "unit",
          startDate,
          endDate,
          xm.getCost()
          );
    } else {
      cm = new ColumnMeasure(new FieldSchema(xm.getName(), xm.getType(), ""),
          xm.getFormatString(),
          xm.getDefaultAggr(),
          "unit",
          startDate,
          endDate,
          xm.getCost()
          );
    }
    return cm;
  }

  /**
   * Convert JAXB properties to Map<String, String>
   * @param xProperties
   * @return
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

  public static List<TableReference> tableRefFromDimensionRef(DimensionReference drf) {
    if (drf == null) {
      return null;
    }

    List<TableReference> tabRefs = new ArrayList<TableReference>(drf.getTableReferences().size());
    for (XTablereference xTabRef : drf.getTableReferences()) {
      TableReference tabRef = new TableReference();
      tabRef.setDestTable(xTabRef.getDestTable());
      tabRef.setDestColumn(xTabRef.getDestColumn());
      tabRefs.add(tabRef);
    }

    return tabRefs;
  }

  public static Map<String, List<TableReference>> mapFromDimensionReferences(DimensionReferences dimRefs) {
    if (dimRefs != null && dimRefs.getDimReferences() != null && !dimRefs.getDimReferences().isEmpty()) {
      Map<String, List<TableReference>> cubeRefs = new LinkedHashMap<String, List<TableReference>>();
      for (DimensionReference drf : dimRefs.getDimReferences()) {
        List<TableReference> refs = cubeRefs.get(drf.getDimensionColumn().toLowerCase());
        if (refs == null) {
          refs = new ArrayList<TableReference>();
          cubeRefs.put(drf.getDimensionColumn().toLowerCase(), refs);
        }
        refs.addAll(tableRefFromDimensionRef(drf));
      }
      return cubeRefs;
    }
    return null;
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
      throw new WebApplicationException("Could not create storage class" + xs.getClassname() + "with name" + xs.getName(), e);
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
    dimTab.setName(cubeDimTable.getName());
    dimTab.setWeight(cubeDimTable.weight());

    Columns cols = XCF.createColumns();

    for (FieldSchema column : cubeDimTable.getColumns()) {
      cols.getColumns().add(columnFromFieldSchema(column));
    }
    dimTab.setColumns(cols);

    if (cubeDimTable.getDimensionReferences() != null && 
        !cubeDimTable.getDimensionReferences().isEmpty()) {
      DimensionReferences dimRefs = XCF.createDimensionReferences();
      for (Entry<String, List<TableReference>> entry : 
        cubeDimTable.getDimensionReferences().entrySet()) {
        DimensionReference ref = XCF.createDimensionReference();
        ref.setDimensionColumn(entry.getKey());
        ref.getTableReferences().addAll(dimRefListFromTabRefList(entry.getValue()));
        dimRefs.getDimReferences().add(ref);
      }
      dimTab.setDimensionsReferences(dimRefs);
    }

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
    Map<String, List<TableReference>> tabrefs = new HashMap<String, List<TableReference>>();

    if (dimensionTable.getDimensionsReferences() != null &&
        dimensionTable.getDimensionsReferences().getDimReferences() != null &&
        !dimensionTable.getDimensionsReferences().getDimReferences().isEmpty()) {
      for (DimensionReference drf : dimensionTable.getDimensionsReferences().getDimReferences()) {
        String col = drf.getDimensionColumn();
        List<TableReference> refs = tableRefFromDimensionRef(drf);
        List<TableReference> val = tabrefs.get(col);
        if (val == null) {
          tabrefs.put(col, refs);
        } else {
          val.addAll(refs);
        }
      }
    }

    CubeDimensionTable cdim = new CubeDimensionTable(dimensionTable.getName(),
        fieldSchemaListFromColumns(dimensionTable.getColumns()), 
        dimensionTable.getWeight(),
        dumpPeriodsFromUpdatePeriods(dimensionTable.getStorageDumpPeriods()),
        tabrefs,
        mapFromXProperties(dimensionTable.getProperties()));

    return cdim;
  }

  public static CubeFactTable cubeFactFromFactTable(FactTable fact) {
    List<FieldSchema> columns = fieldSchemaListFromColumns(fact.getColumns());

    Map<String, Set<UpdatePeriod>> storageUpdatePeriods = getFactUpdatePeriodsFromUpdatePeriods(fact.getStorageUpdatePeriods());

    return new CubeFactTable(Arrays.asList(fact.getCubeName()),
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
}
