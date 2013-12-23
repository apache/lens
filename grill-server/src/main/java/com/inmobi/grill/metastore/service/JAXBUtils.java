package com.inmobi.grill.metastore.service;


import com.inmobi.grill.metastore.model.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

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
    for (XDimension xd : cube.getDimensions().getDimension()) {
      dims.add(hiveDimFromXDim(xd));
    }

    Set<CubeMeasure> measures = new LinkedHashSet<CubeMeasure>();
    for (XMeasure xm : cube.getMeasures().getMeasure()) {
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
    List<XMeasure> xmsList = xms.getMeasure();
    for (CubeMeasure cm : c.getMeasures()) {
      xmsList.add(xMeasureFromHiveMeasure(cm));
    }
    xc.setMeasures(xms);


    XDimensions xdm = XCF.createXDimensions();
    List<XDimension> xdmList = xdm.getDimension();
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
    Date startDate = getDateFromXML(xd.getStarttime());
    Date endDate = getDateFromXML(xd.getEndtime());

    CubeDimension hiveDim;

    if (xd.getReferences() != null && xd.getReferences().getTablereference() != null &&
      !xd.getReferences().getTablereference().isEmpty()) {

      List<TableReference> dimRefs = new ArrayList<TableReference>(xd.getReferences().getTablereference().size());

      for (XTablereference xRef : xd.getReferences().getTablereference()) {
        dimRefs.add(new TableReference(xRef.getDesttable(), xRef.getDestcolumn()));
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
    xm.setDefaultaggr(cm.getAggregate());
    xm.setFormatString(cm.getFormatString());
    xm.setType(cm.getType());
    xm.setUnit(cm.getUnit());
    xm.setCost(cm.getCost());
    xm.setStarttime(getXMLGregorianCalendar(cm.getStartTime()));
    xm.setEndtime(getXMLGregorianCalendar(cm.getEndTime()));

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
    List<XTablereference> xrefList = xrefs.getTablereference();

    for (TableReference hRef : hiveRefs) {
      XTablereference xref = XCF.createXTablereference();
      xref.setDesttable(hRef.getDestTable());
      xref.setDestcolumn(hRef.getDestColumn());
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
    Date startDate = xm.getStarttime() == null ? null : xm.getStarttime().toGregorianCalendar().getTime();
    Date endDate = xm.getEndtime() == null ? null : xm.getEndtime().toGregorianCalendar().getTime();
    CubeMeasure cm;
    if (xm.getExpr() != null && !xm.getExpr().isEmpty()) {
      cm = new ExprMeasure(new FieldSchema(xm.getName(), xm.getType(), ""),
        xm.getExpr(),
        xm.getFormatString(),
        xm.getDefaultaggr(),
        "unit",
        startDate,
        endDate,
        xm.getCost()
      );
    } else {
      cm = new ColumnMeasure(new FieldSchema(xm.getName(), xm.getType(), ""),
        xm.getFormatString(),
        xm.getDefaultaggr(),
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
    if (xProperties != null && xProperties.getProperty() != null &&
        !xProperties.getProperty().isEmpty()) {
      Map<String, String> properties = new HashMap<String, String>();
      for (XProperty xp : xProperties.getProperty()) {
        properties.put(xp.getName(), xp.getValue());
      }
      return properties;
    }
    return null;
  }

  /**
   * Convert string map to XProperties
   */
  public static XProperties xPropertiesFromMap(Map<String, String> map) {
    if (map != null && !map.isEmpty()) {
      XProperties xp = XCF.createXProperties();
      List<XProperty> xpList = xp.getProperty();
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

    List<TableReference> tabRefs = new ArrayList<TableReference>(drf.getTableReference().size());
    for (XTablereference xTabRef : drf.getTableReference()) {
      TableReference tabRef = new TableReference();
      tabRef.setDestTable(xTabRef.getDesttable());
      tabRef.setDestColumn(xTabRef.getDestcolumn());
      tabRefs.add(tabRef);
    }

    return tabRefs;
  }

  public static Map<String, List<TableReference>> mapFromDimensionReferences(DimensionReferences dimRefs) {
    if (dimRefs != null && dimRefs.getReference() != null && !dimRefs.getReference().isEmpty()) {
      Map<String, List<TableReference>> cubeRefs = new LinkedHashMap<String, List<TableReference>>();
      for (DimensionReference drf : dimRefs.getReference()) {
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

  public static List<FieldSchema> fieldSchemaListFromColumns(Columns columns) {
    if (columns != null && columns.getColumns() != null && !columns.getColumns().isEmpty()) {
      List<FieldSchema> fsList = new ArrayList<FieldSchema>(columns.getColumns().size());
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

  public static Map<String, UpdatePeriod> dumpPeriodsFromUpdatePeriods(UpdatePeriods periods) {
    if (periods != null && periods.getUpdatePeriodElement() != null && !periods.getUpdatePeriodElement().isEmpty()) {
      Map<String, UpdatePeriod> map = new LinkedHashMap<String, UpdatePeriod>();

      for (UpdatePeriodElement upd : periods.getUpdatePeriodElement()) {
        UpdatePeriod updatePeriod = UpdatePeriod.valueOf(upd.getUpdatePeriod().toUpperCase());
        map.put(upd.getStorageAttr().getName(), updatePeriod);
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
      storage = Storage.createInstance("TODO", "TODO");
    } catch (HiveException e) {
      e.printStackTrace();
    }


    for (Column c : xs.getPartCols()) {
      //storage.addToPartCols(fieldSchemaFromColumn(c));
    }
    return storage;
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
				ref.getTableReference().addAll(dimRefListFromTabRefList(entry.getValue()));
				dimRefs.getReference().add(ref);
			}
			dimTab.setDimensionsReferences(dimRefs);
		}

		dimTab.setProperties(xPropertiesFromMap(cubeDimTable.getProperties()));
		
		Map<String, UpdatePeriod> storageToUpdatePeriod = cubeDimTable.getSnapshotDumpPeriods();
		if (storageToUpdatePeriod != null && !storageToUpdatePeriod.isEmpty()) {
			UpdatePeriods periods = XCF.createUpdatePeriods();
			
			for (Entry<String, UpdatePeriod> entry : storageToUpdatePeriod.entrySet()) {
				UpdatePeriodElement e = XCF.createUpdatePeriodElement();
				e.setUpdatePeriod(entry.getValue().toString());
				XStorage xStorage = XCF.createXStorage();
				// Only the name has to be returned in the result, other attributes are not necessary
				xStorage.setName(entry.getKey());
				e.setStorageAttr(xStorage);
				periods.getUpdatePeriodElement().add(e);
			}
			dimTab.setUpdatePeriods(periods);
		}
		
		return dimTab;
	}

	public static List<? extends XTablereference> dimRefListFromTabRefList(
			List<TableReference> tabRefs) {
		if (tabRefs != null && !tabRefs.isEmpty()) {
			List<XTablereference> xTabRefs = new ArrayList<XTablereference>(tabRefs.size());
			for (TableReference ref : tabRefs) {
				XTablereference xRef = XCF.createXTablereference();
				xRef.setDestcolumn(ref.getDestColumn());
				xRef.setDesttable(ref.getDestTable());
				xTabRefs.add(xRef);
			}
			return xTabRefs;
		}
		
		return null;
	}

	public static CubeDimensionTable cubeDimTableFromDimTable(DimensionTable dimensionTable) {
		Map<String, UpdatePeriod> storageToUpdatePeriod = new HashMap<String, UpdatePeriod>();
		if (dimensionTable.getUpdatePeriods() != null
				&& dimensionTable.getUpdatePeriods().getUpdatePeriodElement() != null
				&& !dimensionTable.getUpdatePeriods().getUpdatePeriodElement().isEmpty()) {
			for (UpdatePeriodElement upd : dimensionTable.getUpdatePeriods().getUpdatePeriodElement()) {
				upd.getUpdatePeriod();
				upd.getStorageAttr().getName();
				storageToUpdatePeriod.put(upd.getStorageAttr().getName(),
						UpdatePeriod.valueOf(upd.getUpdatePeriod().toUpperCase()));
			}
		}
		
		Map<String, List<TableReference>> tabrefs = new HashMap<String, List<TableReference>>();
		
		if (dimensionTable.getDimensionsReferences() != null &&
				dimensionTable.getDimensionsReferences().getReference() != null &&
				!dimensionTable.getDimensionsReferences().getReference().isEmpty()) {
			for (DimensionReference drf : dimensionTable.getDimensionsReferences().getReference()) {
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
				storageToUpdatePeriod,
				tabrefs,
				mapFromXProperties(dimensionTable.getProperties()));
		
		return cdim;
	}
	
	public static CubeFactTable cubeFactFromFactTable(FactTable fact) {
		List<FieldSchema> columns = fieldSchemaListFromColumns(fact.getColumns());
		
		UpdatePeriods periods = fact.getUpdatePeriods();
		List<UpdatePeriodElement> updList = periods.getUpdatePeriodElement();

		Map<String, Set<UpdatePeriod>> storageUpdatePeriods = new HashMap<String, Set<UpdatePeriod>>();
		for (UpdatePeriodElement upd : updList) {
			UpdatePeriod updPeriod = UpdatePeriod.valueOf(upd.getUpdatePeriod().toUpperCase());
			String storage = upd.getStorageAttr().getName();
			
			Set<UpdatePeriod> updSet = storageUpdatePeriods.get(storage);
			if (updSet == null) {
				updSet = new TreeSet<UpdatePeriod>();
				storageUpdatePeriods.put(storage, updSet);
			}
			updSet.add(updPeriod);
		}
		
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
				String storage = e.getKey();
				XStorage xs = XCF.createXStorage();
				xs.setName(storage);
				for (UpdatePeriod p : e.getValue()) {
					UpdatePeriodElement uel = XCF.createUpdatePeriodElement();
					uel.setStorageAttr(xs);
					uel.setUpdatePeriod(p.toString());
					periods.getUpdatePeriodElement().add(uel);
				}
			}
			fact.setUpdatePeriods(periods);
		}
		return fact;
	}
}
