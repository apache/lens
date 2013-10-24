package com.inmobi.grill.metastore.service;


import com.inmobi.grill.metastore.model.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.util.*;

/**
 * Utilities for converting to and from JAXB types to hive.ql.metadata.cube types
 */
public class JAXBUtils {
  public static final Logger LOG = LogManager.getLogger(JAXBUtils.class);
  private static final ObjectFactory xCubeObjectFactory = new ObjectFactory();

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
    XCube xc = xCubeObjectFactory.createXCube();
    xc.setName(c.getName());
    xc.setWeight(c.weight());

    XMeasures xms = xCubeObjectFactory.createXMeasures();
    List<XMeasure> xmsList = xms.getMeasure();
    for (CubeMeasure cm : c.getMeasures()) {
      xmsList.add(xMeasureFromHiveMeasure(cm));
    }
    xc.setMeasures(xms);


    XDimensions xdm = xCubeObjectFactory.createXDimensions();
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

    XMeasure xm = xCubeObjectFactory.createXMeasure();
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
    XDimension xd = xCubeObjectFactory.createXDimension();
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
    XTablereferences xrefs = xCubeObjectFactory.createXTablereferences();
    List<XTablereference> xrefList = xrefs.getTablereference();

    for (TableReference hRef : hiveRefs) {
      XTablereference xref = xCubeObjectFactory.createXTablereference();
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
      XProperties xp = xCubeObjectFactory.createXProperties();
      List<XProperty> xpList = xp.getProperty();
      for (Map.Entry<String, String> e : map.entrySet()) {
        XProperty property = xCubeObjectFactory.createXProperty();
        property.setName(e.getKey());
        property.setValue(e.getValue());
        xpList.add(property);
      }

      return xp;
    }
    return null;
  }
}
