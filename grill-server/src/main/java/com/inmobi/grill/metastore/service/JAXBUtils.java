package com.inmobi.grill.metastore.service;


import com.inmobi.grill.metastore.model.*;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.*;

import java.util.*;

public class JAXBUtils {
  /**
   * Create a hive ql cube obejct from corresponding JAXB object
   * @param cube JAXB Cube
   * @return
   */
  public static Cube xCubeToHiveCube(XCube cube) {
    Set<CubeDimension> dims = new LinkedHashSet<CubeDimension>();
    for (XDimension xd : cube.getDimensions().getDimension()) {
      dims.add(xDimToHiveCubeDim(xd));
    }

    Set<CubeMeasure> measures = new LinkedHashSet<CubeMeasure>();
    for (XMeasure xm : cube.getMeasures().getMeasure()) {
      measures.add(xMeasureToHiveCubeMeasure(xm));
    }

    Map<String, String> properties = xPropertiesToMap(cube.getProperties());
    double cubeWeight = cube.getWeight() == null ? 0d : cube.getWeight();
    return new Cube(cube.getName(), measures, dims, properties, cubeWeight);
  }

  /**
   * Create a hive ql CubeDimension from JAXB counterpart
   * @param xd
   * @return
   */
  public static CubeDimension xDimToHiveCubeDim(XDimension xd) {
    Date startDate = xd.getStarttime() == null ? null : xd.getStarttime().toGregorianCalendar().getTime();
    Date endDate = xd.getEndtime() == null ? null : xd.getEndtime().toGregorianCalendar().getTime();
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

  /**
   * Create hive ql CubeMeasure from JAXB counterpart
   * @param xm
   * @return
   */
  public static CubeMeasure xMeasureToHiveCubeMeasure(XMeasure xm) {
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
  public static Map<String, String> xPropertiesToMap(XProperties xProperties) {
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
}
