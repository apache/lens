package com.inmobi.yoda.cube.ddl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.inmobi.dw.yoda.tools.util.cube.CubeDefinitionReader;

public class CreateCube {
  private static final Log LOG = LogFactory.getLog(
      CreateCube.class);

  public static String cubeStorageSchema = "network_object.proto";
  private final String cube;
  private final CubeDefinitionReader cubeReader;
  private final Properties allProps;
  
  public CreateCube(String cubeName, CubeDefinitionReader cubeReader,
      Properties properties) {
    this.cube = cubeName;
    this.cubeReader = cubeReader;
    this.allProps = properties;
  }

  private void create() {
    Set<String> cubeDimensions = new HashSet<String>();
    Set<String> cubeMeasures = new HashSet<String>();
    Map<String, String> cubeProperties = getProperties();
    cubeDimensions = cubeReader.getAllDimensionNames(cube);
    cubeMeasures = cubeReader.getAllMeasureNames(cube);

    //new Cube(cube, cubeDimensions, cubeMeasures, cubeProperties);
    Map<String, Set<String>> summaryDimensions = new HashMap<String, Set<String>>();
    Map<String, Set<String>> summaryMeasures = new HashMap<String, Set<String>>();
    Map<String, Map<String, String>> summaryProperties =
        new HashMap<String, Map<String, String>>();    
    Set<String> summaryList = cubeReader.getSummaryNames(cube);
    for (String summary : summaryList) {
      Set<String> dimensions = new HashSet<String>();
      Set<String> measures = new HashSet<String>();
      Map<String, String> properties = new HashMap<String, String>();
      readCubeFact(summary, dimensions, measures, properties);
      summaryDimensions.put(summary, dimensions);
      summaryMeasures.put(summary, measures);
      summaryProperties.put(summary, getSummaryProperties(summary));
    }
  }

  private Map<String, String> getProperties() {
    return getProperties("cube." + cube, allProps);
  }

  private Map<String, String> getSummaryProperties(String fact) {
    return getProperties("cube." + cube + ".summary." + fact, allProps);
  }

  static Map<String, String> getProperties(String prefix, Properties allProps) {
    Map<String, String> props = new HashMap<String, String>();
    for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
      String key = entry.toString();
      if (key.startsWith(prefix)) {
        props.put(key, entry.getValue().toString());
      }
    }
    return props;
  }

  private void readCubeFact(String factName,
      Set<String> dimensions, Set<String> measures,
      Map<String, String> properties) {
    dimensions = cubeReader.getSummaryDimensionNames(cube, factName);
    measures = cubeReader.getSummaryMeasureNames(cube, factName);    
  }

  public static void main(String[] args) throws IOException {
    CubeReader cubeReader = new CubeReader();
    CreateCube cc;

    if (args[0] == null) {
      LOG.info("Creating all cubes " + cubeReader.getReader().getCubeNames());
      for (String cubeName : cubeReader.getReader().getCubeNames()) {
        cc = new CreateCube(cubeName, cubeReader.getReader(),
            cubeReader.getProps());
        cc.create();        
      }
    } else {
      if (!cubeReader.getReader().getCubeNames().contains(args[0])) {
        LOG.error("Cube " + args[0] + " is not defined in cubeDefinition");
        return;
      } else {
        LOG.info("Creating cube " + args[0]);
        cc = new CreateCube(args[0], cubeReader.getReader(),
            cubeReader.getProps());
        cc.create();        
      }
    }
  }
}
