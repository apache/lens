package com.inmobi.yoda.cube.ddl;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.inmobi.dw.yoda.tools.util.cube.CubeDefinitionReader;

public class CubeReader {
  private final CubeDefinitionReader reader;
  private final Properties props = new Properties();

  public CubeReader() throws IOException {
    reader = CubeDefinitionReader.get();
    props.load(Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("cube_defn.properties"));
  }

  public CubeReader(final String propertyFilePath) throws IOException { 
    reader = CubeDefinitionReader.get(propertyFilePath);
    props.load(new FileInputStream(propertyFilePath));
  }

  public CubeReader(final Properties changes) throws IOException {
    reader = CubeDefinitionReader.get(changes);
    props.load(Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("cube_defn.properties"));
    for (final Object key : changes.keySet()) {
      props.put(key, changes.get(key));
    }
  }

  /**
   * @return the reader
   */
  public CubeDefinitionReader getReader() {
    return reader;
  }

  public Properties getProps() {
    return props;
  }

}
