package com.inmobi.grill.cli;

import java.io.File;

public class TestUtil {

  public static File getPath(String name) {
    File f = new File(".",
        "../grill-examples/src/main/resources/"+name);
    return f;
  }
}
