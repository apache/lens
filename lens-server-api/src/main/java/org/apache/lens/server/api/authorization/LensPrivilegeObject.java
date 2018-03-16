package org.apache.lens.server.api.authorization;

import lombok.Data;

@Data
public class LensPrivilegeObject {

  private final LensPrivilegeObjectType objectType;
  private final String cubeOrFactOrDim;
  private final String column;

  public LensPrivilegeObject(LensPrivilegeObjectType objectType, String cubeOrFactOrDim) {
    this(objectType, cubeOrFactOrDim, null);
  }

  public LensPrivilegeObject(LensPrivilegeObjectType objectType, String cubeOrFactOrDim, String column) {
    this.objectType = objectType;
    this.cubeOrFactOrDim = cubeOrFactOrDim;
    this.column = column;
  }

  public enum LensPrivilegeObjectType {
    DATABASE, CUBE, FACT, DIMENSION, DIMENSIONTABLE, COLUMN, NONE
  };

}


