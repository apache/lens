package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

@NoArgsConstructor
public abstract class MLModel<PREDICTION> implements Serializable {
  @Getter
  @Setter
  private String id;

  @Getter
  @Setter
  private Date createdAt;

  @Getter
  @Setter
  private String trainerName;

  @Getter
  @Setter
  private String table;

  @Getter
  @Setter
  private List<String> params;

  @Getter
  @Setter
  private String labelColumn;

  @Getter
  @Setter
  private List<String> featureColumns;

  public abstract PREDICTION predict(Object ... args);
}
