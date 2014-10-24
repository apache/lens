package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Instantiates a new ML model.
 */
@NoArgsConstructor
public abstract class MLModel<PREDICTION> implements Serializable {

  /** The id. */
  @Getter
  @Setter
  private String id;

  /** The created at. */
  @Getter
  @Setter
  private Date createdAt;

  /** The trainer name. */
  @Getter
  @Setter
  private String trainerName;

  /** The table. */
  @Getter
  @Setter
  private String table;

  /** The params. */
  @Getter
  @Setter
  private List<String> params;

  /** The label column. */
  @Getter
  @Setter
  private String labelColumn;

  /** The feature columns. */
  @Getter
  @Setter
  private List<String> featureColumns;

  /**
   * Predict.
   *
   * @param args
   *          the args
   * @return the prediction
   */
  public abstract PREDICTION predict(Object... args);
}
