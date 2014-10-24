package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * Instantiates a new ML test report.
 */
@NoArgsConstructor
public class MLTestReport implements Serializable {

  /** The test table. */
  @Getter
  @Setter
  private String testTable;

  /** The output table. */
  @Getter
  @Setter
  private String outputTable;

  /** The output column. */
  @Getter
  @Setter
  private String outputColumn;

  /** The label column. */
  @Getter
  @Setter
  private String labelColumn;

  /** The feature columns. */
  @Getter
  @Setter
  private List<String> featureColumns;

  /** The algorithm. */
  @Getter
  @Setter
  private String algorithm;

  /** The model id. */
  @Getter
  @Setter
  private String modelID;

  /** The report id. */
  @Getter
  @Setter
  private String reportID;

  /** The query id. */
  @Getter
  @Setter
  private String queryID;

  /** The test output path. */
  @Getter
  @Setter
  private String testOutputPath;

  /** The prediction result column. */
  @Getter
  @Setter
  private String predictionResultColumn;

  /** The lens query id. */
  @Getter
  @Setter
  private String lensQueryID;
}
