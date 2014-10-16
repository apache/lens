package org.apache.lens.ml;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@NoArgsConstructor
public class MLTestReport implements Serializable {
  @Getter @Setter
  private String testTable;

  @Getter @Setter
  private String outputTable;

  @Getter @Setter
  private String outputColumn;

  @Getter @Setter
  private String labelColumn;

  @Getter @Setter
  private List<String> featureColumns;

  @Getter @Setter
  private String algorithm;

  @Getter @Setter
  private String modelID;

  @Getter @Setter
  private String reportID;

  @Getter @Setter
  private String queryID;

  @Getter @Setter
  private String testOutputPath;

  @Getter @Setter
  private String predictionResultColumn;

  @Getter @Setter
  private String grillQueryID;
}
