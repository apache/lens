package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class TestReport {
  @XmlElement @Getter private String testTable;
  @XmlElement @Getter private String outputTable;
  @XmlElement @Getter private String outputColumn;
  @XmlElement @Getter private String labelColumn;
  @XmlElement @Getter private String featureColumns;
  @XmlElement @Getter private String algorithm;
  @XmlElement @Getter private String modelID;
  @XmlElement @Getter private String reportID;
  @XmlElement @Getter private String queryID;

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Input test table: ").append(testTable).append('\n');
    builder.append("Algorithm: ").append(algorithm).append('\n');
    builder.append("Report id: ").append(reportID).append('\n');
    builder.append("Model id: ").append(modelID).append('\n');
    builder.append("Grill Query id: ").append(queryID).append('\n');
    builder.append("Feature columns: ").append(featureColumns).append('\n');
    builder.append("Labelled column: ").append(labelColumn).append('\n');
    builder.append("Predicted column: ").append(outputColumn).append('\n');
    builder.append("Test output table: ").append(outputTable).append('\n');
    return builder.toString();
  }
}
