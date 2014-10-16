package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@AllArgsConstructor
@NoArgsConstructor
public class ModelMetadata {
  @XmlElement @Getter
  private String modelID;

  @XmlElement @Getter
  private String table;

  @XmlElement @Getter
  private String algorithm;

  @XmlElement @Getter
  private String params;

  @XmlElement @Getter
  private String createdAt;

  @XmlElement @Getter
  private String modelPath;

  @XmlElement @Getter
  private String labelColumn;

  @XmlElement @Getter
  private String features;

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    builder.append("Algorithm: ").append(algorithm).append('\n');
    builder.append("Model ID: ").append(modelID).append('\n');
    builder.append("Training table: ").append(table).append('\n');
    builder.append("Features: ").append(features).append('\n');
    builder.append("Labelled Column: ").append(labelColumn).append('\n');
    builder.append("Training params: ").append(params).append('\n');
    builder.append("Created on: ").append(createdAt).append('\n');
    builder.append("Model saved at: ").append(modelPath).append('\n');
    return builder.toString();
  }
}
