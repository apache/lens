package org.apache.lens.api.ml;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * The Class ModelMetadata.
 */
@XmlRootElement
/**
 * Instantiates a new model metadata.
 *
 * @param modelID
 *          the model id
 * @param table
 *          the table
 * @param algorithm
 *          the algorithm
 * @param params
 *          the params
 * @param createdAt
 *          the created at
 * @param modelPath
 *          the model path
 * @param labelColumn
 *          the label column
 * @param features
 *          the features
 */
@AllArgsConstructor
/**
 * Instantiates a new model metadata.
 */
@NoArgsConstructor
public class ModelMetadata {

  /** The model id. */
  @XmlElement
  @Getter
  private String modelID;

  /** The table. */
  @XmlElement
  @Getter
  private String table;

  /** The algorithm. */
  @XmlElement
  @Getter
  private String algorithm;

  /** The params. */
  @XmlElement
  @Getter
  private String params;

  /** The created at. */
  @XmlElement
  @Getter
  private String createdAt;

  /** The model path. */
  @XmlElement
  @Getter
  private String modelPath;

  /** The label column. */
  @XmlElement
  @Getter
  private String labelColumn;

  /** The features. */
  @XmlElement
  @Getter
  private String features;

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
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
