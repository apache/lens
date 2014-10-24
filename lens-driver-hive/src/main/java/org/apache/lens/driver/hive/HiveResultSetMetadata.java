package org.apache.lens.driver.hive;

import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hive.service.cli.ColumnDescriptor;
import org.apache.lens.server.api.driver.LensResultSetMetadata;

import java.util.List;

/**
 * The top level Result Set metadata class which is used by the jackson to
 * serialize to JSON.
 */

/**
 * Instantiates a new hive result set metadata.
 */
@NoArgsConstructor
public class HiveResultSetMetadata extends LensResultSetMetadata {

  /** The columns. */
  @Setter
  private List<ColumnDescriptor> columns;

  @Override
  public List<ColumnDescriptor> getColumns() {
    return columns;
  }
}
