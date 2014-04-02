package com.inmobi.grill.cli.commands;

/*
 * #%L
 * Grill CLI
 * %%
 * Copyright (C) 2014 Inmobi
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Joiner;
import com.inmobi.grill.api.metastore.*;

import java.util.List;

public class FormatUtils {
  public static String getStorageString(XStorageTableElement element) {
    StringBuilder buf = new StringBuilder();
    buf.append("Name ").append(element.getStorageName()).append("\n");
    buf.append("Update Period : ").append("\n");
    buf.append(Joiner.on(",").join(element.getUpdatePeriods()));
    XStorageTableDesc desc = element.getTableDesc();
    buf.append("Location :  ").append(desc.getTableLocation()).append("\n");
    buf.append("External : ").append(desc.isExternal()).append("\n");
    buf.append("Escape character").append(desc.getEscapeChar()).append("\n");
    buf.append("Field delimiter").append(desc.getFieldDelimiter()).append("\n");
    buf.append("Line delimiter").append(desc.getLineDelimiter()).append("\n");
    buf.append("Map Key delimiter").append(desc.getMapKeyDelimiter()).append("\n");
    buf.append("Input format").append(desc.getInputFormat()).append("\n");
    buf.append("Output Format").append(desc.getOutputFormat()).append("\n");
    buf.append("Storage Handler").append(desc.getLineDelimiter()).append("\n");

    buf.append("Columns: ").append("\n").append("\n\n");
    buf.append("\t")
        .append("NAME")
        .append("\t")
        .append("TYPE")
        .append("\t")
        .append("COMMENTS")
        .append("\n");
    for (Column col : desc.getPartCols().getColumns()) {
      buf.append("\t")
          .append(col.getName())
          .append("\t")
          .append(col.getType())
          .append("\t")
          .append(col.getComment())
          .append("\n");
    }

    buf.append("Table Params : ").append("\n");
    buf.append("\t").append("key=value").append("\n").append("\n\n");
    for (XProperty property : desc.getTableParameters().getProperties()) {
      buf.append("\t")
          .append(property.getName())
          .append("=")
          .append(property.getValue())
          .append("\n");
    }
    buf.append("Table Serde Name: ").append(desc.getSerdeClassName()).append("\n");
    buf.append("Table Serde Params : ").append("\n");
    buf.append("\t").append("key=value").append("\n");
    for (XProperty property : desc.getSerdeParameters().getProperties()) {
      buf.append("\t")
          .append(property.getName())
          .append("=")
          .append(property.getValue())
          .append("\n");
    }
    return buf.toString();
  }

  public static String formatPartition(XPartition partition) {
    StringBuilder b = new StringBuilder();
    b.append("Name :").append(partition.getName()).append("\n");
    b.append("Cube Name: ").append(partition.getCubeTableName() != null ? partition.getCubeTableName() : "").append("\n");
    b.append("Update Period: ").append(partition.getUpdatePeriod() != null ? partition.getUpdatePeriod() : "").append("\n");
    b.append("Input Format: ").append(partition.getInputFormat() != null ? partition.getInputFormat() : "").append("\n");
    b.append("Output Format: ").append(partition.getOutputFormat() != null ? partition.getOutputFormat() : "").append("\n");
    b.append("Serde className: ").append(partition.getSerdeClassname() != null ? partition.getSerdeClassname() : "").append("\n");
    b.append("Non Time Partition Spec: ").append("\n");
    b.append(partition.getNonTimePartitionSpec() != null ? formatPartNSpec(partition.getNonTimePartitionSpec()) : "").append("\n");
    b.append("Time Partition Spec : ").append("\n");
    b.append(partition.getTimePartitionSpec() != null ? formatPartSpec(partition.getTimePartitionSpec()) : "").append("\n");
    b.append("Partition Parameters").append("\n")
        .append(partition.getPartitionParameters() != null ? formatProperties(partition.getPartitionParameters().getProperties()) : "").append("\n");
    b.append("Serde Parameters").append("\n")
        .append(partition.getSerdeParameters() != null ? formatProperties(partition.getSerdeParameters().getProperties()) : "").append("\n");
    return b.toString();
  }

  public static String formatPartNSpec(XPartSpec nonTimePartitionSpec) {
    StringBuilder b = new StringBuilder();
    b.append("\t").append("Key = value").append("\n").append("\n\n");
    for (XPartSpecElement ele : nonTimePartitionSpec.getPartSpecElement()) {
      b.append("\t").append(ele.getKey()).append("=").append(ele.getValue()).append("\n");
    }
    return b.toString();
  }

  public static String formatPartSpec(XTimePartSpec timePartitionSpec) {
    StringBuilder b = new StringBuilder();
    b.append("\t").append("Key=value").append("\n").append("\n\n");
    for (XTimePartSpecElement ele : timePartitionSpec.getPartSpecElement()) {
      b.append("\t").append(ele.getKey()).append("=")
          .append(ele.getValue().toGregorianCalendar().getTime()).append("\n");
    }
    return b.toString();

  }

  public static String formatProperties(List<XProperty> properties) {

    StringBuilder b = new StringBuilder();
    b.append("Properties : ").append("\n");
    b.append("\t").append("key=value").append("\n");
    for (XProperty property : properties) {
      b.append("\t")
          .append(property.getName())
          .append("=")
          .append(property.getValue())
          .append("\n");
    }
    return b.toString();
  }

  public static String formatPartitions(List<XPartition> partitions) {
    StringBuilder builder = new StringBuilder();

    for (XPartition partition : partitions) {
      builder.append(formatPartition(partition)).append("\n\n");
    }
    return builder.toString();
  }
}
