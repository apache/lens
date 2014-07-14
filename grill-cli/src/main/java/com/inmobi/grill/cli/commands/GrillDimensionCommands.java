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
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.inmobi.grill.api.APIResult;
import com.inmobi.grill.api.metastore.*;
import com.inmobi.grill.client.GrillClient;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.List;

@Component
public class GrillDimensionCommands implements CommandMarker {
  private GrillClient client;


  public void setClient(GrillClient client) {
    this.client = client;
  }

  @CliCommand(value = "show dimensions", help = "show list of dimensions in database")
  public String showDimensions() {
    List<String> dimensions = client.getAllDimensions();
    if( dimensions != null) {
      return Joiner.on("\n").join(dimensions);
    } else {
      return "No Dimensions found";
    }
  }

  @CliCommand(value = "create dimension", help = "Create a new Dimension")
  public String createDimension(@CliOption(key = {"", "table"},
      mandatory = true, help = "<path to dimension-spec file>") String dimensionSpec) {
    File f = new File(dimensionSpec);

    if (!f.exists()) {
      return "dimension spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }
    APIResult result = client.createDimension(dimensionSpec);

    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "create dimension succeeded";
    } else {
      return "create dimension failed";
    }
  }

  @CliCommand(value = "drop dimension", help = "drop dimension")
  public String dropDimension(@CliOption(key = {"", "table"},
      mandatory = true, help = "dimension name to be dropped") String dimension) {
    APIResult result = client.dropDimension(dimension);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Successfully dropped " + dimension + "!!!";
    } else {
      return "Dropping dimension failed";
    }
  }


  @CliCommand(value = "update dimension", help = "update dimension")
  public String updateDimension(@CliOption(key = {"", "dimension"}, mandatory = true,
      help = "<dimension-name> <path to dimension-spec file>") String specPair) {
    Iterable<String> parts = Splitter.on(' ')
        .trimResults()
        .omitEmptyStrings()
        .split(specPair);
    String[] pair = Iterables.toArray(parts, String.class);
    if (pair.length != 2) {
      return "Syntax error, please try in following " +
          "format. create fact <fact spec path> <storage spec path>";
    }

    File f = new File(pair[1]);

    if (!f.exists()) {
      return "Fact spec path"
          + f.getAbsolutePath()
          + " does not exist. Please check the path";
    }

    APIResult result = client.updateDimension(pair[0], pair[1]);
    if (result.getStatus() == APIResult.Status.SUCCEEDED) {
      return "Update of " + pair[0] + " succeeded";
    } else {
      return "Update of " + pair[0] + " failed";
    }
  }

  @CliCommand(value = "describe dimension", help = "describe dimension")
  public String describeDimension(@CliOption(key = {"", "dimension"},
      mandatory = true, help = "<dimension-name>") String dimensionName) {

    XDimension dimension = client.getDimension(dimensionName);
    StringBuilder builder = new StringBuilder();
    builder.append("Dimension Name : ").append(dimension.getName()).append("\n");
    builder.append("Description : ").append(dimension.getDescription() != null ? dimension.getDescription() : "");
    if (dimension.getAttributes() != null) {
      builder.append("Dimensions  :").append("\n");
      builder.append("\t").append("name").append("\t").append("type").append("\t")
          .append("cost").append("\t").append("Expression").append("\t")
          .append("table references").append("\t").append("starttime(in miliseconds)")
          .append("\t").append("endtime(in miliseconds)").append("\n");
      for (XDimAttribute dim : dimension.getAttributes().getDimAttributes()) {
        builder.append("\t")
            .append(dim.getName()!=null ? dim.getName() : "").append("\t")
            .append(dim.getType()!=null? dim.getType(): "").append("\t")
            .append(dim.getCost()!= null ? dim.getCost() : "").append("\t")
            .append(dim.getExpr()!= null ? dim.getExpr() : "").append("\t")
            .append(dim.getReferences()!= null? getXtableString(dim.getReferences().getTableReferences()) : "")
            .append(dim.getStartTime()!=null ? dim.getStartTime().toGregorianCalendar().getTimeInMillis(): "").append("\t")
            .append(dim.getEndTime()!=null?dim.getEndTime().toGregorianCalendar().getTimeInMillis():"").append("\t")
            .append("\n");
      }
    }
    builder.append(FormatUtils.formatProperties(dimension.getProperties().getProperties()));
    return builder.toString();

  }

  private String getXtableString(List<XTablereference> tableReferences) {
    StringBuilder builder = new StringBuilder();
    for (XTablereference ref : tableReferences) {
      builder.append(ref.getDestTable())
          .append(".")
          .append(ref.getDestColumn())
          .append(",");
    }
    return builder.toString();
  }
}
