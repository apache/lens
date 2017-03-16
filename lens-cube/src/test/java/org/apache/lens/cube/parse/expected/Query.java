package org.apache.lens.cube.parse.expected;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import lombok.Builder;
import lombok.Data;

/**
 * Created on 15/03/17.
 */
@Data
@Builder
public class Query {

  final List<Project> projects = Lists.newArrayList();
  final Table from;
  final List<String> wheres = Lists.newArrayList();
  final List<String> havings = Lists.newArrayList();
  final List<String> groupBys = Lists.newArrayList();

  Query project(Project... projects) {
    Collections.addAll(this.projects, projects);
    return this;
  }

  Query where(String... wheres) {
    Collections.addAll(this.wheres, wheres);
    return this;
  }

  Query groupBy(String... gbys) {
    Collections.addAll(this.groupBys, gbys);
    return this;
  }

  Query having(String... havings) {
    Collections.addAll(this.havings, havings);
    return this;
  }

  public String toString() {
    return "";
  }
}
