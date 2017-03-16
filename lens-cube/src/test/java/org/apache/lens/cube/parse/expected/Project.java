package org.apache.lens.cube.parse.expected;

import lombok.Builder;
import lombok.Data;

/**
 * Created on 15/03/17.
 */
@Data
@Builder
class Project {
  final String expression;
  String alias;
}
