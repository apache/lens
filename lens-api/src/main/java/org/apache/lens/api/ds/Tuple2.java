package org.apache.lens.api.ds;

import lombok.Data;

/**
 * Created on 20/03/17.
 */
@Data(staticConstructor = "of")
public class Tuple2<A, B> {
  final A _1;
  final B _2;

}
