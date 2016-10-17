/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lens.server.api.query.comparators;

import static org.testng.Assert.*;

import java.util.Comparator;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import lombok.Data;

public class ChainedComparatorTest {
  @Data
  private static class Tuple {
    final Integer a, b, c;
  }

  private Tuple tuple(Integer a, Integer b, Integer c) {
    return new Tuple(a, b, c);
  }

  public static final ChainedComparator<Tuple> COMPARATOR = new ChainedComparator<>(Lists.newArrayList(
    new Comparator<Tuple>() {
      @Override
      public int compare(Tuple o1, Tuple o2) {
        return o1.getA().compareTo(o2.getA());
      }
    },
    new Comparator<Tuple>() {

      @Override
      public int compare(Tuple o1, Tuple o2) {
        return o1.getB().compareTo(o2.getB());
      }
    },
    new Comparator<Tuple>() {
      @Override
      public int compare(Tuple o1, Tuple o2) {
        return o1.getC().compareTo(o2.getC());
      }
    }
  ));

  @DataProvider
  public Object[][] comparisonData() {
    return new Object[][]{
      {tuple(0, 0, 0), tuple(0, 0, 0), 0},
      {tuple(0, 0, 1), tuple(0, 0, 0), 1},
      {tuple(0, 0, 1), tuple(0, 0, 4), -1},
      {tuple(0, 0, 1), tuple(1, 0, 4), -1},
      {tuple(0, 0, 1), tuple(0, -10, 4), 1},
    };
  }

  @Test(dataProvider = "comparisonData")
  public void testCompare(Tuple a, Tuple b, int expected) throws Exception {
    assertEquals(COMPARATOR.compare(a, b), expected);
  }
}
