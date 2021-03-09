package org.hypertrace.core.documentstore.postgres;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ParamsTest {

  Params.Builder paramBuilder;

  @BeforeEach
  void setUp() {
    paramBuilder = Params.newBuilder();
  }

  @Test
  public void testAllParamsAndIndex() {
    paramBuilder.addObjectParam(1);
    paramBuilder.addObjectParam(2L);
    paramBuilder.addObjectParam("Alice");
    paramBuilder.addObjectParam(3L);
    paramBuilder.addObjectParam(4L);
    Params params = paramBuilder.build();
    int index = 1;
    Assertions.assertEquals(params.getObjectParams().get(index++), 1);
    Assertions.assertEquals(params.getObjectParams().get(index++), 2L);
    Assertions.assertEquals(params.getObjectParams().get(index++), "Alice");
    Assertions.assertEquals(params.getObjectParams().get(index++), 3L);
    Assertions.assertEquals(params.getObjectParams().get(index), 4L);
    Assertions.assertEquals(index, 5);
  }
}
