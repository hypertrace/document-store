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
  public void testAddAndGetIntegerParam() {
    paramBuilder.addIntegerParam(1);
    paramBuilder.addIntegerParam(2);
    Params params = paramBuilder.build();
    Assertions.assertEquals(params.getIntegerParams().size(), 2);
  }

  @Test
  public void testAddAndGetLongParam() {
    paramBuilder.addLongParam(1L);
    paramBuilder.addLongParam(2L);
    Params params = paramBuilder.build();
    Assertions.assertEquals(params.getLongParams().size(), 2);
  }

  @Test
  public void testAddAndGetStringParam() {
    paramBuilder.addStringParam("Bob");
    paramBuilder.addStringParam("Alice");
    Params params = paramBuilder.build();
    Assertions.assertEquals(params.getStringParams().size(), 2);
  }

  @Test
  public void testAddAndGetFloatParam() {
    paramBuilder.addFloatParam(1L);
    paramBuilder.addFloatParam(2L);
    Params params = paramBuilder.build();
    Assertions.assertEquals(params.getFloatParams().size(), 2);
  }

  @Test
  public void testAddAndGetDoubleParam() {
    paramBuilder.addDoubleParam(1L);
    paramBuilder.addDoubleParam(2L);
    Params params = paramBuilder.build();
    Assertions.assertEquals(params.getDoubleParams().size(), 2);
  }

  @Test
  public void testAllParamsAndIndex() {
    paramBuilder.addIntegerParam(1);
    paramBuilder.addLongParam(2L);
    paramBuilder.addStringParam("Alice");
    paramBuilder.addFloatParam(3L);
    paramBuilder.addDoubleParam(4L);
    Params params = paramBuilder.build();
    int index = 0;
    Assertions.assertEquals(params.getIntegerParams().get(index++), 1);
    Assertions.assertEquals(params.getLongParams().get(index++), 2L);
    Assertions.assertEquals(params.getStringParams().get(index++), "Alice");
    Assertions.assertEquals(params.getFloatParams().get(index++), 3L);
    Assertions.assertEquals(params.getDoubleParams().get(index), 4L);
    Assertions.assertEquals(index, 4);
  }

}
