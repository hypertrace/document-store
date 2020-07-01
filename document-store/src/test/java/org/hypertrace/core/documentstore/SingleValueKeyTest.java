package org.hypertrace.core.documentstore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SingleValueKeyTest {

  @Test
  public void testSingleValueKeyToString() {
    SingleValueKey singleValueKey1 = new SingleValueKey("tenant1", "key1");
    Assertions.assertEquals("tenant1:key1", singleValueKey1.toString());
  }

  @Test
  public void testSingleValueKeyEqualsHashCode() {
    SingleValueKey singleValueKey1 = new SingleValueKey("tenant1", "key1");
    SingleValueKey singleValueKey2 = new SingleValueKey("tenant1", "key1");
    SingleValueKey singleValueKey3 = new SingleValueKey("tenant1", "key2");
    SingleValueKey singleValueKey4 = new SingleValueKey("tenant2", "key1");

    Assertions.assertEquals(singleValueKey1, singleValueKey2);
    Assertions.assertNotEquals(singleValueKey1, singleValueKey3);
    Assertions.assertNotEquals(singleValueKey1, singleValueKey4);

    Assertions.assertEquals(singleValueKey1.hashCode(), singleValueKey2.hashCode());
  }
}
