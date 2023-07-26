package org.hypertrace.core.documentstore.model.config;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ConnectionConfigTest {
  @Test
  void testMissingType_throwsException() {
    assertThrows(IllegalArgumentException.class, () -> ConnectionConfig.builder().build());
  }

  @Test
  void testInvalidType_throwsException() {
    assertThrows(
        IllegalArgumentException.class, () -> ConnectionConfig.builder().type("invalid").build());
  }
}
