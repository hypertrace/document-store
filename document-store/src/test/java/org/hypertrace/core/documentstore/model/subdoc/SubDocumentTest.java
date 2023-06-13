package org.hypertrace.core.documentstore.model.subdoc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SubDocumentTest {

  @MethodSource
  private static Stream<Arguments> validPathsProvider() {
    return Stream.of(
        Arguments.of("price"),
        Arguments.of("1234567890"),
        Arguments.of("price.quantity"),
        Arguments.of("67aa7623-fc40-4112-b958-cbd6d4c077b1"),
        Arguments.of("first_name"));
  }

  @MethodSource
  private static Stream<Arguments> inValidPathsProvider() {
    return Stream.of(
        Arguments.of("price::quantity"),
        Arguments.of("{hello_world"),
        Arguments.of("hello_world}"),
        Arguments.of("createdTime"),
        Arguments.of("lastUpdatedTime"));
  }

  @ParameterizedTest
  @MethodSource("validPathsProvider")
  void testValidSubDocumentPaths(String validPath) {
    SubDocument subDocumentForValidPath = SubDocument.builder().path(validPath).build();
    assertEquals(validPath, subDocumentForValidPath.getPath());
  }

  @ParameterizedTest
  @MethodSource("inValidPathsProvider")
  void testInvalidSubDocumentPaths(String inValidPath) {
    assertThrows(
        IllegalArgumentException.class, () -> SubDocument.builder().path(inValidPath).build());
  }

  @Test
  void testImplicitSubDocumentPaths() {
    String createdTimeImplicitPath = "createdTime";
    String lastUpdatedTimeImplicitPath = "lastUpdatedTime";
    SubDocument implicitSubDocumentForCreatedTime = SubDocument.implicitCreatedTime();
    SubDocument implicitSubDocumentForUpdatedTime = SubDocument.implicitUpdatedTime();
    assertEquals(createdTimeImplicitPath, implicitSubDocumentForCreatedTime.getPath());
    assertEquals(lastUpdatedTimeImplicitPath, implicitSubDocumentForUpdatedTime.getPath());
  }
}
