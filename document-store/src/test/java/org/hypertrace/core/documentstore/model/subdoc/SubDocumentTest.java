package org.hypertrace.core.documentstore.model.subdoc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class SubDocumentTest {
  @Test
  void testValidSubDocumentPaths() {
    String alphabetsValidPath = "price";
    String numbersValidPath = "1234567890";
    String stringSeparatedByDotValidPath = "price.quantity";
    String uuidValidPath = "67aa7623-fc40-4112-b958-cbd6d4c077b1";
    String createdTimeImplicitValidPath = "createdTime";
    String lastUpdatedTimeImplicitValidPath = "lastUpdatedTime";
    SubDocument subDocumentForAlphabetsValidPath =
        SubDocument.builder().path(alphabetsValidPath).build();
    assertEquals(alphabetsValidPath, subDocumentForAlphabetsValidPath.getPath());
    SubDocument subDocumentForNumbersValidPath =
        SubDocument.builder().path(numbersValidPath).build();
    assertEquals(numbersValidPath, subDocumentForNumbersValidPath.getPath());
    SubDocument subDocumentForDotValidPath =
        SubDocument.builder().path(stringSeparatedByDotValidPath).build();
    assertEquals(stringSeparatedByDotValidPath, subDocumentForDotValidPath.getPath());
    SubDocument subDocumentForUuidValidPath = SubDocument.builder().path(uuidValidPath).build();
    assertEquals(uuidValidPath, subDocumentForUuidValidPath.getPath());
    assertEquals(createdTimeImplicitValidPath, SubDocument.implicitCreatedTime().getPath());
    assertEquals(lastUpdatedTimeImplicitValidPath, SubDocument.implicitUpdatedTime().getPath());
  }

  @Test
  void testInValidSubDocumentPaths() {
    String doubleColonsInBetweenInvalidPath = "price::quantity";
    String startWithSpecialCharInvalidPath = "{hello_world";
    String endWithSpecialCharInvalidPath = "hello_world}";
    String createdTimeImplicitInvalidPath = "createdTime";
    String lastUpdatedTimeImplicitInvalidPath = "lastUpdatedTime";
    assertThrows(
        IllegalArgumentException.class,
        () -> SubDocument.builder().path(doubleColonsInBetweenInvalidPath).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SubDocument.builder().path(startWithSpecialCharInvalidPath).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SubDocument.builder().path(endWithSpecialCharInvalidPath).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SubDocument.builder().path(createdTimeImplicitInvalidPath).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> SubDocument.builder().path(lastUpdatedTimeImplicitInvalidPath).build());
  }
}
