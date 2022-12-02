package org.hypertrace.core.documentstore.commons;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UpdateValidatorTest {

  private CommonUpdateValidator updateValidator;

  @BeforeEach
  void setUp() {
    updateValidator = new CommonUpdateValidator();
  }

  @Test
  void testValidate() {
    final Collection<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("path.one", "Mars"));
    assertDoesNotThrow(() -> updateValidator.validate(updates));
  }

  @Test
  void testValidateWithNoUpdates() {
    final Collection<SubDocumentUpdate> updates = List.of();
    assertThrows(IOException.class, () -> updateValidator.validate(updates));
  }

  @Test
  void testValidateWithDuplicatePaths() {
    final Collection<SubDocumentUpdate> updates =
        List.of(
            SubDocumentUpdate.of("path.one", 1),
            SubDocumentUpdate.of("path.two", 2),
            SubDocumentUpdate.of("path.one", "Mars"));
    assertThrows(IOException.class, () -> updateValidator.validate(updates));
  }

  @Test
  void testValidateWithPathsInTheSameHierarchy() {
    final Collection<SubDocumentUpdate> updates =
        List.of(
            SubDocumentUpdate.of("path.one", 1),
            SubDocumentUpdate.of("path.two", 2),
            SubDocumentUpdate.of("path.one.after", "Mars"));
    assertThrows(IOException.class, () -> updateValidator.validate(updates));
  }

  @Test
  void testValidateWithPseudoPathsInTheSameHierarchy() {
    final Collection<SubDocumentUpdate> updates =
        List.of(
            SubDocumentUpdate.of("path.one", 1),
            SubDocumentUpdate.of("path.two", 2),
            SubDocumentUpdate.of("path.one_after", "Mars"));
    assertDoesNotThrow(() -> updateValidator.validate(updates));
  }
}
