package org.hypertrace.core.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class DocumentChangeTest {

  @Test
  void createdHasEmptyBefore() throws IOException {
    Document after = new JSONDocument("{\"item\":\"after\"}");
    DocumentChange change = DocumentChange.created(after);
    assertFalse(change.getBefore().isPresent());
    assertSame(after, change.getAfter());
  }

  @Test
  void updatedHasBothImages() throws IOException {
    Document before = new JSONDocument("{\"item\":\"before\"}");
    Document after = new JSONDocument("{\"item\":\"after\"}");
    DocumentChange change = DocumentChange.updated(before, after);
    assertTrue(change.getBefore().isPresent());
    assertEquals(before, change.getBefore().get());
    assertSame(after, change.getAfter());
  }
}
