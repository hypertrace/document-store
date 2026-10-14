package org.hypertrace.core.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class DocumentChangeTest {

  @Test
  void bothImagesPresent() throws IOException {
    Document before = new JSONDocument("{\"item\":\"before\"}");
    Document after = new JSONDocument("{\"item\":\"after\"}");
    DocumentChange change = DocumentChange.of(before, after);
    assertSame(before, change.getBefore().orElseThrow());
    assertSame(after, change.getAfter().orElseThrow());
  }

  @Test
  void nullImagesAreEmpty() throws IOException {
    Document document = new JSONDocument("{\"item\":\"doc\"}");

    DocumentChange afterOnly = DocumentChange.of(null, document);
    assertFalse(afterOnly.getBefore().isPresent());
    assertSame(document, afterOnly.getAfter().orElseThrow());

    DocumentChange beforeOnly = DocumentChange.of(document, null);
    assertSame(document, beforeOnly.getBefore().orElseThrow());
    assertFalse(beforeOnly.getAfter().isPresent());

    DocumentChange none = DocumentChange.of(null, null);
    assertFalse(none.getBefore().isPresent());
    assertFalse(none.getAfter().isPresent());
  }

  @Test
  void equalityUsesBothImages() throws IOException {
    Document document = new JSONDocument("{\"item\":\"doc\"}");
    assertEquals(DocumentChange.of(null, document), DocumentChange.of(null, document));
    assertEquals(
        DocumentChange.of(null, document).hashCode(), DocumentChange.of(null, document).hashCode());
    assertNotEquals(DocumentChange.of(null, document), DocumentChange.of(document, null));
  }
}
