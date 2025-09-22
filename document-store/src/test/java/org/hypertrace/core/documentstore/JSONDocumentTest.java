package org.hypertrace.core.documentstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JSONDocumentTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testJSONDocument() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");
    JSONDocument document1 = JSONDocument.fromObject(data);
    JSONDocument document2 = JSONDocument.fromJson(document1.toJson());
    assertEquals(document1, document2);
    assertEquals(document1.toJson(), document2.toJson());
  }

  @Test
  void testFromJson() throws IOException {
    String json = "{\"name\":\"John\",\"age\":30}";
    JSONDocument document = JSONDocument.fromJson(json);

    assertNotNull(document);
    // default document type is DOCUMENT_STORE
    assertEquals(DocumentType.DOCUMENT_STORE, document.getDocumentType());
    assertTrue(document.toJson().contains("John"));
  }

  @Test
  void testFromJsonWithDocumentType() throws IOException {
    String json = "{\"name\":\"John\",\"age\":30}";
    JSONDocument document = JSONDocument.fromJson(json, DocumentType.SQL_STORE);

    assertNotNull(document);
    assertEquals(DocumentType.SQL_STORE, document.getDocumentType());
  }

  @Test
  void testFromObject() throws IOException {
    Map<String, Object> data = Map.of("name", "Alice", "age", 25);
    JSONDocument document = JSONDocument.fromObject(data);

    assertNotNull(document);
    assertEquals(DocumentType.DOCUMENT_STORE, document.getDocumentType());
    assertTrue(document.toJson().contains("Alice"));
  }

  @Test
  void testFromObjectWithDocumentType() throws IOException {
    Map<String, String> data = Map.of("key", "value");
    JSONDocument document = JSONDocument.fromObject(data, DocumentType.SQL_STORE);

    assertNotNull(document);
    assertEquals(DocumentType.SQL_STORE, document.getDocumentType());
  }

  @Test
  void testFromJsonNode() throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    node.put("city", "New York");

    JSONDocument document = JSONDocument.fromJsonNode(node);

    assertNotNull(document);
    assertEquals(DocumentType.DOCUMENT_STORE, document.getDocumentType());
    assertTrue(document.toJson().contains("New York"));
  }

  @Test
  void testErrorDocument() {
    String errorMessage = "Something went wrong";
    JSONDocument errorDoc = JSONDocument.errorDocument(errorMessage);

    assertNotNull(errorDoc);
    assertEquals(DocumentType.DOCUMENT_STORE, errorDoc.getDocumentType());
    assertTrue(errorDoc.toJson().contains(errorMessage));
  }

  @Test
  void testErrorDocumentWithDocumentType() {
    String errorMessage = "Database connection failed";
    JSONDocument errorDoc = JSONDocument.errorDocument(errorMessage, DocumentType.SQL_STORE);

    assertNotNull(errorDoc);
    assertEquals(DocumentType.SQL_STORE, errorDoc.getDocumentType());
    assertTrue(errorDoc.toJson().contains(errorMessage));
  }

  @Test
  void testToString() throws IOException {
    Map<String, String> data = Map.of("key", "value");
    JSONDocument document = JSONDocument.fromObject(data);

    assertEquals(document.toJson(), document.toString());
  }
}
