package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JSONDocumentTest {

  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testJSONDocument() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");
    JSONDocument document1 = new JSONDocument(data);
    JSONDocument document2 = new JSONDocument(document1.toJson());
    Assertions.assertEquals(document1, document2);
    Assertions.assertEquals(document1.toJson(), document2.toJson());
  }

  @Test
  public void testJSONDocumentWithDefaultDocumentType() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");
    JSONDocument document = new JSONDocument(data);
    Assertions.assertEquals(DocumentType.NESTED, document.getDocumentType());
  }

  @Test
  public void testJSONDocumentWithExplicitDocumentType() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");

    JSONDocument nestedDocument = new JSONDocument(data, DocumentType.NESTED);
    Assertions.assertEquals(DocumentType.NESTED, nestedDocument.getDocumentType());

    JSONDocument flatDocument = new JSONDocument(data, DocumentType.FLAT);
    Assertions.assertEquals(DocumentType.FLAT, flatDocument.getDocumentType());
  }

  @Test
  public void testJSONDocumentConstructorsWithDocumentType() throws Exception {
    String json = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
    JsonNode node = mapper.readTree(json);
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");

    // Test string constructor with DocumentType
    JSONDocument stringDoc = new JSONDocument(json, DocumentType.FLAT);
    Assertions.assertEquals(DocumentType.FLAT, stringDoc.getDocumentType());

    // Test object constructor with DocumentType
    JSONDocument objectDoc = new JSONDocument(data, DocumentType.FLAT);
    Assertions.assertEquals(DocumentType.FLAT, objectDoc.getDocumentType());

    // Test JsonNode constructor with DocumentType
    JSONDocument nodeDoc = new JSONDocument(node, DocumentType.FLAT);
    Assertions.assertEquals(DocumentType.FLAT, nodeDoc.getDocumentType());
  }

  @Test
  public void testEqualsWithSameContentDifferentDocumentType() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");

    JSONDocument nestedDoc = new JSONDocument(data, DocumentType.NESTED);
    JSONDocument flatDoc = new JSONDocument(data, DocumentType.FLAT);

    // Current implementation only compares JsonNode, not DocumentType
    // This test documents the current behavior - documents with same content but different types
    // are equal
    Assertions.assertNotEquals(nestedDoc, flatDoc);
    Assertions.assertEquals(nestedDoc.toJson(), flatDoc.toJson());
  }

  @Test
  public void testEqualsWithSameContentSameDocumentType() throws Exception {
    Map<String, String> data = Map.of("key1", "value1", "key2", "value2");

    JSONDocument doc1 = new JSONDocument(data, DocumentType.FLAT);
    JSONDocument doc2 = new JSONDocument(data, DocumentType.FLAT);

    Assertions.assertEquals(doc1, doc2);
    Assertions.assertEquals(doc1.getDocumentType(), doc2.getDocumentType());
  }

  @Test
  public void testErrorDocument() throws Exception {
    String errorMessage = "Test error message";
    JSONDocument errorDoc = JSONDocument.errorDocument(errorMessage);

    // Verify default document type
    Assertions.assertEquals(DocumentType.NESTED, errorDoc.getDocumentType());

    // Verify error message is in the JSON
    String expectedJson = "{\"errorMessage\":\"" + errorMessage + "\"}";
    Assertions.assertEquals(expectedJson, errorDoc.toJson());

    // Test error document equality
    JSONDocument anotherErrorDoc = JSONDocument.errorDocument(errorMessage);
    Assertions.assertEquals(errorDoc, anotherErrorDoc);
  }

  @Test
  public void testToStringMethod() throws Exception {
    Map<String, String> data = Map.of("key1", "value1");
    JSONDocument document = new JSONDocument(data, DocumentType.FLAT);

    // toString should return the same as toJson
    Assertions.assertEquals(document.toJson(), document.toString());
  }

  @Test
  public void testJsonNodeConstructors() throws Exception {
    JsonNode node = mapper.createObjectNode().put("test", "value");

    // Test JsonNode constructor without DocumentType
    JSONDocument doc1 = new JSONDocument(node);
    Assertions.assertEquals(DocumentType.NESTED, doc1.getDocumentType());

    // Test JsonNode constructor with DocumentType
    JSONDocument doc2 = new JSONDocument(node, DocumentType.FLAT);
    Assertions.assertEquals(DocumentType.FLAT, doc2.getDocumentType());
  }

  @Test
  public void testStringConstructorWithInvalidJson() {
    // Test invalid JSON handling
    Assertions.assertThrows(IOException.class, () -> new JSONDocument("invalid json {"));

    Assertions.assertThrows(
        IOException.class, () -> new JSONDocument("invalid json {", DocumentType.FLAT));
  }

  @Test
  public void testNullHandling() throws Exception {
    // Test null JsonNode
    JSONDocument nullNodeDoc = new JSONDocument((JsonNode) null);
    Assertions.assertNotNull(nullNodeDoc.toJson()); // Should handle gracefully
  }
}
