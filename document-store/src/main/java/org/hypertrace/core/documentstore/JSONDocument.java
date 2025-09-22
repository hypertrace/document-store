package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.Objects;

public class JSONDocument implements Document {

  private static ObjectMapper mapper = new ObjectMapper();
  private JsonNode node;
  private DocumentType documentType;

  public JSONDocument(String json) throws IOException {
    node = mapper.readTree(json);
  }

  public JSONDocument(String json, DocumentType documentType) throws IOException {
    node = mapper.readTree(json);
    this.documentType = documentType;
  }

  public JSONDocument(Object object) throws IOException {
    node = mapper.readTree(mapper.writeValueAsString(object));
  }

  public JSONDocument(Object object, DocumentType documentType) throws IOException {
    node = mapper.readTree(mapper.writeValueAsString(object));
    this.documentType = documentType;
  }

  public JSONDocument(JsonNode node) {
    this.node = node;
  }

  public JSONDocument(JsonNode node, DocumentType documentType) {
    this.node = node;
    this.documentType = documentType;
  }

  @Override
  public DocumentType getDocumentType() {
    return this.documentType;
  }

  @Override
  public String toJson() {
    try {
      return mapper.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      return "{}";
    }
  }

  public static JSONDocument errorDocument(String message) {
    ObjectNode objectNode = mapper.createObjectNode();
    objectNode.put("errorMessage", message);
    return new JSONDocument(objectNode);
  }

  public static JSONDocument errorDocument(String message, DocumentType documentType) {
    ObjectNode objectNode = mapper.createObjectNode();
    objectNode.put("errorMessage", message);
    return new JSONDocument(objectNode, documentType);
  }

  @Override
  public String toString() {
    return toJson();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    JSONDocument other = (JSONDocument) obj;
    return Objects.equals(node, other.node);
  }
}
