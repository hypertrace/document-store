package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Objects;
import lombok.Builder;

@Builder
public class JSONDocument implements Document {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final JsonNode node;
  private final DocumentType documentType;

  private JSONDocument(JsonNode node, DocumentType documentType) {
    this.node = Preconditions.checkNotNull(node, "JsonNode cannot be null");
    this.documentType = Preconditions.checkNotNull(documentType, "DocumentType cannot be null");
  }

  @Override
  public String toJson() {
    try {
      return MAPPER.writeValueAsString(node);
    } catch (JsonProcessingException e) {
      return "{}";
    }
  }

  @Override
  public DocumentType getDocumentType() {
    return documentType;
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

  @Override
  public int hashCode() {
    return Objects.hash(node, documentType);
  }

  // Builder class
  public static class Builder {

    private JsonNode node;
    private DocumentType documentType = DocumentType.DOCUMENT_STORE; // default is DOCUMENT_STORE

    public JSONDocument.Builder fromJson(String json) throws IOException {
      this.node = MAPPER.readTree(json);
      return this;
    }

    public JSONDocument.Builder fromObject(Object object) throws IOException {
      this.node = MAPPER.readTree(MAPPER.writeValueAsString(object));
      return this;
    }

    public JSONDocument.Builder fromJsonNode(JsonNode node) {
      this.node = node;
      return this;
    }

    public JSONDocument.Builder withDocumentType(DocumentType documentType) {
      this.documentType = documentType;
      return this;
    }

    public JSONDocument build() {
      if (node == null) {
        throw new IllegalStateException("JsonNode must be set before building");
      }
      return new JSONDocument(node, documentType);
    }
  }

  public static JSONDocument fromJson(String json) throws IOException {
    return new JSONDocument.Builder().fromJson(json).build();
  }

  public static JSONDocument fromJson(String json, DocumentType type) throws IOException {
    return new JSONDocument.Builder().fromJson(json).withDocumentType(type).build();
  }

  public static JSONDocument fromObject(Object object) throws IOException {
    return new JSONDocument.Builder().fromObject(object).build();
  }

  public static JSONDocument fromObject(Object object, DocumentType type) throws IOException {
    return new JSONDocument.Builder().fromObject(object).withDocumentType(type).build();
  }

  public static JSONDocument fromJsonNode(JsonNode jsonNode) {
    return new JSONDocument.Builder().fromJsonNode(jsonNode).build();
  }

  public static JSONDocument fromJsonNode(JsonNode jsonNode, DocumentType type) throws IOException {
    return new JSONDocument.Builder().fromJsonNode(jsonNode).withDocumentType(type).build();
  }

  public static JSONDocument errorDocument(String message) {
    return errorDocument(message, DocumentType.DOCUMENT_STORE);
  }

  public static JSONDocument errorDocument(String message, DocumentType documentType) {
    ObjectNode objectNode = MAPPER.createObjectNode();
    objectNode.put("errorMessage", message);
    return new JSONDocument.Builder()
        .fromJsonNode(objectNode)
        .withDocumentType(documentType)
        .build();
  }
}
