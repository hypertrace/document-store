package org.hypertrace.core.documentstore;

import com.fasterxml.jackson.databind.JsonNode;

public interface Document {

  String toJson();

  default JsonNode getJsonNode() {
    return null;
  }

  default DocumentType getDocumentType() {
    return DocumentType.NESTED;
  }
}
