package org.hypertrace.core.documentstore;

public interface Document {

  String toJson();

  default DocumentType getDocumentType() {
    return null;
  }
}
