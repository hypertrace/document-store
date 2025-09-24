package org.hypertrace.core.documentstore;

public enum DocumentType {
  // FLAT documents contain individual columns for each attribute
  FLAT,
  // NESTED documents contains attributes as a nested JSON document
  NESTED
}
