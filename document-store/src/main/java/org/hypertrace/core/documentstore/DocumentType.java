package org.hypertrace.core.documentstore;

public enum DocumentType {
  // FLAT documents don't contain columns/fields with type annotations like value, valueList,
  // valueMap. They're plain JSON documents.
  FLAT,
  // NESTED documents contain columns/fields with the above type information.
  NESTED
}
