package org.hypertrace.core.documentstore;

public class BulkUpdateRequest {

  private final Key key;
  private final Document document;
  private final Filter filter;

  public BulkUpdateRequest(Key key, Document document, Filter filter) {
    this.key = key;
    this.document = document;
    this.filter = filter;
  }

  public Key getKey() {
    return key;
  }

  public Document getDocument() {
    return document;
  }

  public Filter getFilter() {
    return filter;
  }

  @Override
  public String toString() {
    return "BulkUpdateRequest{"
        + "key="
        + key
        + ", document="
        + document
        + ", filter="
        + filter
        + '}';
  }
}
