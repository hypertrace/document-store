package org.hypertrace.core.documentstore;

import java.util.Collection;
import java.util.Set;

public class BulkArrayValueUpdateRequest {

  private final Set<Key> keys;
  private final String subDocPath;
  private final Operation operation;
  private final Collection<Document> subDocuments;

  public BulkArrayValueUpdateRequest(
      Set<Key> keys, String subDocPath, Operation operation, Collection<Document> subDocuments) {
    this.keys = keys;
    this.subDocPath = subDocPath;
    this.operation = operation;
    this.subDocuments = subDocuments;
  }

  public Set<Key> getKeys() {
    return keys;
  }

  public String getSubDocPath() {
    return subDocPath;
  }

  public Operation getOperation() {
    return operation;
  }

  public Collection<Document> getSubDocuments() {
    return subDocuments;
  }

  public enum Operation {
    ADD,
    REMOVE,
    SET
  }
}
