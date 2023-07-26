package org.hypertrace.core.documentstore;

import java.util.Map;
import java.util.Set;

public interface Datastore {
  Set<String> listCollections();

  boolean createCollection(String collectionName, Map<String, String> options);

  boolean deleteCollection(String collectionName);

  Collection getCollection(String collectionName);

  boolean healthCheck();
}
