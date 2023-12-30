package org.hypertrace.core.documentstore;

import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.metric.DocStoreMetricProvider;

public interface Datastore {
  Set<String> listCollections();

  boolean createCollection(String collectionName, Map<String, String> options);

  boolean deleteCollection(String collectionName);

  Collection getCollection(String collectionName);

  boolean healthCheck();

  @SuppressWarnings("unused")
  DocStoreMetricProvider getDocStoreMetricProvider();

  void close();
}
