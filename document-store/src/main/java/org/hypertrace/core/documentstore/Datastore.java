package org.hypertrace.core.documentstore;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;

public interface Datastore {

  /** Deprecated in favour of {@link Datastore#init(ConnectionConfig)} */
  @Deprecated(forRemoval = true)
  boolean init(Config datastoreConfig);

  void init(final ConnectionConfig connectionConfig);

  Set<String> listCollections();

  boolean createCollection(String collectionName, Map<String, String> options);

  boolean deleteCollection(String collectionName);

  Collection getCollection(String collectionName);

  boolean healthCheck();
}
