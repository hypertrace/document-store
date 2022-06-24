package org.hypertrace.core.documentstore;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Set;

public interface Datastore {

  boolean init(Config datastoreConfig);

  Set<String> listCollections();

  boolean createCollection(String collectionName, Map<String, String> options);

  boolean deleteCollection(String collectionName);

  Collection getCollection(String collectionName);

  boolean healthCheck();

  String getCreatedTimePath();
}
