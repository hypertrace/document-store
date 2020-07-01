package org.hypertrace.core.documentstore.mongo;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;

public class MongoDatastore implements Datastore {

  private static final String DEFAULT_DB_NAME = "default_db";
  private MongoClient client;

  @Override
  public boolean init(Config config) {
    try {
      if (config.hasPath("url")) {
        client = new MongoClient(new MongoClientURI(config.getString("url")));
      } else {
        String hostName = config.getString("host");
        int port = config.getInt("port");
        client = new MongoClient(hostName, port);
      }
    } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Unable to instantiate MongoClient with config:%s",config), e);
    }
    return true;
  }

  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    List<String> databaseNames = client.getDatabaseNames();
    for (String dbName : databaseNames) {
      Set<String> collectionNames = client.getDB(dbName).getCollectionNames();
      for (String collectionName : collectionNames) {
        collections.add(dbName + "." + collectionName);
      }
    }
    return collections;
  }

  @Override
  public boolean createCollection(String collectionName, Map<String, String> options) {
    DBObject dbObject = null;
    DBCollection collection =
        client.getDB(DEFAULT_DB_NAME).createCollection(collectionName, dbObject);

    return collection != null;
  }

  @Override
  public boolean deleteCollection(String collectionName) {
    DBCollection collection = client.getDB(DEFAULT_DB_NAME).getCollection(collectionName);
    if (collection != null) {
      collection.drop();
    }
    return true;
  }

  @Override
  public Collection getCollection(String collectionName) {
    return new MongoCollection(client.getDB(DEFAULT_DB_NAME).getCollection(collectionName));
  }

  @Override
  public boolean healthCheck() {
    return this.client.getDB(DEFAULT_DB_NAME).command("ping").ok();
  }

  @VisibleForTesting
  MongoClient getMongoClient() {
    return client;
  }
}
