package org.hypertrace.core.documentstore.mongo;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDatastore implements Datastore {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDatastore.class);

  private static final String DEFAULT_DB_NAME = "default_db";
  private MongoClient client;
  private MongoDatabase database;

  @Override
  public boolean init(Config config) {
    ConnectionString connString;
    if (config.hasPath("url")) {
      connString = new ConnectionString(config.getString("url"));
    } else {
      String hostName = config.getString("host");
      int port = config.getInt("port");
      connString = new ConnectionString("mongodb://" + hostName + ":" + port);
    }

    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(connString)
        .retryWrites(true)
        .build();
    client = MongoClients.create(settings);

    database = client.getDatabase(DEFAULT_DB_NAME);
    return true;
  }

  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    for (String collectionName : database.listCollectionNames()) {
      collections.add(database.getName() + "." + collectionName);
    }
    return collections;
  }

  @Override
  public boolean createCollection(String collectionName, Map<String, String> options) {
    try {
      database.createCollection(collectionName);
    } catch (MongoCommandException e) {
      if (!"NamespaceExists".equals(e.getErrorCodeName())) {
        LOGGER.error("Could not create collection: {}", collectionName, e);
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean deleteCollection(String collectionName) {
    com.mongodb.client.MongoCollection<Document> collection = database.getCollection(collectionName);
    collection.drop();
    return true;
  }

  @Override
  public Collection getCollection(String collectionName) {
    return new MongoCollection(database.getCollection(collectionName, BasicDBObject.class));
  }

  @Override
  public boolean healthCheck() {
    Document document = this.database.runCommand(new Document("ping", "1"));
    return !document.isEmpty();
  }

  @VisibleForTesting
  MongoClient getMongoClient() {
    return client;
  }
}
