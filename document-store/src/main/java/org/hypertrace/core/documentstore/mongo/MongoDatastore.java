package org.hypertrace.core.documentstore.mongo;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.FIELD_SEPARATOR;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
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

  private static final String ADMIN_DB_NAME = "admin";
  private static final String DEFAULT_DB_NAME = "default_db";

  private static final String MONGODB_USERNAME_KEY = "MONGODB_USERNAME";
  private static final String MONGODB_PASSWORD_KEY = "MONGODB_PASSWORD";
  private static final String SERVICE_NAME_KEY = "SERVICE_NAME";

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

    final MongoClientSettings.Builder clientSettingsBuilder =
        MongoClientSettings.builder().applyConnectionString(connString).retryWrites(true);

    addCredentialsIfAvailable(clientSettingsBuilder);
    addAppNameIfAvailable(clientSettingsBuilder);

    final MongoClientSettings settings = clientSettingsBuilder.build();
    client = MongoClients.create(settings);

    database = client.getDatabase(DEFAULT_DB_NAME);
    return true;
  }

  @Override
  public Set<String> listCollections() {
    Set<String> collections = new HashSet<>();
    for (String collectionName : database.listCollectionNames()) {
      collections.add(database.getName() + FIELD_SEPARATOR + collectionName);
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
    com.mongodb.client.MongoCollection<Document> collection =
        database.getCollection(collectionName);
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

  private void addCredentialsIfAvailable(final MongoClientSettings.Builder clientSettingsBuilder) {
    final String username = System.getenv(MONGODB_USERNAME_KEY);
    final String password = System.getenv(MONGODB_PASSWORD_KEY);

    if (isNotBlank(username) && isNotBlank(password)) {
      clientSettingsBuilder.credential(
          MongoCredential.createCredential(username, ADMIN_DB_NAME, password.toCharArray()));
    }
  }

  private void addAppNameIfAvailable(final MongoClientSettings.Builder clientSettingsBuilder) {
    final String serviceName = System.getenv(SERVICE_NAME_KEY);

    if (isNotBlank(serviceName)) {
      clientSettingsBuilder.applicationName(serviceName);
    }
  }
}
