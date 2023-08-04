package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.mongo.MongoUtils.FIELD_SEPARATOR;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.metric.exporter.CommonMetricExporter;
import org.hypertrace.core.documentstore.metric.exporter.DBMetricExporter;
import org.hypertrace.core.documentstore.metric.exporter.MetricExporter;
import org.hypertrace.core.documentstore.metric.exporter.mongo.MongoMetricValuesProvider;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDatastore implements Datastore {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDatastore.class);

  private final MongoClient client;
  private final MongoDatabase database;

  public MongoDatastore(final DatastoreConfig datastoreConfig) {
    final ConnectionConfig connectionConfig = datastoreConfig.connectionConfig();

    if (!(connectionConfig instanceof MongoConnectionConfig)) {
      throw new IllegalArgumentException(
          String.format(
              "Can't pass %s to %s",
              connectionConfig.getClass().getSimpleName(), this.getClass().getSimpleName()));
    }

    final MongoConnectionConfig mongoConfig = (MongoConnectionConfig) connectionConfig;
    client = MongoClients.create(mongoConfig.toSettings());
    database = client.getDatabase(mongoConfig.database());

    startMetricExporters(datastoreConfig, mongoConfig);
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

  private void startMetricExporters(
      final DatastoreConfig datastoreConfig, final MongoConnectionConfig mongoConfig) {
    if (datastoreConfig.metricExporterConfig().exportingEnabled()) {
      getMetricExporters(datastoreConfig, mongoConfig).forEach(MetricExporter::reportMetrics);
    }
  }

  private Set<MetricExporter> getMetricExporters(
      final DatastoreConfig datastoreConfig, final MongoConnectionConfig mongoConfig) {
    return Set.of(
        getDBSpecificExporter(datastoreConfig, mongoConfig), getCommonExporter(datastoreConfig));
  }

  private MetricExporter getDBSpecificExporter(
      final DatastoreConfig datastoreConfig, final MongoConnectionConfig mongoConfig) {
    return new DBMetricExporter(
        Set.of(new MongoMetricValuesProvider(mongoConfig, client)), datastoreConfig);
  }

  private MetricExporter getCommonExporter(final DatastoreConfig datastoreConfig) {
    return new CommonMetricExporter(this, datastoreConfig);
  }
}
