package org.hypertrace.document.store.metric.exporter.mongo;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.typesafe.config.Config;
import javax.inject.Inject;
import javax.inject.Provider;

public class MongoConnectionProvider implements Provider<MongoClient> {
  private final Config config;

  @Inject
  public MongoConnectionProvider(final Config config) {
    this.config = config.getConfig("document.store.mongo");
  }

  @Override
  public MongoClient get() {
    final ConnectionString connString;

    if (config.hasPath("url")) {
      connString = new ConnectionString(config.getString("url"));
    } else {
      final String hostName = config.getString("host");
      final int port = config.getInt("port");
      connString = new ConnectionString("mongodb://" + hostName + ":" + port);
    }

    final MongoClientSettings settings =
        MongoClientSettings.builder().applyConnectionString(connString).build();
    return MongoClients.create(settings);
  }
}
