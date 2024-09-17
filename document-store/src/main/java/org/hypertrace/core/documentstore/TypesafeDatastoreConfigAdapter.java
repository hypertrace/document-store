package org.hypertrace.core.documentstore;

import static java.util.Collections.emptyList;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.typesafe.config.Config;
import java.time.Duration;
import org.hypertrace.core.documentstore.model.config.AggregatePipelineMode;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.TypesafeConfigDatastoreConfigExtractor;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.model.options.DataFreshness;

@Deprecated(forRemoval = true)
interface TypesafeDatastoreConfigAdapter {

  DatastoreConfig convert(final Config config);

  @Deprecated(forRemoval = true)
  class MongoTypesafeDatastoreConfigAdapter implements TypesafeDatastoreConfigAdapter {

    @Override
    public DatastoreConfig convert(final Config config) {
      final MongoConnectionConfig overridingConnectionConfig =
          new MongoConnectionConfig(
              emptyList(),
              null,
              null,
              "",
              null,
              null,
              AggregatePipelineMode.DEFAULT_ALWAYS,
              DataFreshness.SYSTEM_DEFAULT,
              Duration.ofMinutes(10)) {
            public MongoClientSettings toSettings() {
              final MongoClientSettings.Builder settingsBuilder =
                  MongoClientSettings.builder()
                      .applyConnectionString(toConnectionString())
                      .retryWrites(true);

              return settingsBuilder.build();
            }

            private ConnectionString toConnectionString() {
              if (config.hasPath("url")) {
                return new ConnectionString(config.getString("url"));
              } else {
                String hostName = config.getString("host");
                int port = config.getInt("port");
                return new ConnectionString("mongodb://" + hostName + ":" + port);
              }
            }
          };

      return DatastoreConfig.builder()
          .type(DatabaseType.MONGO)
          .connectionConfig(overridingConnectionConfig)
          .build();
    }
  }

  @Deprecated(forRemoval = true)
  class PostgresTypesafeDatastoreConfigAdapter implements TypesafeDatastoreConfigAdapter {

    @Override
    public DatastoreConfig convert(final Config config) {
      final PostgresConnectionConfig connectionConfig =
          (PostgresConnectionConfig)
              TypesafeConfigDatastoreConfigExtractor.from(config, DatabaseType.POSTGRES)
                  .hostKey("host")
                  .portKey("port")
                  .usernameKey("user")
                  .passwordKey("password")
                  .databaseKey("database")
                  .applicationNameKey("applicationName")
                  .poolMaxConnectionsKey("connectionPool.maxConnections")
                  .poolConnectionAccessTimeoutKey("connectionPool.maxWaitTime")
                  .poolConnectionSurrenderTimeoutKey("connectionPool.removeAbandonedTimeout")
                  .extract()
                  .connectionConfig();

      final PostgresConnectionConfig overridingConnectionConfig =
          new PostgresConnectionConfig(
              connectionConfig.endpoints(),
              connectionConfig.database(),
              connectionConfig.credentials(),
              connectionConfig.applicationName(),
              connectionConfig.connectionPoolConfig()) {
            @Override
            public String toConnectionString() {
              return config.hasPath("url")
                  ? config.getString("url") + connectionConfig.database()
                  : super.toConnectionString();
            }
          };

      return DatastoreConfig.builder()
          .type(DatabaseType.POSTGRES)
          .connectionConfig(overridingConnectionConfig)
          .build();
    }
  }
}
