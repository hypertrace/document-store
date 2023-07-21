package org.hypertrace.core.documentstore.model.config;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresDefaults;

public class ConnectionConfig {

  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_APP_NAME = "document-store";

  public static ConnectionConfigBuilder newBuilder() {
    return new ConnectionConfigBuilder();
  }

  @Getter
  @Setter
  @Accessors(fluent = true, chain = true)
  public static class ConnectionConfigBuilder {
    DatabaseType type;
    String host = DEFAULT_HOST;
    Integer port;
    ConnectionCredentials credentials = ConnectionCredentials.builder().build();
    String database;
    String applicationName = DEFAULT_APP_NAME;
    ConnectionPoolConfig connectionPoolConfig;

    public ConnectionConfigBuilder type(final DatabaseType type) {
      this.type = type;
      return this;
    }

    public ConnectionConfigBuilder type(final String type) {
      return type(DatabaseType.getType(type));
    }

    public ConnectionConfig build() {
      Preconditions.checkArgument(type != null, "The database type is mandatory");

      switch (type) {
        case MONGO:
          return MongoConnectionConfig.builder()
              .host(host)
              .port(port)
              .database(database)
              .credentials(credentials)
              .applicationName(applicationName)
              .build();

        case POSTGRES:
          return PostgresConnectionConfig.builder()
              .host(host)
              .port(port)
              .database(database)
              .credentials(credentials)
              .applicationName(applicationName)
              .connectionPoolConfig(connectionPoolConfig)
              .build();
      }

      throw new IllegalArgumentException("Unsupported database type: " + type);
    }
  }
}
