package org.hypertrace.core.documentstore.model.config;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;

@Value
@NonFinal
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ConnectionConfig {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_APP_NAME = "document-store";

  @NonNull DatabaseType type;
  @NonNull String host;
  @NonNull @Nonnegative Integer port;
  @NonNull String database;
  @Nullable ConnectionCredentials credentials;

  public static ConnectionConfigBuilder builder() {
    return new ConnectionConfigBuilder();
  }

  @Getter
  @Setter
  @Accessors(fluent = true, chain = true)
  @FieldDefaults(level = AccessLevel.PRIVATE)
  public static class ConnectionConfigBuilder {
    DatabaseType type;
    String host = DEFAULT_HOST;
    Integer port;
    String database;
    ConnectionCredentials credentials;
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
          return new MongoConnectionConfig(host, port, database, credentials, applicationName);

        case POSTGRES:
          return new PostgresConnectionConfig(
              host, port, database, credentials, applicationName, connectionPoolConfig);
      }

      throw new IllegalArgumentException("Unsupported database type: " + type);
    }
  }
}
