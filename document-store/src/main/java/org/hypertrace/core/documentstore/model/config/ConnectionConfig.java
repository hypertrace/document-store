package org.hypertrace.core.documentstore.model.config;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnegative;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionPoolConfig;

@Value
@Builder
@NonFinal
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ConnectionConfig {
  @NonNull DatabaseType type;

  @NonNull String host;

  @NonNull @Nonnegative Integer port;

  @NonNull ConnectionCredentials credentials;

  @NonNull String database;

  @NonNull String applicationName;

  private static ConnectionConfigBuilder builder() {
    throw new IllegalArgumentException("type is required");
  }

  public static ConnectionConfigBuilder builderFor(final String type) {
    return builder().type(type);
  }

  public static ConnectionConfigBuilder builderFor(final DatabaseType type) {
    return builder().type(type);
  }

  public static class ConnectionConfigBuilder {
    private final PostgresConnectionPoolConfig connectionPoolConfig =
        PostgresConnectionPoolConfig.builder().build();

    public ConnectionConfigBuilder type(final DatabaseType type) {
      this.type = type;
      return this;
    }

    public ConnectionConfigBuilder type(final String type) {
      return type(DatabaseType.getType(type));
    }

    public ConnectionConfig build() {
      if (DatabaseType.MONGO == type) {
        return new MongoConnectionConfig(
            type, host, port, credentials, database, applicationName);
      }

      return new ConnectionConfig(
          type, host, port, credentials, database, applicationName, connectionPoolConfig);
    }
  }
}
