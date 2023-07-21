package org.hypertrace.core.documentstore.model.config;

import com.google.common.base.Preconditions;
import javax.annotation.Nonnegative;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Builder
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConnectionConfig {
  @NonNull DatabaseType type;

  @NonNull String host;

  @NonNull @Nonnegative Integer port;

  @NonNull ConnectionCredentials credentials;

  @NonNull String database;

  @NonNull String applicationName;

  @NonNull @Builder.Default
  ConnectionPoolConfig connectionPoolConfig = ConnectionPoolConfig.builder().build();

  @SuppressWarnings("unused")
  public static class ConnectionConfigBuilder {
    private final ConnectionPoolConfig connectionPoolConfig =
        ConnectionPoolConfig.builder().build();

    public ConnectionConfigBuilder type(final DatabaseType type) {
      this.type = type;
      return this;
    }

    public ConnectionConfigBuilder type(final String type) {
      return type(DatabaseType.getType(type));
    }

    public ConnectionConfig build() {
      validateMongoProperties();
      return new ConnectionConfig(
          type, host, port, credentials, database, applicationName, connectionPoolConfig);
    }

    private void validateMongoProperties() {
      if (DatabaseType.MONGO != type) {
        return;
      }

      Preconditions.checkArgument(
          credentials.authDatabase().isPresent(),
          "Authentication database is mandatory for MongoDB");
    }
  }
}
