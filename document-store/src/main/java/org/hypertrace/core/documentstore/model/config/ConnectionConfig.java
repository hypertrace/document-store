package org.hypertrace.core.documentstore.model.config;

import static java.util.Collections.unmodifiableList;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Singular;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.hypertrace.core.documentstore.model.config.mongo.MongoConnectionConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionConfig;
import org.hypertrace.core.documentstore.model.options.DataFreshness;

@Value
@NonFinal
@Accessors(fluent = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
public class ConnectionConfig {
  private static final String DEFAULT_APP_NAME = "document-store";

  @Singular @NonNull List<@NonNull Endpoint> endpoints;
  @NonNull String database;
  @Nullable ConnectionCredentials credentials;
  @NonNull AggregatePipelineMode aggregationPipelineMode;
  @NonNull DataFreshness dataFreshness;
  @NonNull Duration queryTimeout;

  public ConnectionConfig(
      @NonNull List<@NonNull Endpoint> endpoints,
      @NonNull String database,
      @Nullable ConnectionCredentials credentials) {
    this(
        endpoints,
        database,
        credentials,
        AggregatePipelineMode.DEFAULT_ALWAYS,
        DataFreshness.SYSTEM_DEFAULT,
        Duration.ofMinutes(20));
  }

  public static ConnectionConfigBuilder builder() {
    return new ConnectionConfigBuilder();
  }

  @Getter
  @Setter
  @Accessors(fluent = true, chain = true)
  @FieldDefaults(level = AccessLevel.PRIVATE)
  public static class ConnectionConfigBuilder {
    DatabaseType type;
    List<Endpoint> endpoints = new ArrayList<>();
    String database;
    ConnectionCredentials credentials;
    String applicationName = DEFAULT_APP_NAME;
    String replicaSet;
    ConnectionPoolConfig connectionPoolConfig;
    AggregatePipelineMode aggregationPipelineMode = AggregatePipelineMode.DEFAULT_ALWAYS;
    DataFreshness dataFreshness = DataFreshness.SYSTEM_DEFAULT;
    Duration queryTimeout = Duration.ofMinutes(20);

    public ConnectionConfigBuilder type(final DatabaseType type) {
      this.type = type;
      return this;
    }

    public ConnectionConfigBuilder type(final String type) {
      return type(DatabaseType.getType(type));
    }

    public ConnectionConfigBuilder addEndpoint(final Endpoint endpoint) {
      endpoints.add(endpoint);
      return this;
    }

    public ConnectionConfig build() {
      Preconditions.checkArgument(type != null, "The database type is mandatory");

      switch (type) {
        case MONGO:
          return new MongoConnectionConfig(
              unmodifiableList(endpoints),
              database,
              credentials,
              applicationName,
              replicaSet,
              connectionPoolConfig,
              aggregationPipelineMode,
              dataFreshness,
              queryTimeout);

        case POSTGRES:
          return new PostgresConnectionConfig(
              unmodifiableList(endpoints),
              database,
              credentials,
              applicationName,
              connectionPoolConfig);
      }

      throw new IllegalArgumentException("Unsupported database type: " + type);
    }
  }
}
