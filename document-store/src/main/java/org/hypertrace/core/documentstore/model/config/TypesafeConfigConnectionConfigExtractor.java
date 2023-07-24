package org.hypertrace.core.documentstore.model.config;

import com.typesafe.config.Config;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig.ConnectionConfigBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials.ConnectionCredentialsBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig.ConnectionPoolConfigBuilder;

@Value
public class TypesafeConfigConnectionConfigExtractor {
  @NonNull Config config;
  ConnectionConfigBuilder connectionConfigBuilder;
  ConnectionCredentialsBuilder connectionCredentialsBuilder;
  ConnectionPoolConfigBuilder connectionPoolConfigBuilder;

  private TypesafeConfigConnectionConfigExtractor(
      @NonNull final Config config, @NonNull final String typeKey) {
    this(
        config,
        Optional.of(typeKey)
            .filter(config::hasPath)
            .map(config::getString)
            .map(DatabaseType::getType)
            .orElseThrow(
                () -> new IllegalArgumentException("Missing or invalid type key: " + typeKey)));
  }

  private TypesafeConfigConnectionConfigExtractor(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    this.config = config;
    this.connectionConfigBuilder = ConnectionConfig.builder().type(type);
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = ConnectionPoolConfig.builder();
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      @NonNull final Config config, @NonNull final String typeKey) {
    return new TypesafeConfigConnectionConfigExtractor(config, typeKey);
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    return new TypesafeConfigConnectionConfigExtractor(config, type);
  }

  public TypesafeConfigConnectionConfigExtractor hostKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.host(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor portKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.port(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor usernameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.username(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor passwordKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.password(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor authDatabaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.authDatabase(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor databaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.database(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor applicationNameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.applicationName(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolMaxConnectionsKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.maxConnections(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolConnectionAccessTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionAccessTimeout(config.getDuration(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor poolConnectionSurrenderTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionSurrenderTimeout(config.getDuration(key));
    }
    return this;
  }

  public ConnectionConfig extract() {
    return connectionConfigBuilder
        .connectionPoolConfig(connectionPoolConfigBuilder.build())
        .credentials(connectionCredentialsBuilder.build())
        .build();
  }
}
