package org.hypertrace.core.documentstore.model.config;

import com.typesafe.config.Config;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig.ConnectionConfigBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials.ConnectionCredentialsBuilder;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionPoolConfig;
import org.hypertrace.core.documentstore.model.config.postgres.PostgresConnectionPoolConfig.ConnectionPoolConfigBuilder;

@Value
public class TypesafeConfigConnectionConfigExtractor {
  @NonNull Config config;
  ConnectionConfigBuilder connectionConfigBuilder;
  ConnectionCredentialsBuilder connectionCredentialsBuilder;
  ConnectionPoolConfigBuilder connectionPoolConfigBuilder;

  private TypesafeConfigConnectionConfigExtractor(
      @NonNull final Config config, @NonNull final String typeKey) {
    this.config = config.getConfig(typeKey);
    this.connectionConfigBuilder = ConnectionConfig.builder().type(config.getString(typeKey));
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = PostgresConnectionPoolConfig.builder();
  }

  private TypesafeConfigConnectionConfigExtractor(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    this.config = config;
    this.connectionConfigBuilder = ConnectionConfig.builder().type(type);
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = PostgresConnectionPoolConfig.builder();
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      final Config config, final String typeKey) {
    return new TypesafeConfigConnectionConfigExtractor(config, typeKey);
  }

  public static TypesafeConfigConnectionConfigExtractor from(
      final Config config, final DatabaseType type) {
    return new TypesafeConfigConnectionConfigExtractor(config, type);
  }

  public TypesafeConfigConnectionConfigExtractor hostKey(@NonNull final String key) {
    connectionConfigBuilder.host(config.getString(key));
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor portKey(@NonNull final String key) {
    connectionConfigBuilder.port(config.getInt(key));
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor usernameKey(@NonNull final String key) {
    connectionCredentialsBuilder.username(config.getString(key));
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor passwordKey(@NonNull final String key) {
    connectionCredentialsBuilder.password(config.getString(key));
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor authDatabaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.authDatabase(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor databaseKey(@NonNull final String key) {
    connectionConfigBuilder.database(config.getString(key));
    return this;
  }

  public TypesafeConfigConnectionConfigExtractor applicationNameKey(@NonNull final String key) {
    connectionConfigBuilder.applicationName(config.getString(key));
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
    return connectionConfigBuilder.credentials(connectionCredentialsBuilder.build()).build();
  }
}
