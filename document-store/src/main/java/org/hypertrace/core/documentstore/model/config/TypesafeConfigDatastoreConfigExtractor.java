package org.hypertrace.core.documentstore.model.config;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.Value;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig.ConnectionConfigBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionCredentials.ConnectionCredentialsBuilder;
import org.hypertrace.core.documentstore.model.config.ConnectionPoolConfig.ConnectionPoolConfigBuilder;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig.DatastoreConfigBuilder;
import org.hypertrace.core.documentstore.model.config.Endpoint.EndpointBuilder;
import org.hypertrace.core.documentstore.model.options.DataFreshness;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value
public class TypesafeConfigDatastoreConfigExtractor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TypesafeConfigDatastoreConfigExtractor.class);
  private static final String DEFAULT_HOST_KEY = "host";
  private static final String DEFAULT_PORT_KEY = "port";
  private static final String DEFAULT_ENDPOINTS_KEY = "endpoints";
  private static final String DEFAULT_AUTH_DB_KEY = "authDb";
  private static final String DEFAULT_REPLICA_SET_KEY = "replicaSet";
  private static final String DEFAULT_DATABASE_KEY = "database";
  private static final String DEFAULT_USER_KEY = "user";
  private static final String DEFAULT_PASSWORD_KEY = "password";
  private static final String DEFAULT_APP_NAME_KEY = "appName";
  private static final String DEFAULT_MAX_POOL_SIZE_KEY = "maxPoolSize";
  private static final String DEFAULT_CONNECTION_ACCESS_TIMEOUT_KEY = "connectionAccessTimeout";
  private static final String DEFAULT_CONNECTION_IDLE_TIME_KEY = "connectionIdleTime";
  private static final String DEFAULT_AGGREGATION_PIPELINE_MODE_KEY = "aggregationPipelineMode";
  private static final String DEFAULT_DATA_FRESHNESS_KEY = "dataFreshness";
  private static final String DEFAULT_QUERY_TIMEOUT_KEY = "queryTimeout";
  private static final String DEFAULT_CONNECTION_TIMEOUT_KEY = "connectionTimeout";
  private static final String DEFAULT_CUSTOM_PARAMETERS_PREFIX = "customParams";

  @NonNull Config config;
  DatastoreConfigBuilder datastoreConfigBuilder;
  ConnectionConfigBuilder connectionConfigBuilder;
  ConnectionCredentialsBuilder connectionCredentialsBuilder;
  ConnectionPoolConfigBuilder connectionPoolConfigBuilder;
  EndpointBuilder endpointBuilder;

  private TypesafeConfigDatastoreConfigExtractor(
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

  private TypesafeConfigDatastoreConfigExtractor(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    this.config = config;
    this.connectionConfigBuilder = ConnectionConfig.builder().type(type);
    this.connectionCredentialsBuilder = ConnectionCredentials.builder();
    this.connectionPoolConfigBuilder = ConnectionPoolConfig.builder();
    this.datastoreConfigBuilder = DatastoreConfig.builder().type(type);
    this.endpointBuilder = Endpoint.builder();

    final String dataStoreType = type.type();
    this.hostKey(dataStoreType + "." + DEFAULT_HOST_KEY)
        .portKey(dataStoreType + "." + DEFAULT_PORT_KEY)
        .keysForEndpoints(
            dataStoreType + "." + DEFAULT_ENDPOINTS_KEY, DEFAULT_HOST_KEY, DEFAULT_PORT_KEY)
        .authDatabaseKey(dataStoreType + "." + DEFAULT_AUTH_DB_KEY)
        .replicaSetKey(dataStoreType + "." + DEFAULT_REPLICA_SET_KEY)
        .databaseKey(dataStoreType + "." + DEFAULT_DATABASE_KEY)
        .usernameKey(dataStoreType + "." + DEFAULT_USER_KEY)
        .passwordKey(dataStoreType + "." + DEFAULT_PASSWORD_KEY)
        .applicationNameKey(DEFAULT_APP_NAME_KEY)
        .poolMaxConnectionsKey(DEFAULT_MAX_POOL_SIZE_KEY)
        .poolConnectionAccessTimeoutKey(DEFAULT_CONNECTION_ACCESS_TIMEOUT_KEY)
        .poolConnectionSurrenderTimeoutKey(DEFAULT_CONNECTION_IDLE_TIME_KEY)
        .aggregationPipelineMode(DEFAULT_AGGREGATION_PIPELINE_MODE_KEY)
        .dataFreshnessKey(DEFAULT_DATA_FRESHNESS_KEY)
        .queryTimeoutKey(DEFAULT_QUERY_TIMEOUT_KEY)
        .connectionTimeoutKey(DEFAULT_CONNECTION_TIMEOUT_KEY)
        .customParametersKey(DEFAULT_CUSTOM_PARAMETERS_PREFIX);
  }

  public static TypesafeConfigDatastoreConfigExtractor from(
      @NonNull final Config config, @NonNull final String typeKey) {
    return new TypesafeConfigDatastoreConfigExtractor(config, typeKey);
  }

  public static TypesafeConfigDatastoreConfigExtractor from(
      @NonNull final Config config, @NonNull final DatabaseType type) {
    return new TypesafeConfigDatastoreConfigExtractor(config, type);
  }

  public TypesafeConfigDatastoreConfigExtractor keysForEndpoints(
      @NonNull final String endpointsBaseKey,
      @NonNull final String hostKey,
      @NonNull final String portKey) {
    if (!config.hasPath(endpointsBaseKey)) {
      return this;
    }

    for (final Config endpointConfig : config.getConfigList(endpointsBaseKey)) {
      final Endpoint.EndpointBuilder builder = Endpoint.builder();

      if (endpointConfig.hasPath(hostKey)) {
        builder.host(endpointConfig.getString(hostKey));
      }

      if (endpointConfig.hasPath(portKey)) {
        builder.port(endpointConfig.getInt(portKey));
      }

      connectionConfigBuilder.addEndpoint(builder.build());
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor hostKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      endpointBuilder.host(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor portKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      endpointBuilder.port(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor usernameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.username(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor passwordKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.password(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor authDatabaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionCredentialsBuilder.authDatabase(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor databaseKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.database(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor applicationNameKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.applicationName(config.getString(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor replicaSetKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.replicaSet(config.getString(key));
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor customParametersKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      try {
        // Try to extract parameters as an object (Config)
        Config paramConfig = config.getConfig(key);
        paramConfig
            .entrySet()
            .forEach(
                entry -> {
                  connectionConfigBuilder.customParameter(
                      entry.getKey(), paramConfig.getString(entry.getKey()));
                });
      } catch (Exception e) {
        // If not a Config object, log warning
        LOGGER.warn("Custom parameters key '{}' exists but is not a config object", key);
      }
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolMaxConnectionsKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.maxConnections(config.getInt(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolConnectionAccessTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionAccessTimeout(config.getDuration(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor poolConnectionSurrenderTimeoutKey(
      @NonNull final String key) {
    if (config.hasPath(key)) {
      connectionPoolConfigBuilder.connectionSurrenderTimeout(config.getDuration(key));
    }
    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor aggregationPipelineMode(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.aggregationPipelineMode(
          AggregatePipelineMode.valueOf(config.getString(key)));
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor dataFreshnessKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.dataFreshness(DataFreshness.valueOf(config.getString(key)));
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor queryTimeoutKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.queryTimeout(config.getDuration(key));
    }

    return this;
  }

  public TypesafeConfigDatastoreConfigExtractor connectionTimeoutKey(@NonNull final String key) {
    if (config.hasPath(key)) {
      connectionConfigBuilder.connectionTimeout(config.getDuration(key));
    }
    return this;
  }

  public DatastoreConfig extract() {
    if (connectionConfigBuilder.endpoints().isEmpty()
        && !Endpoint.builder().build().equals(endpointBuilder.build())) {
      connectionConfigBuilder.endpoints(List.of(endpointBuilder.build()));
    }

    return datastoreConfigBuilder
        .connectionConfig(
            connectionConfigBuilder
                .connectionPoolConfig(connectionPoolConfigBuilder.build())
                .credentials(connectionCredentialsBuilder.build())
                .customParameters(connectionConfigBuilder.customParameters())
                .build())
        .build();
  }
}
