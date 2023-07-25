package org.hypertrace.core.documentstore;

import static java.util.Map.entry;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.NonNull;
import org.hypertrace.core.documentstore.model.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;

public class DatastoreProvider {
  private static final Map<DatabaseType, Supplier<Datastore>> DATASTORE_MAPPING =
      Map.ofEntries(
          entry(DatabaseType.MONGO, MongoDatastore::new),
          entry(DatabaseType.POSTGRES, PostgresDatastore::new));

  /** @deprecated Use {@link DatastoreProvider#getDatastore(DatastoreConfig)} instead */
  @Deprecated
  public static Datastore getDatastore(String type, Config config) {
    return Optional.ofNullable(type)
        .map(DatabaseType::getType)
        .map(DATASTORE_MAPPING::get)
        .map(Supplier::get)
        .map(store -> initialize(store, config))
        .orElseThrow(() -> new IllegalArgumentException("Unknown database type: " + type));
  }

  @SuppressWarnings("unused")
  public static Datastore getDatastore(@NonNull final DatastoreConfig datastoreConfig) {
    final ConnectionConfig connectionConfig = datastoreConfig.connectionConfig();
    return Optional.ofNullable(DATASTORE_MAPPING.get(connectionConfig.type()))
        .map(Supplier::get)
        .map(store -> initialize(store, connectionConfig))
        .orElseThrow(
            () ->
                new IllegalArgumentException("Unknown database type: " + connectionConfig.type()));
  }

  private static Datastore initialize(
      final Datastore datastore, final ConnectionConfig connectionConfig) {
    datastore.init(connectionConfig);
    return datastore;
  }

  private static Datastore initialize(final Datastore datastore, final Config config) {
    datastore.init(config);
    return datastore;
  }
}
