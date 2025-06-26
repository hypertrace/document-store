package org.hypertrace.core.documentstore;

import static java.util.Map.entry;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.NonNull;
import org.hypertrace.core.documentstore.TypesafeDatastoreConfigAdapter.MongoTypesafeDatastoreConfigAdapter;
import org.hypertrace.core.documentstore.TypesafeDatastoreConfigAdapter.PostgresTypesafeDatastoreConfigAdapter;
import org.hypertrace.core.documentstore.model.config.DatabaseType;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.mongo.MongoDatastore;
import org.hypertrace.core.documentstore.postgres.PostgresDatastore;

public class DatastoreProvider {
  private static final Map<DatabaseType, Function<DatastoreConfig, Datastore>> DATASTORE_MAPPING =
      Map.ofEntries(
          entry(DatabaseType.MONGO, MongoDatastore::new),
          entry(DatabaseType.POSTGRES, PostgresDatastore::new));

  // NOTE: The adapter mapping is only for maintaining backward compatibility.
  // Going forward, we SHOULD NOT be adding more adapters here.
  // Instead, we should migrate the client code to be invoking the non-deprecated methods.
  private static final Map<DatabaseType, Supplier<TypesafeDatastoreConfigAdapter>> ADAPTER_MAP =
      Map.ofEntries(
          entry(DatabaseType.MONGO, MongoTypesafeDatastoreConfigAdapter::new),
          entry(DatabaseType.POSTGRES, PostgresTypesafeDatastoreConfigAdapter::new));

  /**
   * @deprecated Use {@link DatastoreProvider#getDatastore(DatastoreConfig)} instead
   */
  @Deprecated
  public static Datastore getDatastore(String type, Config config) {
    return Optional.ofNullable(type)
        .map(DatabaseType::getType)
        .map(ADAPTER_MAP::get)
        .map(Supplier::get)
        .map(adapter -> adapter.convert(config))
        .map(DatastoreProvider::getDatastore)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "No adapter found for %s. Please migrate to invoke the non-deprecated methods"
                        + type));
  }

  @SuppressWarnings("unused")
  public static Datastore getDatastore(@NonNull final DatastoreConfig datastoreConfig) {
    return Optional.ofNullable(DATASTORE_MAPPING.get(datastoreConfig.type()))
        .map(constructor -> constructor.apply(datastoreConfig))
        .orElseThrow(
            () -> new IllegalArgumentException("Unknown database type: " + datastoreConfig.type()));
  }
}
