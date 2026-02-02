package org.hypertrace.core.documentstore.postgres;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lazily-loaded, cached schema registry for PostgreSQL tables.
 *
 * <p>This registry fetches and caches column metadata from PostgreSQL's {@code information_schema}
 * on demand. It provides:
 *
 * <ul>
 *   <li><b>Lazy loading</b>: Schema metadata is fetched only when first requested for the
 *       particular table.
 *   <li><b>TTL-based caching</b>: Cached schemas expire after a configurable duration (default: 24
 *       hours).
 *   <li><b>Circuit breaker</b>: Prevents excessive database calls by enforcing a cooldown period
 *       between refresh attempts for missing columns (default: 15 minutes).
 * </ul>
 *
 * <h3>Usage Example</h3>
 *
 * <pre>{@code
 * PostgresMetadataFetcher fetcher = new PostgresMetadataFetcher(datastore);
 * PostgresSchemaRegistry registry = new PostgresSchemaRegistry(fetcher);
 *
 * // Get all columns for a table
 * Map<String, PostgresColumnMetadata> schema = registry.getSchema("my_table");
 *
 * // Get a specific column, refreshing if not found (respects cooldown)
 * PostgresColumnMetadata column = registry.getColumnOrRefresh("my_table", "my_column");
 * }</pre>
 *
 * <h3>Thread Safety</h3>
 *
 * <p>This class is thread-safe. The underlying Guava {@link LoadingCache} and {@link
 * ConcurrentHashMap} handle concurrent access.
 *
 * @see PostgresMetadataFetcher
 * @see PostgresColumnMetadata
 */
public class PostgresLazyilyLoadedSchemaRegistry implements SchemaRegistry<PostgresColumnMetadata> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PostgresLazyilyLoadedSchemaRegistry.class);

  // The cache registry - Key: Table name, value: Map of column name to column metadata
  private final LoadingCache<String, Map<String, PostgresColumnMetadata>> cache;
  // This tracks when was the last time a cache was refreshed for a table. This helps track the
  // cooldown period on a per-table level
  private final ConcurrentHashMap<String, Instant> lastRefreshTimes;
  // How long to wait for a tables' schema refresh before it is refreshed again
  private final Duration refreshCooldown;

  /**
   * Creates a new schema registry with custom cache settings.
   *
   * @param fetcher the metadata fetcher to use for loading schema information
   * @param cacheExpiry how long to keep cached schemas before they expire
   * @param refreshCooldown minimum time between refresh attempts for missing columns
   */
  public PostgresLazyilyLoadedSchemaRegistry(
      PostgresMetadataFetcher fetcher, Duration cacheExpiry, Duration refreshCooldown) {

    this.refreshCooldown = refreshCooldown;
    this.lastRefreshTimes = new ConcurrentHashMap<>();
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheExpiry.toMinutes(), TimeUnit.MINUTES)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Map<String, PostgresColumnMetadata> load(String tableName) {
                    LOGGER.info("Loading schema for table: {}", tableName);
                    Map<String, PostgresColumnMetadata> updatedSchema = fetcher.fetch(tableName);
                    lastRefreshTimes.put(tableName, Instant.now());
                    LOGGER.info("Successfully loading schema for table: {}", tableName);
                    return updatedSchema;
                  }
                });
  }

  /**
   * Gets the schema for a table, loading it from the database if not cached.
   *
   * @param tableName the name of the table
   * @return a map of column names to their metadata
   * @throws RuntimeException if the schema cannot be fetched
   */
  @Override
  public Map<String, PostgresColumnMetadata> getSchema(String tableName) {
    try {
      return cache.get(tableName);
    } catch (ExecutionException e) {
      LOGGER.error("Could not fetch the schema for table from the cache: {}", tableName, e);
      throw new RuntimeException("Failed to fetch schema for " + tableName, e.getCause());
    }
  }

  /**
   * Invalidates the cached schema for a specific table.
   *
   * <p>The next call to {@link #getSchema(String)} will reload the schema from the database.
   *
   * @param tableName the name of the table to invalidate
   */
  @Override
  public void invalidate(String tableName) {
    LOGGER.info("Invalidating schema for table: {}", tableName);
    cache.invalidate(tableName);
  }

  /**
   * Gets metadata for a specific column, optionally refreshing the schema if the column is not
   * found.
   *
   * <p>This method implements a circuit breaker pattern:
   *
   * <ul>
   *   <li>If the column exists in the cached schema, it is returned immediately.
   *   <li>If the column is not found and the cooldown period has elapsed since the last refresh,
   *       the schema is reloaded from the database.
   *   <li>If the column is not found but the cooldown period has not elapsed, {@code null} is
   *       returned without hitting the database.
   * </ul>
   *
   * <p>Note that this is a check-then-act sequence that should ideally be atomic. However, this
   * method is deliberately not thread-safe since even in case of a data race, it will result in one
   * extra call to the DB, which will not be allowed anyway due to the cooldown period having been
   * reset by the previous call. This is likely to be more performant than contending for locks.
   *
   * @param tableName the name of the table
   * @param colName the name of the column
   * @return an Optional containing the column metadata, or empty if the column does not exist
   */
  @Override
  public Optional<PostgresColumnMetadata> getColumnOrRefresh(String tableName, String colName) {
    Map<String, PostgresColumnMetadata> schema = getSchema(tableName);

    if (!schema.containsKey(colName) && canRefresh(tableName)) {
      invalidate(tableName);
      schema = getSchema(tableName);
    }

    return Optional.ofNullable(schema.get(colName));
  }

  /**
   * Checks if a refresh is allowed for the given table based on the cooldown period.
   *
   * @param tableName the name of the table
   * @return {@code true} if the cooldown period has elapsed since the last refresh
   */
  private boolean canRefresh(String tableName) {
    Instant lastRefresh = lastRefreshTimes.get(tableName);
    if (lastRefresh == null) {
      return true;
    }
    return Duration.between(lastRefresh, Instant.now()).compareTo(refreshCooldown) >= 0;
  }

  /**
   * Returns the primary key column name for the given table. Currently, composite PKs are not
   * supported. <a
   * href="See:">https://github.com/hypertrace/document-store/pull/268#discussion_r27526</a>59524
   *
   * @param tableName the name of the table
   * @return optional of the primary key column name, or empty if no primary key is found
   */
  @Override
  public Optional<String> getPrimaryKeyColumn(String tableName) {
    Map<String, PostgresColumnMetadata> schema = getSchema(tableName);
    return schema.values().stream()
        .filter(PostgresColumnMetadata::isPrimaryKey)
        .map(PostgresColumnMetadata::getName)
        .findFirst();
  }
}
