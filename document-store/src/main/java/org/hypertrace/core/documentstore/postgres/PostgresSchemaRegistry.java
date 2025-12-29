package org.hypertrace.core.documentstore.postgres;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;

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
public class PostgresSchemaRegistry implements SchemaRegistry<PostgresColumnMetadata> {

  /** Default cache expiry time: 24 hours. */
  private static final Duration DEFAULT_CACHE_EXPIRY = Duration.ofHours(24);

  /** Default cooldown period between refresh attempts: 15 minutes. */
  private static final Duration DEFAULT_REFRESH_COOLDOWN = Duration.ofMinutes(15);

  private final LoadingCache<String, Map<String, PostgresColumnMetadata>> cache;
  private final Map<String, Instant> lastRefreshTimes;
  private final Duration refreshCooldown;
  private final Clock clock;

  /**
   * Creates a new schema registry with default settings.
   *
   * <p>Uses default cache expiry of 24 hours and refresh cooldown of 15 minutes.
   *
   * @param fetcher the metadata fetcher to use for loading schema information
   */
  public PostgresSchemaRegistry(PostgresMetadataFetcher fetcher) {
    this(fetcher, DEFAULT_CACHE_EXPIRY, DEFAULT_REFRESH_COOLDOWN, Clock.systemUTC());
  }

  /**
   * Creates a new schema registry with custom cache settings.
   *
   * @param fetcher the metadata fetcher to use for loading schema information
   * @param cacheExpiry how long to keep cached schemas before they expire
   * @param refreshCooldown minimum time between refresh attempts for missing columns
   */
  public PostgresSchemaRegistry(
      PostgresMetadataFetcher fetcher, Duration cacheExpiry, Duration refreshCooldown) {
    this(fetcher, cacheExpiry, refreshCooldown, Clock.systemUTC());
  }

  /**
   * Creates a new schema registry with custom settings and clock (for testing).
   *
   * @param fetcher the metadata fetcher to use for loading schema information
   * @param cacheExpiry how long to keep cached schemas before they expire
   * @param refreshCooldown minimum time between refresh attempts for missing columns
   * @param clock the clock to use for time-based operations
   */
  PostgresSchemaRegistry(
      PostgresMetadataFetcher fetcher,
      Duration cacheExpiry,
      Duration refreshCooldown,
      Clock clock) {
    this.refreshCooldown = refreshCooldown;
    this.clock = clock;
    this.lastRefreshTimes = new ConcurrentHashMap<>();
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(cacheExpiry.toMinutes(), TimeUnit.MINUTES)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Map<String, PostgresColumnMetadata> load(String tableName) {
                    lastRefreshTimes.put(tableName, clock.instant());
                    return fetcher.fetch(tableName);
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
   * @param tableName the name of the table
   * @param colName the name of the column
   * @return the column metadata, or {@code null} if the column does not exist
   */
  @Override
  public PostgresColumnMetadata getColumnOrRefresh(String tableName, String colName) {
    Map<String, PostgresColumnMetadata> schema = getSchema(tableName);

    if (!schema.containsKey(colName) && canRefresh(tableName)) {
      invalidate(tableName);
      schema = getSchema(tableName);
    }

    return schema.get(colName);
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
    return Duration.between(lastRefresh, clock.instant()).compareTo(refreshCooldown) >= 0;
  }
}
