package org.hypertrace.core.documentstore.postgres;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;

public class PostgresSchemaRegistry implements SchemaRegistry<PostgresColumnMetadata> {

  private final LoadingCache<String, Map<String, PostgresColumnMetadata>> cache;
  private final PostgresMetadataFetcher fetcher;

  public PostgresSchemaRegistry(PostgresMetadataFetcher fetcher) {
    this.fetcher = fetcher;
    this.cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(24, TimeUnit.HOURS)
            .build(
                new CacheLoader<>() {
                  @Override
                  public Map<String, PostgresColumnMetadata> load(String tableName) {
                    return fetcher.fetch(tableName); // Hardcoded SQL Discovery
                  }
                });
  }

  @Override
  public Map<String, PostgresColumnMetadata> getSchema(String tableName) {
    try {
      return cache.get(tableName);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed to fetch schema for " + tableName, e.getCause());
    }
  }

  @Override
  public void invalidate(String tableName) {
    cache.invalidate(tableName);
  }

  @Override
  public PostgresColumnMetadata getColumnOrRefresh(String tableName, String colName) {
    Map<String, PostgresColumnMetadata> schema = getSchema(tableName);

    if (!schema.containsKey(colName)) {
      invalidate(tableName);
      schema = getSchema(tableName);
    }

    return schema.get(colName);
  }
}
