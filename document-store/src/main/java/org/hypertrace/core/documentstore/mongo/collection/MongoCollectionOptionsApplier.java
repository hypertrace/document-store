package org.hypertrace.core.documentstore.mongo.collection;

import static org.hypertrace.core.documentstore.model.options.DataFreshness.SYSTEM_DEFAULT;
import static org.hypertrace.core.documentstore.mongo.collection.MongoReadPreferenceConverter.convert;

import com.mongodb.BasicDBObject;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.config.ConnectionConfig;
import org.hypertrace.core.documentstore.model.options.QueryOptions;

public class MongoCollectionOptionsApplier {
  private final Map<CacheKey, MongoCollection<BasicDBObject>> collectionCache;

  public MongoCollectionOptionsApplier() {
    this.collectionCache = new ConcurrentHashMap<>();
  }

  public MongoCollection<BasicDBObject> applyOptions(
      final ConnectionConfig connectionConfig,
      final QueryOptions queryOptions,
      final MongoCollection<BasicDBObject> collection) {
    final CacheKey cacheKey =
        CacheKey.builder().readPreference(readPreference(connectionConfig, queryOptions)).build();
    return collectionCache.computeIfAbsent(
        cacheKey, key -> collection.withReadPreference(key.readPreference()));
  }

  private ReadPreference readPreference(
      final ConnectionConfig connectionConfig, final QueryOptions queryOptions) {
    return SYSTEM_DEFAULT.equals(queryOptions.dataFreshness())
        ? convert(connectionConfig.dataFreshness())
        : convert(queryOptions.dataFreshness());
  }

  @Value
  @Builder
  @Accessors(fluent = true, chain = true)
  private static class CacheKey {
    ReadPreference readPreference;
  }
}
