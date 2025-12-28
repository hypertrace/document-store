package org.hypertrace.core.documentstore;

import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.commons.ColumnMetadata;
import org.hypertrace.core.documentstore.commons.SchemaRegistry;
import org.hypertrace.core.documentstore.metric.DocStoreMetricProvider;

public interface Datastore {

  Set<String> listCollections();

  boolean createCollection(String collectionName, Map<String, String> options);

  boolean deleteCollection(String collectionName);

  Collection getCollection(String collectionName);

  boolean healthCheck();

  @SuppressWarnings("unused")
  DocStoreMetricProvider getDocStoreMetricProvider();

  default <T extends ColumnMetadata> SchemaRegistry<T> getSchemaRegistry() {
    return null;
  }

  void close();

  /**
   * Returns a collection with the given name and type. A type can be used to specify different
   * storage modes of the collection. For example, a collection can have all top-level fields or a
   * single JSON column that contains all the fields. Both collections are handled differently in
   * this case.
   *
   * @param collectionName name of the collection
   * @param documentType type of the collection. For PG, we support FLAT and Legacy (for backward
   *     compatibility)
   * @return the corresponding collection impl
   */
  default Collection getCollectionForType(String collectionName, DocumentType documentType) {
    throw new UnsupportedOperationException("Unsupported collection type: " + documentType);
  }
}
