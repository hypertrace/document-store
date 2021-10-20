package org.hypertrace.core.documentstore;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Interface spec for common operations on a collection of documents */
public interface Collection {
  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc
   * store.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return True if this operation resulted in update of an existing document. False, otherwise.
   * @throws IOException the io exception
   */
  boolean upsert(Key key, Document document) throws IOException;

  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc
   * store.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return Returns the updated document regardless if an update occurred
   * @throws IOException the io exception
   */
  Document upsertAndReturn(Key key, Document document) throws IOException;

  /**
   * Update a sub document
   *
   * @param key Unique key of the document in the collection.
   * @param subDocPath Path to the sub document that needs to be updated
   * @param subDocument Sub document that needs to be updated at the above path
   * @return the boolean
   * @deprecated use {@link #bulkUpdateSubDocs(Map)} ()} instead.
   */
  @Deprecated
  boolean updateSubDoc(Key key, String subDocPath, Document subDocument);

  /**
   * Updates sub documents
   *
   * @param documents contains the mapping of key and the corresponding sub doc update queries
   * @return the update count or -1 if there is any exception
   * @throws Exception the exception
   */
  BulkUpdateResult bulkUpdateSubDocs(Map<Key, Map<String, Document>> documents) throws Exception;

  /**
   * Bulk operation to add to the array value for the given set of keys at given sub doc path
   *
   * @param keys the keys
   * @param subDocPath the sub doc path
   * @param documents the documents
   * @return the bulk update result
   */
  BulkUpdateResult bulkAddToArrayValue(Set<Key> keys, String subDocPath, List<Document> documents)
      throws Exception;

  /**
   * Bulk operation to remove from the array value for the given set of keys at given sub doc path
   *
   * @param keys the keys
   * @param subDocPath the sub doc path
   * @param documents the documents
   * @return the bulk update result
   */
  BulkUpdateResult bulkRemoveFromArrayValue(
      Set<Key> keys, String subDocPath, List<Document> documents) throws Exception;

  /**
   * Search for documents matching the query
   *
   * @param query filter to query matching documents
   * @return {@link Iterator} of matching documents
   */
  Iterator<Document> search(Query query);

  /**
   * Delete the document with the given key.
   *
   * @param key The {@link Key} of the document to be deleted.
   * @return True if the document was deleted, false otherwise.
   */
  boolean delete(Key key);

  /**
   * Deletes a sub document
   *
   * @param key Unique key of the document in the collection
   * @param subDocPath Path to the sub document that needs to be updated
   * @return True if the sub document was deleted
   */
  boolean deleteSubDoc(Key key, String subDocPath);

  /**
   * Deletes all documents in the collection
   *
   * @return true if the documents are deleted
   */
  boolean deleteAll();

  /**
   * Count long.
   *
   * @return the number of documents in the collection
   */
  long count();

  /**
   * Total long.
   *
   * @param query the query
   * @return the total number of documents matching the query applying the filters passed, and
   *     ignoring offset and limit
   */
  long total(Query query);

  /**
   * Bulk upsert boolean.
   *
   * @param documents to be upserted in bulk
   * @return true if the operation succeeded
   */
  boolean bulkUpsert(Map<Key, Document> documents);

  /**
   * Method to bulkUpsert the given documents and return the previous copies of those documents.
   * This helps the clients to see how the documents were prior to upserting them and do that in one
   * less round trip.
   *
   * @param documents the documents
   * @return the iterator
   * @throws IOException the io exception
   */
  Iterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException;

  /** Drops a collections */
  void drop();

  /**
   * create a new document if one doesn't exists with the key
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be created.
   * @return an instance of {@link CreateResult}
   * @throws IOException the io exception
   */
  CreateResult create(Key key, Document document) throws IOException;

  /**
   * Updates existing documents if the corresponding Filter condition evaluates to true
   *
   * @param bulkUpdateRequests the bulk update requests
   * @return an instance of {@link BulkUpdateResult}
   * @throws Exception the exception
   */
  BulkUpdateResult bulkUpdate(List<BulkUpdateRequest> bulkUpdateRequests) throws Exception;

  /**
   * Update an existing document if condition is evaluated to true. Condition will help in providing
   * optimistic locking support for concurrency update.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be updated.
   * @param condition Filter condition to be evaluated if present, on success update the document
   * @return an instance of {@link UpdateResult}
   * @throws IOException the io exception
   */
  UpdateResult update(Key key, Document document, Filter condition) throws IOException;

  /** The constant UNSUPPORTED_QUERY_OPERATION. */
  String UNSUPPORTED_QUERY_OPERATION = "Query operation is not supported";
}
