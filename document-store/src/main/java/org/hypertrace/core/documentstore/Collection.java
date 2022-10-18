package org.hypertrace.core.documentstore;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;

/** Interface spec for common operations on a collection of documents */
public interface Collection {
  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc
   * store.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return True if this operation resulted in update of an existing document. False, otherwise.
   */
  boolean upsert(Key key, Document document) throws IOException;

  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc
   * store.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return Returns the updated document regardless if an update occurred
   */
  Document upsertAndReturn(Key key, Document document) throws IOException;

  /**
   * Update a sub document
   *
   * @param key Unique key of the document in the collection.
   * @param subDocPath Path to the sub document that needs to be updated
   * @param subDocument Sub document that needs to be updated at the above path
   * @deprecated use {@link #bulkUpdateSubDocs(Map)} ()} instead.
   */
  @Deprecated
  boolean updateSubDoc(Key key, String subDocPath, Document subDocument);

  /**
   * Updates sub documents
   *
   * @param documents contains the mapping of key and the corresponding sub doc update queries
   * @return the update count or -1 if there is any exception
   */
  BulkUpdateResult bulkUpdateSubDocs(Map<Key, Map<String, Document>> documents) throws Exception;

  /**
   * Bulk operation on array value for the given set of keys at given sub doc path
   *
   * @param request bullk array value update request
   * @return the bulk update result
   */
  BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request) throws Exception;

  /**
   * Search for documents matching the query. For advanced functionalities like selecting summing
   * column values, sorting by expressions, etc., ref. {@link #find}
   *
   * @param query filter to query matching documents
   * @return {@link CloseableIterator} of matching documents
   */
  CloseableIterator<Document> search(Query query);

  /**
   * Find the documents matching the query. Note that this method is a generic version of {@link
   * #search(Query)}
   *
   * @param query The query definition to find
   * @return {@link CloseableIterator} of matching documents
   */
  CloseableIterator<Document> find(final org.hypertrace.core.documentstore.query.Query query);

  /**
   * Aggregate the documents conforming to the query specification. SELECT * FROM collection WHERE
   * col1 + col2 > ?
   *
   * @param query The aggregate query specification
   * @return {@link CloseableIterator} of matching documents
   */
  CloseableIterator<Document> aggregate(final org.hypertrace.core.documentstore.query.Query query);

  /**
   * Delete the document with the given key.
   *
   * @param key The {@link Key} of the document to be deleted.
   * @return True if the document was deleted, false otherwise.
   */
  boolean delete(Key key);

  /**
   * Delete the document matching the given filter.
   *
   * @param filter The filter to determine documents to be deleted. Only the filter clause.
   * @return True if the documents are deleted, false otherwise.
   */
  boolean delete(Filter filter);

  /**
   * Delete the documents for the given keys
   *
   * @param keys {@link Key}s of the document to be deleted
   */
  BulkDeleteResult delete(Set<Key> keys);

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

  /** @return the number of documents in the collection */
  long count();

  /**
   * @return the total number of documents matching the query applying the filters passed, and
   *     ignoring offset and limit
   */
  long total(Query query);

  /**
   * Count the result-set size of executing the given query. Note that this method is a generic
   * version of {@link #count()} and {@link #total(Query)}
   *
   * @param query The query definition whose result-set size is to be determined
   * @return The number of documents conforming to the input query
   */
  long count(final org.hypertrace.core.documentstore.query.Query query);

  /**
   * @param documents to be upserted in bulk
   * @return true if the operation succeeded
   */
  boolean bulkUpsert(Map<Key, Document> documents);

  /**
   * Method to bulkUpsert the given documents and return the previous copies of those documents.
   * This helps the clients to see how the documents were prior to upserting them and do that in one
   * less round trip.
   */
  CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException;

  /** Drops a collections */
  void drop();

  /**
   * Create a new document if one doesn't violate any unique constraints (including the unique key
   * constraint)
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be created.
   * @return an instance of {@link CreateResult}
   * @throws DuplicateDocumentException if either
   *     <ul>
   *       <li>a document with the given key already exists or
   *       <li>the given document violates an unique constraint
   *     </ul>
   *
   * @throws IOException if there was any other exception while creating the document
   */
  CreateResult create(Key key, Document document) throws DuplicateDocumentException, IOException;

  /**
   * Updates existing documents if the corresponding Filter condition evaluates to true
   *
   * @param documents to be updated in bulk
   * @return an instance of {@link BulkUpdateResult}
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
   */
  UpdateResult update(Key key, Document document, Filter condition) throws IOException;

  /**
   * Atomically
   *
   * <ol>
   *   <li>reads the first document matching the filter and sorting criteria given in the query,
   *   <li>updates the document as specified in updates and
   *   <li>returns the document (if exists) including the fields selected in the query
   * </ol>
   *
   * @param query The query specifying the desired filter and sorting criteria along with the
   *     necessary selections
   * @param updates The list of sub-document updates to be performed atomically
   * @param updateOptions Options for updating/returning the document
   * @return The old (before update) or new (after update) document optional if one exists,
   *     otherwise an empty optional.
   * @throws IOException if there was any error in updating/fetching the document or the updates is
   *     empty
   * @implSpec The definition of an update here is
   *     <ol>
   *       <li>The existing sub-documents will be updated
   *       <li>New sub-documents will be created if they do not exist
   *       <li>None of the existing sub-documents will be removed
   *     </ol>
   *     $expr
   */
  Optional<Document> update(
      final org.hypertrace.core.documentstore.query.Query query,
      final java.util.Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException;

  String UNSUPPORTED_QUERY_OPERATION = "Query operation is not supported";
}
