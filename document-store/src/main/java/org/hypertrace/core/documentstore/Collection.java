package org.hypertrace.core.documentstore;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

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
   * Upsert (create a new doc or update if one already exists) the given document into the doc store
   * if condition is evaluated to true. Provides optimistic lock support for concurrency update.
   *
   * @param key Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @param condition Filter condition to be evaluated, on success update the document
   * @param isUpsert Optional parameter to explicitly control insert or update, default is true.
   *     True indicates if the document doesn't exist, it will insert a new document False indicates
   *     if the document exists, update it otherwise don't do anything.
   * @return True if success. False otherwise.
   */
  boolean upsert(Key key, Document document, Filter condition, @Nullable Boolean isUpsert)
      throws IOException;

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
   */
  boolean updateSubDoc(Key key, String subDocPath, Document subDocument);

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
   * Deletes a sub documents
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
   * @param documents to be upserted in bulk
   * @return true if the operation succeeded
   */
  boolean bulkUpsert(Map<Key, Document> documents);

  /**
   * Method to bulkUpsert the given documents and return the previous copies of those documents.
   * This helps the clients to see how the documents were prior to upserting them and do that in one
   * less round trip.
   */
  Iterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException;

  /** Drops a collections */
  void drop();

  String UNSUPPORTED_QUERY_OPERATION = "Query operation is not supported";
}
