package org.hypertrace.core.documentstore;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Interface spec for common operations on a collection of documents
 */
public interface Collection {
  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc store.
   *
   * @param key      Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return True if this operation resulted in update of an existing document. False, otherwise.
   */
  boolean upsert(Key key, Document document) throws IOException;

  /**
   * Upsert (create a new doc or update if one already exists) the given document into the doc store.
   *
   * @param key      Unique key of the document in the collection.
   * @param document Document to be upserted.
   * @return Returns the updated document regardless if an update occurred
   */
  Document upsertAndReturn(Key key, Document document) throws IOException;

  /**
   * Update a sub document
   * @param key         Unique key of the document in the collection.
   * @param subDocPath  Path to the sub document that needs to be updated
   * @param subDocument Sub document that needs to be updated at the above path
   */
  boolean updateSubDoc(Key key, String subDocPath, Document subDocument);

  /**
   * Search for documents matching the query
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
   * @param key Unique key of the document in the collection
   * @param subDocPath Path to the sub document that needs to be updated
   * @return True if the sub document was deleted
   */
  boolean deleteSubDoc(Key key, String subDocPath);

  /**
   * Deletes all documents in the collection
   * @return true if the documents are deleted
   */
  boolean deleteAll();

  /**
   * @return the number of documents in the collection
   */
  long count();

  /**
   * @return the total number of documents matching the query
   * applying the filters passed, and ignoring offset and limit
   */
  long total(Query query);

  /**
   * @param documents to be upserted in bulk
   * @return true if the operation succeeded
   */
  boolean bulkUpsert(Map<Key, Document> documents);

  /**
   * Method to bulkUpsert the given documents and return the latest copies of those documents.
   * This helps the clients to avoid an additional round trip.
   */
  Iterator<Document> bulkUpsertAndReturn(Map<Key, Document> documents) throws IOException;

  /**
   * Drops a collections
   */
  void drop();
}
