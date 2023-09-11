package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.commons.DocStoreConstants.CREATED_TIME;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.LAST_UPDATED_TIME;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.dbObjectToDocument;
import static org.hypertrace.core.documentstore.mongo.MongoUtils.sanitizeJsonString;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.mongo.query.MongoQueryExecutor;
import org.hypertrace.core.documentstore.mongo.update.MongoUpdateExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the {@link Collection} interface with MongoDB as the backend */
public class MongoCollection implements Collection {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoCollection.class);

  // Fields automatically added for each document
  public static final String ID_KEY = "_id";
  private static final String LAST_UPDATE_TIME = "_lastUpdateTime";

  private static final int MAX_RETRY_ATTEMPTS_FOR_DUPLICATE_KEY_ISSUE = 2;
  private static final int DELAY_BETWEEN_RETRIES_MILLIS = 10;
  private static final int MONGODB_DUPLICATE_KEY_ERROR_CODE = 11000;

  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;
  private final MongoQueryExecutor queryExecutor;
  private final MongoUpdateExecutor updateExecutor;

  /**
   * The current MongoDB servers we use have a known issue - https://jira.mongodb
   * .org/browse/SERVER-47212 where the findAndModify operation might fail with duplicate key
   * exception and server was supposed to retry that but it doesn't. Since the fix isn't available
   * in the released MongoDB versions, we are retrying the upserts in these cases in client layer so
   * that we avoid frequent failures in this layer. TODO: This code should be removed once MongoDB
   * server is upgraded to 4.7.0+
   */
  private final RetryPolicy<Object> upsertRetryPolicy =
      new RetryPolicy<>()
          .handleIf(
              failure ->
                  failure instanceof MongoCommandException
                      && ((MongoCommandException) failure).getErrorCode()
                          == MONGODB_DUPLICATE_KEY_ERROR_CODE)
          .withDelay(Duration.ofMillis(DELAY_BETWEEN_RETRIES_MILLIS))
          .withMaxRetries(MAX_RETRY_ATTEMPTS_FOR_DUPLICATE_KEY_ISSUE);

  private final RetryPolicy<Object> bulkWriteRetryPolicy =
      new RetryPolicy<>()
          .handleIf(
              failure ->
                  failure instanceof MongoBulkWriteException
                      && allBulkWriteErrorsAreDueToDuplicateKey((MongoBulkWriteException) failure))
          .withDelay(Duration.ofMillis(DELAY_BETWEEN_RETRIES_MILLIS))
          .withMaxRetries(MAX_RETRY_ATTEMPTS_FOR_DUPLICATE_KEY_ISSUE);

  MongoCollection(com.mongodb.client.MongoCollection<BasicDBObject> collection) {
    this.collection = collection;
    this.queryExecutor = new MongoQueryExecutor(collection);
    this.updateExecutor = new MongoUpdateExecutor(collection);
  }

  /**
   * Returns true if all the BulkWriteErrors that are present in the given BulkWriteException have
   * happened due to duplicate key errors.
   */
  private boolean allBulkWriteErrorsAreDueToDuplicateKey(
      MongoBulkWriteException bulkWriteException) {
    return bulkWriteException.getWriteErrors().stream()
        .allMatch(error -> error.getCode() == MONGODB_DUPLICATE_KEY_ERROR_CODE);
  }

  /**
   * Adds the following fields automatically: _id, _lastUpdateTime, lastUpdatedTime and created Time
   */
  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try {
      UpdateOptions options = new UpdateOptions().upsert(true);
      UpdateResult writeResult =
          collection.updateOne(
              this.selectionCriteriaForKey(key), this.prepareUpsert(key, document), options);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + writeResult);
      }

      return (writeResult.getUpsertedId() != null || writeResult.getModifiedCount() > 0);
    } catch (IOException e) {
      LOGGER.error("Exception upserting document. key: {} content:{}", key, document, e);
      throw e;
    }
  }

  /**
   * Bulk updates existing documents if condition for the corresponding document evaluates to true.
   */
  @Override
  public BulkUpdateResult bulkUpdate(List<BulkUpdateRequest> bulkUpdateRequests) throws Exception {
    try {
      BulkWriteResult result = bulkUpdateImpl(bulkUpdateRequests);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(result.toString());
      }
      return new BulkUpdateResult(result.getModifiedCount());
    } catch (IOException | MongoServerException e) {
      LOGGER.error("Error during bulk update for documents:{}", bulkUpdateRequests, e);
      throw new Exception(e);
    }
  }

  private BulkWriteResult bulkUpdateImpl(List<BulkUpdateRequest> bulkUpdateRequests)
      throws JsonProcessingException {
    List<UpdateOneModel<BasicDBObject>> bulkCollection = new ArrayList<>();
    for (BulkUpdateRequest bulkUpdateRequest : bulkUpdateRequests) {
      Key key = bulkUpdateRequest.getKey();

      Map<String, Object> conditionMap =
          bulkUpdateRequest.getFilter() == null
              ? new HashMap<>()
              : MongoQueryParser.parseFilter(bulkUpdateRequest.getFilter());
      conditionMap.put(ID_KEY, key.toString());
      BasicDBObject conditionObject = new BasicDBObject(conditionMap);

      // update if filter condition is satisfied
      bulkCollection.add(
          new UpdateOneModel<>(
              conditionObject,
              prepareUpsert(key, bulkUpdateRequest.getDocument()),
              new UpdateOptions().upsert(false)));
    }

    return Failsafe.with(bulkWriteRetryPolicy)
        .get(() -> collection.bulkWrite(bulkCollection, new BulkWriteOptions().ordered(false)));
  }

  /**
   * Update an existing document if condition is evaluated to true. Conditional will help in
   * providing optimistic locking support for concurrency update.
   */
  @Override
  public org.hypertrace.core.documentstore.UpdateResult update(
      Key key, Document document, Filter condition) throws IOException {
    try {
      Map<String, Object> conditionMap =
          condition == null ? new HashMap<>() : MongoQueryParser.parseFilter(condition);
      conditionMap.put(ID_KEY, key.toString());
      BasicDBObject conditionObject = new BasicDBObject(conditionMap);
      UpdateOptions options = new UpdateOptions().upsert(false);

      UpdateResult writeResult =
          collection.updateOne(conditionObject, this.prepareUpsert(key, document), options);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Update result: " + writeResult);
      }
      return new org.hypertrace.core.documentstore.UpdateResult(writeResult.getModifiedCount());
    } catch (Exception e) {
      LOGGER.error("Exception updating document. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  @Override
  public CreateResult create(Key key, Document document) throws IOException {
    try {
      BasicDBObject basicDBObject = this.prepareInsert(key, document);
      InsertOneResult insertOneResult = collection.insertOne(basicDBObject);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Create result: " + insertOneResult);
      }
      return new CreateResult(insertOneResult.getInsertedId() != null);
    } catch (final MongoWriteException e) {
      if (e.getCode() == MONGODB_DUPLICATE_KEY_ERROR_CODE) {
        throw new DuplicateDocumentException();
      }

      LOGGER.error("SQLException creating document. key: " + key + " content: " + document, e);
      throw new IOException(e);
    } catch (final Exception e) {
      LOGGER.error("SQLException creating document. key: " + key + " content: " + document, e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean createOrReplace(final Key key, final Document document) throws IOException {
    try {
      final UpdateOptions options = new UpdateOptions().upsert(true);
      final UpdateResult updateResult =
          collection.updateOne(
              this.selectionCriteriaForKey(key),
              this.prepareForCreateOrReplace(key, document),
              options);
      LOGGER.debug("Create or replace result: {}", updateResult);
      return updateResult.getUpsertedId() != null;
    } catch (IOException e) {
      LOGGER.error("Exception creating/replacing document. key: {} content:{}", key, document, e);
      throw e;
    }
  }

  /**
   * Adds the following fields automatically: _id, _lastUpdateTime, lastUpdatedTime and created Time
   */
  @Override
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    BasicDBObject upsertResult =
        Failsafe.with(upsertRetryPolicy)
            .get(
                () ->
                    collection.findOneAndUpdate(
                        this.selectionCriteriaForKey(key),
                        this.prepareUpsert(key, document),
                        new FindOneAndUpdateOptions()
                            .upsert(true)
                            .returnDocument(ReturnDocument.AFTER)));
    if (upsertResult == null) {
      throw new IOException("Could not upsert the document with key: " + key);
    }

    return dbObjectToDocument(upsertResult);
  }

  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private List<BasicDBObject> prepareForCreateOrReplace(final Key key, final Document document)
      throws JsonProcessingException {
    final long now = System.currentTimeMillis();
    final BasicDBObject project =
        new BasicDBObject(
            "$project",
            new BasicDBObject(
                CREATED_TIME,
                new BasicDBObject(
                    "$ifNull",
                    new BsonValue[] {new BsonString("$" + CREATED_TIME), new BsonInt64(now)})));
    final BasicDBObject set = new BasicDBObject("$set", prepareDocument(key, document, now));

    return List.of(project, set);
  }

  private BasicDBObject prepareUpsert(Key key, Document document) throws JsonProcessingException {
    long now = System.currentTimeMillis();
    BasicDBObject setObject = prepareDocument(key, document, now);
    return new BasicDBObject("$set", setObject)
        .append("$currentDate", new BasicDBObject(LAST_UPDATE_TIME, true))
        .append("$setOnInsert", new BasicDBObject(CREATED_TIME, now));
  }

  private BasicDBObject prepareInsert(Key key, Document document) throws JsonProcessingException {
    long now = System.currentTimeMillis();
    BasicDBObject insertDbObject = prepareDocument(key, document, now);
    insertDbObject.put(CREATED_TIME, now);
    return insertDbObject;
  }

  private BasicDBObject prepareDocument(Key key, Document document, long now)
      throws JsonProcessingException {
    BasicDBObject basicDBObject = getSanitizedBasicDBObject(document);
    basicDBObject.put(ID_KEY, key.toString());
    basicDBObject.put(LAST_UPDATED_TIME, now);
    return basicDBObject;
  }

  /** Updates auto-field lastUpdatedTime when sub doc is updated */
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    try {
      BasicDBObject dbObject =
          new BasicDBObject(subDocPath, getSanitizedBasicDBObject(subDocument));
      dbObject.append(LAST_UPDATED_TIME, System.currentTimeMillis());
      BasicDBObject setObject = new BasicDBObject("$set", dbObject);

      UpdateResult writeResult =
          collection.updateOne(selectionCriteriaForKey(key), setObject, new UpdateOptions());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + writeResult);
      }
      // TODO:look into the writeResult to ensure it was successful. Was not easy to find this from
      // docs.
      return true;
    } catch (IOException e) {
      LOGGER.error("Exception updating document. key: {} content:{}", key, subDocument);
      return false;
    }
  }

  @Override
  public BulkUpdateResult bulkUpdateSubDocs(Map<Key, Map<String, Document>> documents)
      throws Exception {
    List<UpdateManyModel<BasicDBObject>> bulkWriteUpdate = new ArrayList<>();
    for (Key key : documents.keySet()) {
      Map<String, Document> subDocuments = documents.get(key);
      List<BasicDBObject> updateOperations = new ArrayList<>();
      for (String subDocPath : subDocuments.keySet()) {
        Document subDocument = subDocuments.get(subDocPath);
        try {
          BasicDBObject setObject = getSubDocumentUpdateObject(subDocPath, subDocument);
          updateOperations.add(setObject);
        } catch (Exception e) {
          LOGGER.error("Exception updating document. key: {} content:{}", key, subDocument);
          throw e;
        }
      }
      bulkWriteUpdate.add(
          new UpdateManyModel(selectionCriteriaForKey(key), updateOperations, new UpdateOptions()));
    }
    if (bulkWriteUpdate.isEmpty()) {
      return new BulkUpdateResult(0);
    }
    BulkWriteResult writeResult = collection.bulkWrite(bulkWriteUpdate);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Write result: " + writeResult);
    }
    return new BulkUpdateResult(writeResult.getModifiedCount());
  }

  @Override
  public BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request)
      throws Exception {
    List<BasicDBObject> basicDBObjects = new ArrayList<>();
    try {
      for (Document subDocument : request.getSubDocuments()) {
        basicDBObjects.add(getSanitizedBasicDBObject(subDocument));
      }
    } catch (Exception e) {
      LOGGER.error(
          "Exception updating document. keys: {} operation {} subDocPath {} subDocuments :{}",
          request.getKeys(),
          request.getOperation(),
          request.getSubDocPath(),
          request.getSubDocuments());
      throw e;
    }
    BasicDBObject operationObject;
    switch (request.getOperation()) {
      case ADD:
        operationObject = getAddOperationObject(request.getSubDocPath(), basicDBObjects);
        break;
      case REMOVE:
        operationObject = getRemoveOperationObject(request.getSubDocPath(), basicDBObjects);
        break;
      case SET:
        operationObject = getSetOperationObject(request.getSubDocPath(), basicDBObjects);
        break;
      default:
        throw new UnsupportedOperationException("Unknown operation : " + request.getOperation());
    }

    List<UpdateManyModel<BasicDBObject>> bulkWriteUpdate =
        List.of(
            new UpdateManyModel(
                selectionCriteriaForKeys(request.getKeys()), operationObject, new UpdateOptions()));
    BulkWriteResult writeResult = collection.bulkWrite(bulkWriteUpdate);
    LOGGER.debug("Write result for bulkOperationOnArrayValue: {}", writeResult);
    return new BulkUpdateResult(writeResult.getModifiedCount());
  }

  private BasicDBObject getSubDocumentUpdateObject(
      final String subDocPath, final Document subDocument) {
    try {
      /* Wrapping the subDocument with $literal to be able to provide empty object "{}" as value
       *  Throws error otherwise if empty object is provided as value.
       *  https://jira.mongodb.org/browse/SERVER-54046 */
      BasicDBObject literalObject =
          new BasicDBObject("$literal", getSanitizedBasicDBObject(subDocument));
      BasicDBObject dbObject = new BasicDBObject(subDocPath, literalObject);
      dbObject.append(LAST_UPDATED_TIME, System.currentTimeMillis());
      return new BasicDBObject("$set", dbObject);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private BasicDBObject getAddOperationObject(
      String subDocPath, List<BasicDBObject> basicDBObjects) {
    BasicDBObject eachObject = new BasicDBObject("$each", basicDBObjects);
    BasicDBObject subDocPathObject = new BasicDBObject(subDocPath, eachObject);
    return new BasicDBObject("$addToSet", subDocPathObject)
        .append("$set", new BasicDBObject(LAST_UPDATED_TIME, System.currentTimeMillis()));
  }

  private BasicDBObject getRemoveOperationObject(
      String subDocPath, List<BasicDBObject> basicDBObjects) {
    BasicDBObject subDocPathObject = new BasicDBObject(subDocPath, basicDBObjects);
    return new BasicDBObject("$pullAll", subDocPathObject)
        .append("$set", new BasicDBObject(LAST_UPDATED_TIME, System.currentTimeMillis()));
  }

  private BasicDBObject getSetOperationObject(
      String subDocPath, List<BasicDBObject> basicDBObjects) {
    BasicDBObject subDocPathObject = new BasicDBObject(subDocPath, basicDBObjects);
    subDocPathObject.append(LAST_UPDATED_TIME, System.currentTimeMillis());
    return new BasicDBObject("$set", subDocPathObject);
  }

  private BasicDBObject getSanitizedBasicDBObject(Document document)
      throws JsonProcessingException {
    String jsonString = document.toJson();
    final String sanitizedJsonString = sanitizeJsonString(jsonString);
    return BasicDBObject.parse(sanitizedJsonString);
  }

  @Override
  public CloseableIterator<Document> search(Query query) {
    Map<String, Object> map = new HashMap<>();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      map = MongoQueryParser.parseFilter(query.getFilter());
    }

    Bson projection = new BasicDBObject();
    if (!query.getSelections().isEmpty()) {
      projection = Projections.include(query.getSelections());
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Sending query to mongo: {} : {}",
          collection.getNamespace().getCollectionName(),
          Arrays.toString(map.entrySet().toArray()));
    }

    // Assume its SimpleAndQuery for now
    BasicDBObject ref = new BasicDBObject(map);
    FindIterable<BasicDBObject> cursor = collection.find(ref).projection(projection);

    Integer offset = query.getOffset();
    if (offset != null && offset >= 0) {
      cursor = cursor.skip(offset);
    }

    Integer limit = query.getLimit();
    if (limit != null && limit >= 0) {
      cursor = cursor.limit(limit);
    }

    if (!query.getOrderBys().isEmpty()) {
      BasicDBObject orderBy =
          new BasicDBObject(MongoQueryParser.parseOrderBys(query.getOrderBys()));
      cursor.sort(orderBy);
    }

    final MongoCursor<BasicDBObject> mongoCursor = cursor.cursor();
    return convertToDocumentIterator(mongoCursor);
  }

  @Override
  public CloseableIterator<Document> find(
      final org.hypertrace.core.documentstore.query.Query query) {
    return convertToDocumentIterator(queryExecutor.find(query));
  }

  @Override
  public CloseableIterator<Document> aggregate(
      final org.hypertrace.core.documentstore.query.Query query) {
    return convertToDocumentIterator(queryExecutor.aggregate(query));
  }

  @Override
  public Optional<Document> update(
      final org.hypertrace.core.documentstore.query.Query query,
      final java.util.Collection<SubDocumentUpdate> updates,
      final org.hypertrace.core.documentstore.model.options.UpdateOptions updateOptions)
      throws IOException {
    return updateExecutor.update(query, updates, updateOptions);
  }

  @Override
  public CloseableIterator<Document> bulkUpdate(
      org.hypertrace.core.documentstore.query.Query query,
      final java.util.Collection<SubDocumentUpdate> updates,
      final org.hypertrace.core.documentstore.model.options.UpdateOptions updateOptions)
      throws IOException {
    return updateExecutor
        .bulkUpdate(query, updates, updateOptions)
        .map(this::convertToDocumentIterator)
        .orElseGet(CloseableIterator::emptyIterator);
  }

  @Override
  public long count(org.hypertrace.core.documentstore.query.Query query) {
    return queryExecutor.count(query);
  }

  @Override
  public boolean delete(Key key) {
    DeleteResult deleteResult = collection.deleteOne(this.selectionCriteriaForKey(key));
    return deleteResult.getDeletedCount() > 0;
  }

  @Override
  public boolean delete(Filter filter) {
    if (filter == null) {
      throw new UnsupportedOperationException("Filter must be provided");
    }
    Map<String, Object> map = MongoQueryParser.parseFilter(filter);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Sending delete query to mongo: {} : {}",
          collection.getNamespace().getCollectionName(),
          Arrays.toString(map.entrySet().toArray()));
    }
    if (map.isEmpty()) {
      throw new UnsupportedOperationException("Parsed filter is invalid");
    }
    BasicDBObject ref = new BasicDBObject(map);
    DeleteResult deleteResult = collection.deleteMany(ref);
    return deleteResult.getDeletedCount() > 0;
  }

  @Override
  public BulkDeleteResult delete(Set<Key> keys) {
    DeleteResult deleteResult = collection.deleteMany(this.selectionCriteriaForKeys(keys));
    return new BulkDeleteResult(deleteResult.getDeletedCount());
  }

  @Override
  public boolean deleteSubDoc(Key key, String subDocPath) {
    BasicDBObject unsetObject = new BasicDBObject("$unset", new BasicDBObject(subDocPath, ""));

    UpdateResult updateResult =
        collection.updateOne(this.selectionCriteriaForKey(key), unsetObject);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Write result: " + updateResult.toString());
    }

    return updateResult.getModifiedCount() > 0;
  }

  @Override
  public boolean deleteAll() {
    collection.deleteMany(new BasicDBObject());

    // If there was no exception, the operation is successful.
    return true;
  }

  @Override
  public long count() {
    return collection.countDocuments();
  }

  @Override
  public long total(Query query) {
    Map<String, Object> map = new HashMap<>();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      map = MongoQueryParser.parseFilter(query.getFilter());
    }

    return collection.countDocuments(new BasicDBObject(map));
  }

  /**
   * Adds the following fields automatically: _id, _lastUpdateTime, lastUpdatedTime and created Time
   */
  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    try {
      BulkWriteResult result = bulkUpsertImpl(documents);
      LOGGER.debug(result.toString());
      return true;
    } catch (IOException | MongoServerException e) {
      LOGGER.error("Error during bulk upsert for documents:{}", documents, e);
      return false;
    }
  }

  private BulkWriteResult bulkUpsertImpl(Map<Key, Document> documents)
      throws JsonProcessingException {
    List<UpdateOneModel<BasicDBObject>> bulkCollection = new ArrayList<>();
    for (Entry<Key, Document> entry : documents.entrySet()) {
      Key key = entry.getKey();
      // insert or overwrite
      bulkCollection.add(
          new UpdateOneModel<>(
              this.selectionCriteriaForKey(key),
              prepareUpsert(key, entry.getValue()),
              new UpdateOptions().upsert(true)));
    }

    return Failsafe.with(bulkWriteRetryPolicy)
        .get(() -> collection.bulkWrite(bulkCollection, new BulkWriteOptions().ordered(false)));
  }

  @Override
  public CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    try {
      // First get all the documents for the given keys.
      FindIterable<BasicDBObject> cursor =
          collection.find(selectionCriteriaForKeys(documents.keySet()));
      final MongoCursor<BasicDBObject> mongoCursor = cursor.cursor();

      // Now go ahead and do the bulk upsert.
      BulkWriteResult result = bulkUpsertImpl(documents);
      LOGGER.debug(result.toString());

      return convertToDocumentIterator(mongoCursor);
    } catch (JsonProcessingException e) {
      LOGGER.error("Error during bulk upsert for documents:{}", documents, e);
      throw new IOException("Error during bulk upsert.");
    }
  }

  @Override
  public void drop() {
    collection.drop();
  }

  private BasicDBObject selectionCriteriaForKey(Key key) {
    return new BasicDBObject(ID_KEY, key.toString());
  }

  private BasicDBObject selectionCriteriaForKeys(Set<Key> keys) {
    return new BasicDBObject(
        Map.of(
            ID_KEY,
            new BasicDBObject(
                "$in", keys.stream().map(Key::toString).collect(Collectors.toList()))));
  }

  private CloseableIterator<Document> convertToDocumentIterator(MongoCursor<BasicDBObject> cursor) {
    return new MongoResultsIterator(cursor);
  }

  static class MongoResultsIterator implements CloseableIterator<Document> {
    private final MongoCursor<BasicDBObject> cursor;
    private boolean closed = false;

    public MongoResultsIterator(final MongoCursor<BasicDBObject> cursor) {
      this.cursor = cursor;
    }

    @Override
    public void close() {
      if (!closed) {
        cursor.close();
      }
      closed = true;
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = !closed && cursor.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    }

    @Override
    public Document next() {
      try {
        return dbObjectToDocument(cursor.next());
      } catch (Exception ex) {
        close();
        throw ex;
      }
    }
  }
}
