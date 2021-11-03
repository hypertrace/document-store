package org.hypertrace.core.documentstore.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.AggregateIterable;
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of the {@link Collection} interface with MongoDB as the backend */
public class MongoCollection implements Collection {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoCollection.class);

  // Fields automatically added for each document
  public static final String ID_KEY = "_id";
  private static final String LAST_UPDATE_TIME = "_lastUpdateTime";
  private static final String LAST_UPDATED_TIME = "lastUpdatedTime";
  /* follow json/protobuf convention to make it deser, let's not make our life harder */
  private static final String CREATED_TIME = "createdTime";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final int MAX_RETRY_ATTEMPTS_FOR_DUPLICATE_KEY_ISSUE = 2;
  private static final int DELAY_BETWEEN_RETRIES_MILLIS = 10;
  private static final int MONGODB_DUPLICATE_KEY_ERROR_CODE = 11000;

  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;

  /**
   * The current MongoDB servers we use have a known issue -
   * https://jira.mongodb.org/browse/SERVER-47212 where the findAndModify operation might fail with
   * duplicate key exception and server was supposed to retry that but it doesn't. Since the fix
   * isn't available in the released MongoDB versions, we are retrying the upserts in these cases in
   * client layer so that we avoid frequent failures in this layer. TODO: This code should be
   * removed once MongoDB server is upgraded to 4.7.0+
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
        LOGGER.debug("Write result: " + writeResult.toString());
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
        LOGGER.debug("Update result: " + writeResult.toString());
      }
      return new org.hypertrace.core.documentstore.UpdateResult(writeResult.getModifiedCount());
    } catch (Exception e) {
      LOGGER.error("Exception updating document. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  /** create a new document if one doesn't exists with key */
  @Override
  public CreateResult create(Key key, Document document) throws IOException {
    try {
      BasicDBObject basicDBObject = this.prepareInsert(key, document);
      InsertOneResult insertOneResult = collection.insertOne(basicDBObject);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Create result: " + insertOneResult.toString());
      }
      return new CreateResult(insertOneResult.getInsertedId() != null);
    } catch (Exception e) {
      LOGGER.error("Exception creating document. key: {} content:{}", key, document, e);
      throw new IOException(e);
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

    return this.dbObjectToDocument(upsertResult);
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
    BasicDBObject basicDBObject = getSanitizedObject(document);
    basicDBObject.put(ID_KEY, key.toString());
    basicDBObject.put(LAST_UPDATED_TIME, now);
    return basicDBObject;
  }

  /** Updates auto-field lastUpdatedTime when sub doc is updated */
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    try {
      BasicDBObject dbObject = new BasicDBObject(subDocPath, getSanitizedObject(subDocument));
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
          BasicDBObject dbObject = new BasicDBObject(subDocPath, getSanitizedObject(subDocument));
          dbObject.append(LAST_UPDATED_TIME, System.currentTimeMillis());
          BasicDBObject setObject = new BasicDBObject("$set", dbObject);
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
        basicDBObjects.add(getSanitizedObject(subDocument));
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

  private BasicDBObject getSanitizedObject(Document document) throws JsonProcessingException {
    String jsonString = document.toJson();
    JsonNode jsonNode = MAPPER.readTree(jsonString);
    // escape "." and "$" in field names since Mongo DB does not like them
    JsonNode sanitizedJsonNode = recursiveClone(jsonNode, this::encodeKey);
    String sanitizedJsonString = MAPPER.writeValueAsString(sanitizedJsonNode);
    return BasicDBObject.parse(sanitizedJsonString);
  }

  private JsonNode recursiveClone(JsonNode src, Function<String, String> function) {
    if (!src.isObject()) {
      return src;
    }
    ObjectNode tgt = JsonNodeFactory.instance.objectNode();
    Iterator<Entry<String, JsonNode>> fields = src.fields();
    while (fields.hasNext()) {
      Entry<String, JsonNode> next = fields.next();
      String fieldName = next.getKey();
      String newFieldName = function.apply(fieldName);
      JsonNode value = next.getValue();
      JsonNode newValue = value;
      if (value.isObject()) {
        newValue = recursiveClone(value, function);
      }
      tgt.set(newFieldName, newValue);
    }
    return tgt;
  }

  private String encodeKey(String key) {
    return key.replace("\\", "\\\\").replace("$", "\\u0024").replace(".", "\\u002e");
  }

  private String decodeKey(String key) {
    return key.replace("\\u002e", ".").replace("\\u0024", "$").replace("\\\\", "\\");
  }

  @Override
  public Iterator<Document> search(Query query) {
    BasicDBObject ref = new BasicDBObject(parseFilter(query.getFilter()));
    FindIterable<BasicDBObject> cursor = collection.find(ref);
    final MongoCursor<BasicDBObject> mongoCursor = processCursor(query, cursor);
    return getIteratorFromCursor(mongoCursor);
  }

  @Override
  public Iterator<Document> aggregate(Query query) {
    Map<String, Object> whereFilterMap = parseFilter(query.getFilter());
    BasicDBObject whereFilters = new BasicDBObject(Map.of("$match", whereFilterMap));

    LinkedHashMap<String, Object> groupByMap = MongoQueryParser.parseGroupBy(query.getGroupBy());
    BasicDBObject groupBy = new BasicDBObject(Map.of("$group", groupByMap));

    Map<String, Object> havingFilterMap = parseFilter(query.getFilter());
    BasicDBObject havingFilters = new BasicDBObject(Map.of("$match", havingFilterMap));

    List<BasicDBObject> pipeline = Arrays.asList(whereFilters, groupBy, havingFilters);

    if (!query.getOrderBys().isEmpty()) {
      LinkedHashMap<String, Object> orderByMap =
          MongoQueryParser.parseOrderBys(query.getOrderBys());
      BasicDBObject orderBy = new BasicDBObject(Map.of("$sort", orderByMap));
      pipeline.add(orderBy);
    }

    AggregateIterable<BasicDBObject> cursor = collection.aggregate(pipeline);
    final MongoCursor<BasicDBObject> mongoCursor = cursor.cursor();
    return getIteratorFromCursor(mongoCursor);
  }

  @Override
  public boolean delete(Key key) {
    DeleteResult deleteResult = collection.deleteOne(this.selectionCriteriaForKey(key));
    return deleteResult.getDeletedCount() > 0;
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
  public Iterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    try {
      // First get all the documents for the given keys.
      FindIterable<BasicDBObject> cursor =
          collection.find(selectionCriteriaForKeys(documents.keySet()));
      final MongoCursor<BasicDBObject> mongoCursor = cursor.cursor();

      // Now go ahead and do the bulk upsert.
      BulkWriteResult result = bulkUpsertImpl(documents);
      LOGGER.debug(result.toString());

      return new Iterator<>() {
        @Override
        public boolean hasNext() {
          return mongoCursor.hasNext();
        }

        @Override
        public Document next() {
          return MongoCollection.this.dbObjectToDocument(mongoCursor.next());
        }
      };
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

  private Document dbObjectToDocument(BasicDBObject dbObject) {
    try {
      // Hack: Remove the _id field since it's an unrecognized field for Proto layer.
      // TODO: We should rather use separate DAO classes instead of using the
      //  DB document directly as proto message.
      dbObject.removeField(ID_KEY);
      String jsonString;
      JsonWriterSettings relaxed =
          JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
      jsonString = dbObject.toJson(relaxed);
      JsonNode jsonNode = MAPPER.readTree(jsonString);
      JsonNode decodedJsonNode = recursiveClone(jsonNode, this::decodeKey);
      return new JSONDocument(decodedJsonNode);
    } catch (IOException e) {
      // throwing exception is not very useful here.
      return JSONDocument.errorDocument(e.getMessage());
    }
  }

  private Map<String, Object> parseFilter(Filter filter) {
    Map<String, Object> map = new HashMap<>();

    // If there is a filter in the query, parse it fully.
    if (filter != null) {
      map = MongoQueryParser.parseFilter(filter);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Sending query to mongo: {} : {}",
          collection.getNamespace().getCollectionName(),
          Arrays.toString(map.entrySet().toArray()));
    }

    return map;
  }

  private MongoCursor<BasicDBObject> processCursor(
      Query query, FindIterable<BasicDBObject> cursor) {
    cursor = applyProjections(query, cursor);
    applySorting(query, cursor);
    cursor = applyPagination(query, cursor);

    return cursor.cursor();
  }

  private Iterator<Document> getIteratorFromCursor(MongoCursor<BasicDBObject> cursor) {
    return new Iterator<>() {

      @Override
      public boolean hasNext() {
        return cursor.hasNext();
      }

      @Override
      public Document next() {
        return MongoCollection.this.dbObjectToDocument(cursor.next());
      }
    };
  }

  private FindIterable<BasicDBObject> applyProjections(
      Query query, FindIterable<BasicDBObject> cursor) {
    Bson projection = new BasicDBObject();
    if (!query.getSelections().isEmpty()) {
      projection = Projections.include(query.getSelections());
    }

    return cursor.projection(projection);
  }

  private FindIterable<BasicDBObject> applyPagination(
      Query query, FindIterable<BasicDBObject> cursor) {
    cursor = applySkip(query, cursor);
    cursor = applyLimit(query, cursor);

    return cursor;
  }

  private void applySorting(Query query, FindIterable<BasicDBObject> cursor) {
    if (!query.getOrderBys().isEmpty()) {
      BasicDBObject orderBy =
          new BasicDBObject(MongoQueryParser.parseOrderBys(query.getOrderBys()));
      cursor.sort(orderBy);
    }
  }

  private FindIterable<BasicDBObject> applySkip(Query query, FindIterable<BasicDBObject> cursor) {
    Integer offset = query.getOffset();
    if (offset != null && offset >= 0) {
      cursor = cursor.skip(offset);
    }

    return cursor;
  }

  private FindIterable<BasicDBObject> applyLimit(Query query, FindIterable<BasicDBObject> cursor) {
    Integer limit = query.getLimit();
    if (limit != null && limit >= 0) {
      cursor = cursor.limit(limit);
    }

    return cursor;
  }
}
