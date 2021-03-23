package org.hypertrace.core.documentstore.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.OrderBy;
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
   * Same as existing upsert method, however, extends the support with condition and optional
   * parameter for explicitly controlling insert and update.
   * */
  @Override
  public boolean upsert(Key key, Document document, Filter condition, @Nullable Boolean isUpsert)
      throws IOException {
    try {
      Map<String, Object> map = parseQuery(condition);
      map.put(ID_KEY, key.toString());
      BasicDBObject conditionObject = new BasicDBObject(map);
      UpdateOptions options = new UpdateOptions().upsert(isUpsert != null ? isUpsert : true);
      UpdateResult writeResult =
          collection.updateOne(conditionObject, this.prepareUpsert(key, document), options);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + writeResult.toString());
      }
      return (writeResult.getUpsertedId() != null || writeResult.getModifiedCount() > 0);
    } catch (Exception e) {
      LOGGER.error("Exception upserting document. key: {} content:{}", key, document, e);
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
    String jsonString = document.toJson();
    JsonNode jsonNode = MAPPER.readTree(jsonString);

    // escape "." and "$" in field names since Mongo DB does not like them
    JsonNode sanitizedJsonNode = recursiveClone(jsonNode, this::encodeKey);
    BasicDBObject setObject = BasicDBObject.parse(MAPPER.writeValueAsString(sanitizedJsonNode));
    long now = System.currentTimeMillis();
    setObject.put(ID_KEY, key.toString());
    setObject.put(LAST_UPDATED_TIME, now);
    return new BasicDBObject("$set", setObject)
        .append("$currentDate", new BasicDBObject(LAST_UPDATE_TIME, true))
        .append("$setOnInsert", new BasicDBObject(CREATED_TIME, now));
  }

  /** Updates auto-field lastUpdatedTime when sub doc is updated */
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    String jsonString = subDocument.toJson();
    try {
      JsonNode jsonNode = MAPPER.readTree(jsonString);

      // escape "." and "$" in field names since Mongo DB does not like them
      JsonNode sanitizedJsonNode = recursiveClone(jsonNode, this::encodeKey);
      BasicDBObject dbObject =
          new BasicDBObject(
              subDocPath, BasicDBObject.parse(MAPPER.writeValueAsString(sanitizedJsonNode)));
      dbObject.append(LAST_UPDATED_TIME, System.currentTimeMillis());
      BasicDBObject setObject = new BasicDBObject("$set", dbObject);

      UpdateResult writeResult =
          collection.updateOne(selectionCriteriaForKey(key), setObject, new UpdateOptions());
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + writeResult.toString());
      }
      // TODO:look into the writeResult to ensure it was successful. Was not easy to find this from
      // docs.
      return true;
    } catch (IOException e) {
      LOGGER.error("Exception inserting document. key: {} content:{}", key, subDocument);
      return false;
    }
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
    Map<String, Object> map = new HashMap<>();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      map = parseQuery(query.getFilter());
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
      Map<String, Object> orderbyMap = new HashMap<>();
      parseOrderByQuery(query.getOrderBys(), orderbyMap);
      BasicDBObject orderBy = new BasicDBObject(orderbyMap);
      cursor.sort(orderBy);
    }

    final MongoCursor<BasicDBObject> mongoCursor = cursor.cursor();
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

  private void parseOrderByQuery(List<OrderBy> orderBys, Map<String, Object> orderbyMap) {
    for (OrderBy orderBy : orderBys) {
      orderbyMap.put(orderBy.getField(), (orderBy.isAsc() ? 1 : -1));
    }
  }

  @VisibleForTesting
  Map<String, Object> parseQuery(Filter filter) {
    if (filter.isComposite()) {
      Filter.Op op = filter.getOp();
      switch (op) {
        case OR:
        case AND:
          {
            List<Map<String, Object>> childMapList =
                Arrays.stream(filter.getChildFilters())
                    .map(this::parseQuery)
                    .filter(map -> !map.isEmpty())
                    .collect(Collectors.toList());
            if (!childMapList.isEmpty()) {
              return Map.of("$" + op.name().toLowerCase(), childMapList);
            } else {
              return Collections.emptyMap();
            }
          }
        default:
          throw new UnsupportedOperationException(
              String.format("Boolean operation:%s not supported", op));
      }
    } else {
      Filter.Op op = filter.getOp();
      Object value = filter.getValue();
      Map<String, Object> map = new HashMap<>();
      switch (op) {
        case EQ:
          map.put(filter.getFieldName(), value);
          break;
        case LIKE:
          // Case insensitive regex search
          map.put(
              filter.getFieldName(), new BasicDBObject("$regex", value).append("$options", "i"));
          break;
        case NOT_IN:
          map.put(filter.getFieldName(), new BasicDBObject("$nin", value));
          break;
        case IN:
          map.put(filter.getFieldName(), new BasicDBObject("$in", value));
          break;
        case CONTAINS:
          map.put(filter.getFieldName(), new BasicDBObject("$elemMatch", filter.getValue()));
          break;
        case GT:
          map.put(filter.getFieldName(), new BasicDBObject("$gt", value));
          break;
        case LT:
          map.put(filter.getFieldName(), new BasicDBObject("$lt", value));
          break;
        case GTE:
          map.put(filter.getFieldName(), new BasicDBObject("$gte", value));
          break;
        case LTE:
          map.put(filter.getFieldName(), new BasicDBObject("$lte", value));
          break;
        case EXISTS:
          map.put(filter.getFieldName(), new BasicDBObject("$exists", true));
          break;
        case NOT_EXISTS:
          map.put(filter.getFieldName(), new BasicDBObject("$exists", false));
          break;
        case NEQ:
          // $ne operator in Mongo also returns the results, where the key does not exist in the
          // document. This is as per semantics of EQ vs NEQ. So, if you need documents where
          // key exists, consumer needs to add additional filter.
          // https://github.com/hypertrace/document-store/pull/20#discussion_r547101520
          map.put(filter.getFieldName(), new BasicDBObject("$ne", value));
          break;
        case AND:
        case OR:
        default:
          throw new UnsupportedOperationException(UNSUPPORTED_QUERY_OPERATION);
      }
      return map;
    }
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
      map = parseQuery(query.getFilter());
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
}
