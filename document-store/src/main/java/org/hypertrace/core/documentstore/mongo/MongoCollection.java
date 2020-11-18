package org.hypertrace.core.documentstore.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoServerException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
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
  public static final String ID_KEY = "_id";
  private static final String LAST_UPDATE_TIME = "_lastUpdateTime";
  /* follow json/protobuf convention to make it deser, let's not make our life harder */
  private static final String CREATED_TIME = "createdTime";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final com.mongodb.client.MongoCollection<BasicDBObject> collection;

  MongoCollection(com.mongodb.client.MongoCollection<BasicDBObject> collection) {
    this.collection = collection;
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try {
      UpdateOptions options = new UpdateOptions().upsert(true);
      UpdateResult writeResult =
          collection.updateOne(this.selectionCriteriaForKey(key), this.prepareUpsert(key, document), options);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: " + writeResult.toString());
      }

      return writeResult.getModifiedCount() > 0;
    } catch (IOException e) {
      LOGGER.error("Exception upserting document. key: {} content:{}", key, document, e);
      throw e;
    }
  }

  @Override
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    BasicDBObject upsertResult = collection.findOneAndUpdate(
        this.selectionCriteriaForKey(key),
        this.prepareUpsert(key, document),
        new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER));
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
    setObject.put(ID_KEY, key.toString());
    return new BasicDBObject("$set", setObject)
        .append("$currentDate", new BasicDBObject(LAST_UPDATE_TIME, true))
        .append("$setOnInsert", new BasicDBObject(CREATED_TIME, System.currentTimeMillis()));
  }

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
      BasicDBObject setObject = new BasicDBObject("$set", dbObject);

      UpdateResult writeResult = collection.updateOne(selectionCriteriaForKey(key), setObject,
          new UpdateOptions());
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Sending query to mongo: {} : {}",
          collection.getNamespace().getCollectionName(),
          Arrays.toString(map.entrySet().toArray()));
    }

    // Assume its SimpleAndQuery for now
    BasicDBObject ref = new BasicDBObject(map);
    FindIterable<BasicDBObject> cursor = collection.find(ref);

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

    UpdateResult updateResult = collection.updateOne(this.selectionCriteriaForKey(key), unsetObject);
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
        case AND:
        case NEQ:
        case OR:
          throw new UnsupportedOperationException("Only Equality predicate is supported");
        default:
          break;
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

  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    try {
      List<UpdateOneModel<BasicDBObject>> bulkCollection = new ArrayList<>();
      for (Entry<Key, Document> entry : documents.entrySet()) {
        Key key = entry.getKey();
        Document document = entry.getValue();

        String jsonString = document.toJson();
        JsonNode jsonNode = MAPPER.readTree(jsonString);

        // escape "." and "$" in field names since Mongo DB does not like them
        JsonNode sanitizedJsonNode = recursiveClone(jsonNode, this::encodeKey);
        BasicDBObject dbObject = BasicDBObject.parse(MAPPER.writeValueAsString(sanitizedJsonNode));

        dbObject.put(ID_KEY, key.toString());
        BasicDBObject doc = new BasicDBObject("$set", dbObject)
            .append("$currentDate", new BasicDBObject("_lastUpdateTime", true))
            .append("$setOnInsert", new BasicDBObject(CREATED_TIME, System.currentTimeMillis()));

        // insert or overwrite
        bulkCollection.add(new UpdateOneModel<>(this.selectionCriteriaForKey(key), doc, new UpdateOptions().upsert(true)));
      }

      BulkWriteResult result = collection.bulkWrite(bulkCollection, new BulkWriteOptions().ordered(false));
      LOGGER.debug(result.toString());

      return true;

    } catch (IOException | MongoServerException e) {
      LOGGER.error("Error during bulk upsert for documents:{}", documents, e);
      return false;
    }
  }

  @Override
  public void drop() {
    collection.drop();
  }

  private BasicDBObject selectionCriteriaForKey(Key key) {
    return new BasicDBObject(ID_KEY, key.toString());
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
