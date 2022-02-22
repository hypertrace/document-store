package org.hypertrace.core.documentstore.mongo;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.SUM;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.*;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.*;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.junit.jupiter.api.Test;

public class Test1 {

  //@Test
  public void test() throws IOException {
    String COLLECTION_NAME = "myTest";
    Datastore datastore;
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    DatastoreProvider.register("MONGO", MongoDatastore.class);

    Map<String, String> mongoConfig = new HashMap<>();
    mongoConfig.putIfAbsent("host", "localhost");
    mongoConfig.putIfAbsent("port", "27017");
    Config config = ConfigFactory.parseMap(mongoConfig);

    datastore = DatastoreProvider.getDatastore("Mongo", config);
    System.out.println(datastore.listCollections());

    datastore.createCollection(COLLECTION_NAME, null);
    Collection collection = datastore.getCollection(COLLECTION_NAME);

    // https://docs.mongodb.com/manual/reference/operator/query/elemMatch/
    //        collection.upsert(
    //                new SingleValueKey("default", "testKey1"),
    //                Utils.createDocument(
    //                        ImmutablePair.of("score", 20),
    //                        ImmutablePair.of("field", "x1"),
    //                        ImmutablePair.of(
    //                                "arrayField",
    //
    // List.of(Utils.createDocument(ImmutablePair.of("nestedArrayField", List.of("c1", "c2"))),
    //
    // Utils.createDocument(ImmutablePair.of("nestedArrayField", List.of("c1", "c7"))))
    //                        )
    //                ));
    //        collection.upsert(
    //                new SingleValueKey("default", "testKey2"),
    //                Utils.createDocument(
    //                        ImmutablePair.of("score", 30),
    //                        ImmutablePair.of("field", "x1"),
    //                        ImmutablePair.of(
    //                                "arrayField",
    //
    // List.of(Utils.createDocument(ImmutablePair.of("nestedArrayField", List.of("c4", "c2"))))
    //                        )
    //                ));

    org.hypertrace.core.documentstore.query.Query query =
        org.hypertrace.core.documentstore.query.Query.builder()
            .addSelection(IdentifierExpression.of("score"))
            .addAggregation(IdentifierExpression.of("field"))
            .addSelection(AggregateExpression.of(SUM, IdentifierExpression.of("score")), "total")
            // .addFromClause(UnnestExpression.of(IdentifierExpression.of("arrayField")))
            //
            // .addFromClause(UnnestExpression.of(IdentifierExpression.of("arrayField.nestedArrayField")))
            .build();

    Iterator<Document> iterator = collection.aggregate(query);

    List<Document> documents = new ArrayList<>();
    iterator.forEachRemaining(documents::add);
    int x = 1;
  }

  public static class Utils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Document createDocument(ImmutablePair<String, Object>... pairs) {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      for (int i = 0; i < pairs.length; i++) {
        if (pairs[i].getRight() instanceof Integer) {
          objectNode.put(pairs[i].getLeft(), (Integer) (pairs[i].getRight()));
        } else if (pairs[i].getRight() instanceof Double) {
          objectNode.put(pairs[i].getLeft(), (Double) (pairs[i].getRight()));
        } else if (pairs[i].getRight() instanceof Boolean) {
          objectNode.put(pairs[i].getLeft(), (Boolean) (pairs[i].getRight()));
        } else if (pairs[i].getRight() instanceof String) {
          objectNode.put(pairs[i].getLeft(), (String) (pairs[i].getRight()));
        } else if (pairs[i].getRight() instanceof List) {
          ArrayNode arrayNode = OBJECT_MAPPER.createArrayNode();
          List<Object> list = (List<Object>) pairs[i].getRight();
          if (list.get(0) instanceof Document)
            list.stream().map(v -> ((JSONDocument) v).getNode()).forEach(arrayNode::add);
          else list.stream().forEach(v -> arrayNode.add((String) v));

          objectNode.set(pairs[i].getLeft(), arrayNode);
        } else {
          objectNode.putPOJO(pairs[i].getLeft(), pairs[i].getRight());
        }
      }
      return new JSONDocument(objectNode);
    }

    public static Document createDocument(String key, String value) {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      objectNode.put(key, value);
      return new JSONDocument(objectNode);
    }

    public static Document createDocument(String... keys) {
      ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
      for (int i = 0; i < keys.length - 1; i++) {
        objectNode.put(keys[i], keys[i + 1]);
      }
      return new JSONDocument(objectNode);
    }

    public static List<Map<String, Object>> convertJsonToMap(String json)
        throws JsonProcessingException {
      return OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
    }

    public static Map<String, Object> convertDocumentToMap(Document document)
        throws JsonProcessingException {
      return OBJECT_MAPPER.readValue(document.toJson(), new TypeReference<>() {});
    }
  }
}
