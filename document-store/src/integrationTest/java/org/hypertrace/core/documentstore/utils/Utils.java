package org.hypertrace.core.documentstore.utils;

import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.commons.DocStoreConstants;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

public class Utils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String MONGO_STORE = "Mongo";
  public static final String POSTGRES_STORE = "Postgres";

  /*
   * These 3 fields should be automatically created when upserting a doc.
   * There are downstream services that depends on this. The test should verify that
   * the string is not changed.
   */
  public static final String MONGO_LAST_UPDATE_TIME_KEY = "_lastUpdateTime";
  public static final String MONGO_LAST_UPDATED_TIME_KEY = "lastUpdatedTime";
  public static final String MONGO_CREATED_TIME_KEY = "createdTime";

  /** Postgres related time fields */
  public static final String POSTGRES_UPDATED_AT = "updated_at";

  public static final String POSTGRES_CREATED_AT = "created_at";
  public static final String TENANT_ID = "default";

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

  public static Optional<String> readFileFromResource(String filePath) throws IOException {
    ClassLoader classLoader = Utils.class.getClassLoader();

    try (InputStream inputStream = classLoader.getResourceAsStream(filePath)) {
      // the stream holding the file content
      if (inputStream == null) {
        throw new IllegalArgumentException("Resource not found: " + filePath);
      }

      return Optional.ofNullable(IOUtils.toString(inputStream, StandardCharsets.UTF_8));
    }
  }

  public static List<Map<String, Object>> convertJsonToMap(String json)
      throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, new TypeReference<>() {});
  }

  public static Map<String, Object> convertDocumentToMap(Document document)
      throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(document.toJson(), new TypeReference<>() {});
  }

  public static void assertDocsAndSizeEqual(
      String dataStoreName, Iterator<Document> documents, String filePath, int expectedSize)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    List<Map<String, Object>> expected = convertJsonToMap(fileContent);

    int actualSize = 0;
    List<Map<String, Object>> actual = new ArrayList<>();
    while (documents.hasNext()) {
      final Map<String, Object> doc = convertDocumentToMap(documents.next());
      removeDateRelatedFields(dataStoreName, doc);
      actual.add(doc);
      actualSize++;
    }

    assertEquals(expectedSize, actualSize);
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(ORDER_MAP_ENTRIES_BY_KEYS, true);
    final JsonNode expectedJson = objectMapper.readTree(objectMapper.writeValueAsString(expected));
    final JsonNode actualJson = objectMapper.readTree(objectMapper.writeValueAsString(actual));

    assertEquals(expectedJson, actualJson);
  }

  public static Map<Key, Document> buildDocumentsFromResource(String resourcePath)
      throws IOException {
    Optional<String> contentOptional = readFileFromResource(resourcePath);
    String json = contentOptional.orElseThrow();
    List<Map<String, Object>> maps = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});

    Map<Key, Document> documentMap = new HashMap<>();
    for (Map<String, Object> map : maps) {
      Key key = new SingleValueKey(TENANT_ID, map.get(MongoCollection.ID_KEY).toString());
      Document value = new JSONDocument(map);
      documentMap.put(key, value);
    }

    return documentMap;
  }

  public static void assertDocsAndSizeEqualWithoutOrder(
      String dataStoreName, Iterator<Document> documents, String filePath, int expectedSize)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    List<Map<String, Object>> expectedDocs = convertJsonToMap(fileContent);

    List<Map<String, Object>> actualDocs = new ArrayList<>();
    int actualSize = 0;
    while (documents.hasNext()) {
      Map<String, Object> doc = convertDocumentToMap(documents.next());
      removeDateRelatedFields(dataStoreName, doc);
      actualDocs.add(doc);
      actualSize++;
    }

    long count = expectedDocs.stream().filter(actualDocs::contains).count();

    assertEquals(expectedSize, actualSize);

    final ObjectMapper objectMapper = new ObjectMapper();
    assertEquals(
        expectedSize,
        count,
        String.format(
            "Expected: %s\nActual: %s",
            objectMapper.writeValueAsString(expectedDocs),
            objectMapper.writeValueAsString(actualDocs)));
  }

  public static void assertSizeEqual(Iterator<Document> documents, String filePath)
      throws IOException {
    String fileContent = readFileFromResource(filePath).orElseThrow();
    int expected = convertJsonToMap(fileContent).size();
    int actual;

    for (actual = 0; documents.hasNext(); actual++) {
      documents.next();
    }

    assertEquals(expected, actual);
  }

  private static void removeDateRelatedFields(String dataStoreName, Map<String, Object> document) {
    if (isMongo(dataStoreName)) {
      document.remove(MONGO_CREATED_TIME_KEY);
      document.remove(MONGO_LAST_UPDATED_TIME_KEY);
      document.remove(MONGO_LAST_UPDATE_TIME_KEY);
    } else if (isPostgress(dataStoreName)) {
      document.remove(POSTGRES_CREATED_AT);
      document.remove(POSTGRES_UPDATED_AT);
      document.remove(DocStoreConstants.CREATED_TIME);
      document.remove(DocStoreConstants.LAST_UPDATED_TIME);
    }
  }

  private static boolean isMongo(String dataStoreName) {
    return MONGO_STORE.equals(dataStoreName);
  }

  private static boolean isPostgress(String dataStoreName) {
    return POSTGRES_STORE.equals(dataStoreName);
  }
}
