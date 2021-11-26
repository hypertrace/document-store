package org.hypertrace.core.documentstore.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.core.documentstore.mongo.MongoCollection;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

public class Utils {
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

  public static Map<Key, Document> createDocumentsFromResource(String resourcePath)
      throws IOException {
    Optional<String> contentOptional = readFileFromResource(resourcePath);
    String json = contentOptional.orElseThrow();
    List<Map<String, Object>> maps = OBJECT_MAPPER.readValue(json, new TypeReference<>() {});

    Map<Key, Document> documentMap = new HashMap<>();
    for (Map<String, Object> map : maps) {
      Key key = new SingleValueKey("default", map.get(MongoCollection.ID_KEY).toString());
      Document value = new JSONDocument(map);
      documentMap.put(key, value);
    }

    return documentMap;
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
}
