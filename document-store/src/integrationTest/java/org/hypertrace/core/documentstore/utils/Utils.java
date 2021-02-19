package org.hypertrace.core.documentstore.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;

public class Utils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static Document createDocument(ImmutablePair<String, Object>...paris) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    for (int i = 0; i < paris.length; i++) {
      if (paris[i].getRight() instanceof Integer) {
        objectNode.put(paris[i].getLeft(), (Integer)(paris[i].getRight()));
      } else if (paris[i].getRight() instanceof Double) {
        objectNode.put(paris[i].getLeft(), (Double)(paris[i].getRight()));
      } else if (paris[i].getRight() instanceof Boolean) {
        objectNode.put(paris[i].getLeft(), (Boolean) (paris[i].getRight()));
      } else if (paris[i].getRight() instanceof String) {
        objectNode.put(paris[i].getLeft(), (String)(paris[i].getRight()));
      } else {
        objectNode.putPOJO(paris[i].getLeft(), paris[i].getRight());
      }
    }
    return new JSONDocument(objectNode);
  }

  public static Document createDocument(String key, String value) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    objectNode.put(key, value);
    return new JSONDocument(objectNode);
  }

  public static Document createDocument(String ...keys) {
    ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
    for (int i = 0; i < keys.length - 1; i++) {
      objectNode.put(keys[i], keys[i + 1]);
    }
    return new JSONDocument(objectNode);
  }
}
