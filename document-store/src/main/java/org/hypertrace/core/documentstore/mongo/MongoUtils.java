package org.hypertrace.core.documentstore.mongo;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static com.mongodb.client.model.ReturnDocument.BEFORE;
import static java.util.Map.entry;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.ReturnDocument;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;

public final class MongoUtils {
  public static final String FIELD_SEPARATOR = ".";
  public static final String PREFIX = "$";
  private static final String UNICODE_FOR_FIELD_PREFIX = "\\u0024";
  private static final String LITERAL = PREFIX + "literal";
  private static final String UNSUPPORTED_OPERATION = "No MongoDB support available for: '%s'";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<ReturnDocumentType, ReturnDocument> RETURN_DOCUMENT_MAP =
      Map.ofEntries(
          entry(BEFORE_UPDATE, BEFORE),
          entry(AFTER_UPDATE, AFTER),
          // Since Mongo doesn't have NONE return type, we map it to AFTER
          entry(NONE, AFTER));

  public static <T> UnsupportedOperationException getUnsupportedOperationException(T t) {
    return new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION, t));
  }

  public static String encodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\", "\\\\")
        .replace(PREFIX, UNICODE_FOR_FIELD_PREFIX)
        .replace(FIELD_SEPARATOR, "\\u002e");
  }

  public static String decodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\u002e", FIELD_SEPARATOR)
        .replace(UNICODE_FOR_FIELD_PREFIX, PREFIX)
        .replace("\\\\", "\\");
  }

  public static String getLastField(final String fieldPath) {
    final String[] fields = fieldPath.split("\\" + FIELD_SEPARATOR);
    return fields[fields.length - 1];
  }

  public static String sanitizeJsonString(final String jsonString) throws JsonProcessingException {
    final JsonNode jsonNode = MAPPER.readTree(jsonString);
    // escape "." and "$" in field names since Mongo DB does not like them
    final JsonNode sanitizedJsonNode = recursiveClone(jsonNode, MongoUtils::encodeKey, identity());
    return MAPPER.writeValueAsString(sanitizedJsonNode);
  }

  public static String sanitizeJsonStringWrappingEmptyObjectsInLiteral(final String jsonString)
      throws JsonProcessingException {
    final JsonNode jsonNode = MAPPER.readTree(jsonString);
    // escape "." and "$" in field names since Mongo DB does not like them
    final JsonNode sanitizedJsonNode =
        recursiveClone(jsonNode, MongoUtils::encodeKey, MongoUtils::wrapInLiteral);
    return MAPPER.writeValueAsString(sanitizedJsonNode);
  }

  public static BasicDBObject merge(final List<BasicDBObject> basicDbObjects) {
    return basicDbObjects.stream()
        .map(Map::entrySet)
        .flatMap(Set::stream)
        .collect(
            collectingAndThen(
                toUnmodifiableMap(Entry::getKey, Entry::getValue), BasicDBObject::new));
  }

  public static JsonNode recursiveClone(
      JsonNode src,
      UnaryOperator<String> function,
      final UnaryOperator<ObjectNode> emptyObjectConverter) {
    if (src.isTextual()) {
      return new TextNode(function.apply(src.asText()));
    }

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
        newValue = recursiveClone(value, function, emptyObjectConverter);
      }
      if (value.isArray()) {
        newValue = new ArrayNode(JsonNodeFactory.instance);
        for (int i = 0; i < value.size(); i++) {
          final JsonNode elementValue =
              recursiveClone(value.get(i), function, emptyObjectConverter);
          ((ArrayNode) newValue).add(elementValue);
        }
      }
      if (newValue.isTextual()) {
        newValue = new TextNode(function.apply(newValue.asText()));
      }
      tgt.set(newFieldName, newValue);
    }
    return tgt.isEmpty() ? emptyObjectConverter.apply(tgt) : tgt;
  }

  public static Document dbObjectToDocument(BasicDBObject dbObject) {
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
      JsonNode decodedJsonNode = recursiveClone(jsonNode, MongoUtils::decodeKey, identity());
      return new JSONDocument(decodedJsonNode);
    } catch (IOException e) {
      // throwing exception is not very useful here.
      return JSONDocument.errorDocument(e.getMessage());
    }
  }

  public static ReturnDocument getReturnDocument(final ReturnDocumentType returnDocumentType) {
    return Optional.ofNullable(RETURN_DOCUMENT_MAP.get(returnDocumentType))
        .orElseThrow(
            () ->
                new UnsupportedOperationException(
                    String.format("Unhandled return document type: %s", returnDocumentType)));
  }

  private static ObjectNode wrapInLiteral(final ObjectNode objectNode) {
    /* Wrapping the subDocument with $literal to be able to provide empty object "{}" as value
     *  Throws error otherwise if empty object is provided as value.
     *  https://jira.mongodb.org/browse/SERVER-54046 */
    final ObjectNode node = JsonNodeFactory.instance.objectNode();
    node.set(LITERAL, objectNode);
    return node;
  }
}
