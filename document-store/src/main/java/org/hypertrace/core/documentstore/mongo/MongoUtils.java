package org.hypertrace.core.documentstore.mongo;

import static com.mongodb.client.model.ReturnDocument.AFTER;
import static com.mongodb.client.model.ReturnDocument.BEFORE;
import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.mongo.MongoCollection.ID_KEY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.ReturnDocument;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;

public final class MongoUtils {
  public static final String FIELD_SEPARATOR = ".";
  public static final String PREFIX = "$";
  private static final String UNSUPPORTED_OPERATION = "No MongoDB support available for: '%s'";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Map<ReturnDocumentType, ReturnDocument> RETURN_DOCUMENT_MAP =
      Map.ofEntries(entry(BEFORE_UPDATE, BEFORE), entry(AFTER_UPDATE, AFTER));

  public static <T> UnsupportedOperationException getUnsupportedOperationException(T t) {
    return new UnsupportedOperationException(String.format(UNSUPPORTED_OPERATION, t));
  }

  public static String encodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\", "\\\\").replace(PREFIX, "\\u0024").replace(FIELD_SEPARATOR, "\\u002e");
  }

  public static String decodeKey(final String key) {
    if (key == null) {
      return null;
    }

    return key.replace("\\u002e", FIELD_SEPARATOR).replace("\\u0024", PREFIX).replace("\\\\", "\\");
  }

  public static String sanitizeJsonString(final String jsonString) throws JsonProcessingException {
    final JsonNode jsonNode = MAPPER.readTree(jsonString);
    // escape "." and "$" in field names since Mongo DB does not like them
    final JsonNode sanitizedJsonNode = recursiveClone(jsonNode, MongoUtils::encodeKey);
    return MAPPER.writeValueAsString(sanitizedJsonNode);
  }

  public static JsonNode recursiveClone(JsonNode src, Function<String, String> function) {
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
      JsonNode decodedJsonNode = recursiveClone(jsonNode, MongoUtils::decodeKey);
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
}
