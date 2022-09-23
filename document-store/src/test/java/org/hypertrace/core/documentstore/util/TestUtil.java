package org.hypertrace.core.documentstore.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;

public class TestUtil {
  public static Optional<String> readFileFromResource(final String filePath) throws IOException {
    ClassLoader classLoader = TestUtil.class.getClassLoader();

    try (final InputStream inputStream = classLoader.getResourceAsStream(filePath)) {
      // the stream holding the file content
      if (inputStream == null) {
        throw new IllegalArgumentException("Resource not found: " + filePath);
      }

      return Optional.of(new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  public static BasicDBObject readBasicDBObject(final String filePath) throws IOException {
    return BasicDBObject.parse(readFileFromResource(filePath).orElseThrow());
  }

  public static Document readDocument(final String filePath) throws IOException {
    return new JSONDocument(readFileFromResource(filePath).orElseThrow());
  }

  @SuppressWarnings("unchecked")
  public static void assertJsonEquals(final String expected, final String actual)
      throws JsonProcessingException {
    final Map<String, Object> expectedMap = new ObjectMapper().readValue(expected, HashMap.class);
    final Map<String, Object> actualMap = new ObjectMapper().readValue(actual, HashMap.class);

    assertEquals(expectedMap, actualMap);
  }
}
