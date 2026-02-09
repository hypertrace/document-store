package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FlatPostgresCollectionTest {

  private FlatPostgresCollection collection;

  @BeforeEach
  void setUp() {
    PostgresClient mockClient = mock(PostgresClient.class);
    when(mockClient.getCustomParameters()).thenReturn(Collections.emptyMap());

    PostgresLazyilyLoadedSchemaRegistry mockSchemaRegistry =
        mock(PostgresLazyilyLoadedSchemaRegistry.class);

    collection = new FlatPostgresCollection(mockClient, "test_table", mockSchemaRegistry);
  }

  @Nested
  @DisplayName("convertTimestampForType Tests")
  class ConvertTimestampForTypeTests {

    private static final long TEST_EPOCH_MILLIS = 1707465494818L; // 2024-02-09T06:58:14.818Z

    @Test
    @DisplayName("BIGINT should return epoch millis as-is")
    void testBigintReturnsEpochMillis() {
      Object result =
          collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.BIGINT);

      assertInstanceOf(Long.class, result);
      assertEquals(TEST_EPOCH_MILLIS, result);
    }

    @Test
    @DisplayName("INTEGER should return epoch seconds (millis / 1000)")
    void testIntegerReturnsEpochSeconds() {
      Object result =
          collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.INTEGER);

      assertInstanceOf(Integer.class, result);
      assertEquals((int) (TEST_EPOCH_MILLIS / 1000), result);
    }

    @Test
    @DisplayName("TIMESTAMPTZ should return java.sql.Timestamp")
    void testTimestamptzReturnsSqlTimestamp() {
      Object result =
          collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.TIMESTAMPTZ);

      assertInstanceOf(Timestamp.class, result);
      Timestamp timestamp = (Timestamp) result;
      assertEquals(TEST_EPOCH_MILLIS, timestamp.getTime());
    }

    @Test
    @DisplayName("DATE should return java.sql.Date")
    void testDateReturnsSqlDate() {
      Object result = collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.DATE);

      assertInstanceOf(Date.class, result);
      Date date = (Date) result;
      assertEquals(TEST_EPOCH_MILLIS, date.getTime());
    }

    @Test
    @DisplayName("TEXT should return ISO-8601 formatted string")
    void testTextReturnsIso8601String() {
      Object result = collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.TEXT);

      assertInstanceOf(String.class, result);
      String isoString = (String) result;
      assertEquals(Instant.ofEpochMilli(TEST_EPOCH_MILLIS).toString(), isoString);
      assertTrue(isoString.contains("2024-02-09"));
    }

    @Test
    @DisplayName("Unsupported type should return epoch millis as string (fallback)")
    void testUnsupportedTypeReturnsStringFallback() {
      // REAL is not a typical timestamp type, should fall through to default
      Object result = collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.REAL);

      assertInstanceOf(String.class, result);
      assertEquals(String.valueOf(TEST_EPOCH_MILLIS), result);
    }

    @Test
    @DisplayName("JSONB type should return epoch millis as string (fallback)")
    void testJsonbTypeReturnsStringFallback() {
      Object result = collection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.JSONB);

      assertInstanceOf(String.class, result);
      assertEquals(String.valueOf(TEST_EPOCH_MILLIS), result);
    }
  }
}
