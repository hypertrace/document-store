package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresLazyilyLoadedSchemaRegistryTest {

  private static final String TEST_TABLE = "test_table";
  private static final String COL_ID = "id";
  private static final String COL_NAME = "name";
  private static final String COL_PRICE = "price";
  private static final Duration CACHE_EXPIRY = Duration.ofHours(24);
  private static final Duration REFRESH_COOLDOWN = Duration.ofMillis(50);

  @Mock private PostgresMetadataFetcher fetcher;

  private PostgresLazyilyLoadedSchemaRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new PostgresLazyilyLoadedSchemaRegistry(fetcher, CACHE_EXPIRY, REFRESH_COOLDOWN);
  }

  @Test
  void getSchemaLoadsFromFetcherOnCacheMiss() {
    Map<String, PostgresColumnMetadata> expectedSchema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(expectedSchema);

    Map<String, PostgresColumnMetadata> result = registry.getSchema(TEST_TABLE);

    assertEquals(expectedSchema, result);
    verify(fetcher, times(1)).fetch(TEST_TABLE);
  }

  @Test
  void getSchemaReturnsCachedValueOnSubsequentCalls() {
    Map<String, PostgresColumnMetadata> expectedSchema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(expectedSchema);

    // First call - loads from fetcher
    Map<String, PostgresColumnMetadata> result1 = registry.getSchema(TEST_TABLE);
    // Second call - should use cache
    Map<String, PostgresColumnMetadata> result2 = registry.getSchema(TEST_TABLE);

    assertEquals(expectedSchema, result1);
    assertEquals(expectedSchema, result2);
    // Fetcher should only be called once
    verify(fetcher, times(1)).fetch(TEST_TABLE);
  }

  @Test
  void getSchemaLoadsEachTableIndependently() {
    String table1 = "table1";
    String table2 = "table2";
    Map<String, PostgresColumnMetadata> schema1 = createTestSchema();
    Map<String, PostgresColumnMetadata> schema2 = new HashMap<>();
    schema2.put(
        "other_col",
        PostgresColumnMetadata.builder()
            .colName("other_col")
            .canonicalType(DataType.BOOLEAN)
            .postgresType(PostgresDataType.BOOLEAN)
            .nullable(false)
            .build());

    when(fetcher.fetch(table1)).thenReturn(schema1);
    when(fetcher.fetch(table2)).thenReturn(schema2);

    Map<String, PostgresColumnMetadata> result1 = registry.getSchema(table1);
    Map<String, PostgresColumnMetadata> result2 = registry.getSchema(table2);

    assertEquals(schema1, result1);
    assertEquals(schema2, result2);
    verify(fetcher, times(1)).fetch(table1);
    verify(fetcher, times(1)).fetch(table2);
  }

  @Test
  void invalidateClearsSpecificTableCache() {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    // Load into cache
    registry.getSchema(TEST_TABLE);
    verify(fetcher, times(1)).fetch(TEST_TABLE);

    // Invalidate the cache
    registry.invalidate(TEST_TABLE);

    // Next call should reload from fetcher
    registry.getSchema(TEST_TABLE);
    verify(fetcher, times(2)).fetch(TEST_TABLE);
  }

  @Test
  void invalidateDoesNotAffectOtherTables() {
    String table1 = "table1";
    String table2 = "table2";
    Map<String, PostgresColumnMetadata> schema1 = createTestSchema();
    Map<String, PostgresColumnMetadata> schema2 = new HashMap<>();

    when(fetcher.fetch(table1)).thenReturn(schema1);
    when(fetcher.fetch(table2)).thenReturn(schema2);

    // Load both tables into cache
    registry.getSchema(table1);
    registry.getSchema(table2);

    // Invalidate only table1
    registry.invalidate(table1);

    // table1 should reload, table2 should use cache
    registry.getSchema(table1);
    registry.getSchema(table2);

    verify(fetcher, times(2)).fetch(table1);
    verify(fetcher, times(1)).fetch(table2);
  }

  @Test
  void getColumnOrRefreshReturnsColumnIfExists() {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    Optional<PostgresColumnMetadata> result = registry.getColumnOrRefresh(TEST_TABLE, COL_ID);

    assertTrue(result.isPresent());
    assertEquals(COL_ID, result.get().getName());
    assertEquals(DataType.INTEGER, result.get().getCanonicalType());
    // Should only call fetcher once (initial load)
    verify(fetcher, times(1)).fetch(TEST_TABLE);
  }

  @Test
  void getColumnOrRefreshRefreshesSchemaIfColumnMissingAndCooldownExpired() throws Exception {
    // Initial schema without the "new_col"
    Map<String, PostgresColumnMetadata> initialSchema = createTestSchema();

    // Updated schema with the "new_col"
    Map<String, PostgresColumnMetadata> updatedSchema = new HashMap<>(initialSchema);
    updatedSchema.put(
        "new_col",
        PostgresColumnMetadata.builder()
            .colName("new_col")
            .canonicalType(DataType.STRING)
            .postgresType(PostgresDataType.TEXT)
            .nullable(true)
            .build());

    when(fetcher.fetch(TEST_TABLE)).thenReturn(initialSchema).thenReturn(updatedSchema);

    // First call loads the schema
    registry.getSchema(TEST_TABLE);
    verify(fetcher, times(1)).fetch(TEST_TABLE);

    // Wait past cooldown period
    Thread.sleep(REFRESH_COOLDOWN.toMillis() + 10);

    // Now try to get missing column - should trigger refresh
    Optional<PostgresColumnMetadata> result = registry.getColumnOrRefresh(TEST_TABLE, "new_col");

    assertTrue(result.isPresent());
    assertEquals("new_col", result.get().getName());
    // Should call fetcher twice (initial load + refresh after cooldown)
    verify(fetcher, times(2)).fetch(TEST_TABLE);
  }

  @Test
  void getColumnOrRefreshDoesNotRefreshIfWithinCooldownPeriod() {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    // First call loads the schema
    registry.getSchema(TEST_TABLE);
    verify(fetcher, times(1)).fetch(TEST_TABLE);

    // Try to get a missing column - should NOT refresh because we're within cooldown
    Optional<PostgresColumnMetadata> result =
        registry.getColumnOrRefresh(TEST_TABLE, "nonexistent_col");

    assertFalse(result.isPresent());
    // Should only call fetcher once (initial load, no refresh due to cooldown)
    verify(fetcher, times(1)).fetch(TEST_TABLE);
  }

  @Test
  void getColumnOrRefreshRefreshesAfterCooldownExpires() throws Exception {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    // First call loads the schema
    registry.getSchema(TEST_TABLE);
    verify(fetcher, times(1)).fetch(TEST_TABLE);

    // Try immediately - should NOT refresh (within cooldown)
    registry.getColumnOrRefresh(TEST_TABLE, "nonexistent_col");
    verify(fetcher, times(1)).fetch(TEST_TABLE);

    // Wait past cooldown
    Thread.sleep(REFRESH_COOLDOWN.toMillis() + 10);

    // Try again - should refresh now
    registry.getColumnOrRefresh(TEST_TABLE, "nonexistent_col");
    verify(fetcher, times(2)).fetch(TEST_TABLE);
  }

  @Test
  void getColumnOrRefreshReturnsEmptyIfColumnStillMissingAfterRefresh() throws Exception {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    // First call loads the schema
    registry.getSchema(TEST_TABLE);

    // Wait past cooldown
    Thread.sleep(REFRESH_COOLDOWN.toMillis() + 10);

    // Try to get a column that doesn't exist even after refresh
    Optional<PostgresColumnMetadata> result =
        registry.getColumnOrRefresh(TEST_TABLE, "nonexistent_col");

    assertFalse(result.isPresent());
    // Should call fetcher twice (initial load + refresh attempt after cooldown)
    verify(fetcher, times(2)).fetch(TEST_TABLE);
  }

  @Test
  void getColumnOrRefreshUsesExistingCacheBeforeRefresh() {
    Map<String, PostgresColumnMetadata> schema = createTestSchema();
    when(fetcher.fetch(TEST_TABLE)).thenReturn(schema);

    // Pre-populate cache
    registry.getSchema(TEST_TABLE);

    // Get existing column - should not trigger refresh
    Optional<PostgresColumnMetadata> result = registry.getColumnOrRefresh(TEST_TABLE, COL_NAME);

    assertTrue(result.isPresent());
    assertEquals(COL_NAME, result.get().getName());
    // Should only call fetcher once (initial getSchema)
    verify(fetcher, times(1)).fetch(TEST_TABLE);
  }

  @Test
  void getSchemaThrowsRuntimeExceptionWhenFetcherFails() {
    when(fetcher.fetch(TEST_TABLE)).thenThrow(new RuntimeException("Database error"));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> registry.getSchema(TEST_TABLE));

    assertEquals("Database error", exception.getCause().getMessage());
  }

  private Map<String, PostgresColumnMetadata> createTestSchema() {
    Map<String, PostgresColumnMetadata> schema = new HashMap<>();
    schema.put(
        COL_ID,
        PostgresColumnMetadata.builder()
            .colName(COL_ID)
            .canonicalType(DataType.INTEGER)
            .postgresType(PostgresDataType.INTEGER)
            .nullable(false)
            .build());
    schema.put(
        COL_NAME,
        PostgresColumnMetadata.builder()
            .colName(COL_NAME)
            .canonicalType(DataType.STRING)
            .postgresType(PostgresDataType.TEXT)
            .nullable(true)
            .build());
    schema.put(
        COL_PRICE,
        PostgresColumnMetadata.builder()
            .colName(COL_PRICE)
            .canonicalType(DataType.DOUBLE)
            .postgresType(PostgresDataType.DOUBLE_PRECISION)
            .nullable(true)
            .build());
    return schema;
  }
}
