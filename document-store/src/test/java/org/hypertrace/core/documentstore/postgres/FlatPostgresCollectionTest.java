package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.DeepBulkUpdateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

@ExtendWith(MockitoExtension.class)
class FlatPostgresCollectionTest {

  private static final String COLLECTION_NAME = "test_flat_collection";

  @Mock private PostgresClient mockClient;
  @Mock private PostgresLazyilyLoadedSchemaRegistry mockSchemaRegistry;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;
  @Mock private ResultSet mockResultSet;

  private FlatPostgresCollection flatPostgresCollection;

  @BeforeEach
  void setUp() {
    when(mockClient.getCustomParameters()).thenReturn(Collections.emptyMap());
    flatPostgresCollection =
        new FlatPostgresCollection(mockClient, COLLECTION_NAME, mockSchemaRegistry);
  }

  private Map<String, PostgresColumnMetadata> createBasicSchema() {
    Map<String, PostgresColumnMetadata> schema = new LinkedHashMap<>();
    schema.put(
        "id",
        PostgresColumnMetadata.builder()
            .colName("id")
            .canonicalType(DataType.STRING)
            .postgresType(PostgresDataType.TEXT)
            .nullable(false)
            .isArray(false)
            .isPrimaryKey(true)
            .build());
    schema.put(
        "item",
        PostgresColumnMetadata.builder()
            .colName("item")
            .canonicalType(DataType.STRING)
            .postgresType(PostgresDataType.TEXT)
            .nullable(true)
            .isArray(false)
            .isPrimaryKey(false)
            .build());
    schema.put(
        "price",
        PostgresColumnMetadata.builder()
            .colName("price")
            .canonicalType(DataType.LONG)
            .postgresType(PostgresDataType.INTEGER)
            .nullable(true)
            .isArray(false)
            .isPrimaryKey(false)
            .build());
    return schema;
  }

  private PSQLException createPSQLException(PSQLState state) {
    return new PSQLException("Test error", state);
  }

  private void setupCommonMocks(Map<String, PostgresColumnMetadata> schema) throws SQLException {
    // Mock getColumnOrRefresh for each field in the document
    when(mockSchemaRegistry.getColumnOrRefresh(anyString(), anyString()))
        .thenAnswer(
            invocation -> {
              String columnName = invocation.getArgument(1);
              return Optional.ofNullable(schema.get(columnName));
            });
    // Mock getPrimaryKeyColumn
    when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
    when(mockClient.getPooledConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
  }

  private void setupCreateOrReplaceMocks(Map<String, PostgresColumnMetadata> schema)
      throws SQLException {
    setupCommonMocks(schema);
    when(mockSchemaRegistry.getSchema(COLLECTION_NAME)).thenReturn(schema);
  }

  @Nested
  @DisplayName("createOrReplace Exception Handling Tests")
  class CreateOrReplaceExceptionTests {

    @Test
    @DisplayName("Should retry on UNDEFINED_COLUMN PSQLException and succeed")
    void testCreateOrReplaceRetriesOnUndefinedColumn() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      // First call throws UNDEFINED_COLUMN, second call succeeds
      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);
      when(mockResultSet.getBoolean("is_insert")).thenReturn(true);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      boolean result = flatPostgresCollection.createOrReplace(key, document);

      assertTrue(result);
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }

    @Test
    @DisplayName("Should retry on DATATYPE_MISMATCH PSQLException and succeed")
    void testCreateOrReplaceRetriesOnDatatypeMismatch() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      // First call throws DATATYPE_MISMATCH, second call succeeds
      PSQLException psqlException = createPSQLException(PSQLState.DATATYPE_MISMATCH);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);
      when(mockResultSet.getBoolean("is_insert")).thenReturn(false);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      boolean result = flatPostgresCollection.createOrReplace(key, document);

      assertFalse(result);
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }

    @Test
    @DisplayName("Should throw IOException on non-retryable PSQLException")
    void testCreateOrReplaceThrowsOnNonRetryablePSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      // Throw a non-retryable PSQLException (e.g., UNIQUE_VIOLATION)
      PSQLException psqlException = createPSQLException(PSQLState.UNIQUE_VIOLATION);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException);

      IOException thrown =
          assertThrows(
              IOException.class, () -> flatPostgresCollection.createOrReplace(key, document));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException on generic SQLException")
    void testCreateOrReplaceThrowsOnSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      // Throw a generic SQLException (not PSQLException)
      SQLException sqlException = new SQLException("Connection lost");
      when(mockPreparedStatement.executeQuery()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(
              IOException.class, () -> flatPostgresCollection.createOrReplace(key, document));

      assertEquals(sqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException when retry also fails")
    void testCreateOrReplaceThrowsWhenRetryFails() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      // Both calls throw UNDEFINED_COLUMN - first triggers retry, second should throw
      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      IOException thrown =
          assertThrows(
              IOException.class, () -> flatPostgresCollection.createOrReplace(key, document));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }
  }

  @Nested
  @DisplayName("upsert Exception Handling Tests")
  class UpsertExceptionTests {

    @Test
    @DisplayName("Should retry on UNDEFINED_COLUMN PSQLException and succeed")
    void testUpsertRetriesOnUndefinedColumn() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // First call throws UNDEFINED_COLUMN, second call succeeds
      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      boolean result = flatPostgresCollection.upsert(key, document);

      assertTrue(result);
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }

    @Test
    @DisplayName("Should retry on DATATYPE_MISMATCH PSQLException and succeed")
    void testUpsertRetriesOnDatatypeMismatch() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // First call throws DATATYPE_MISMATCH, second call succeeds
      PSQLException psqlException = createPSQLException(PSQLState.DATATYPE_MISMATCH);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException).thenReturn(mockResultSet);
      when(mockResultSet.next()).thenReturn(true);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      boolean result = flatPostgresCollection.upsert(key, document);

      // upsert always returns true if it succeeds
      assertTrue(result);
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }

    @Test
    @DisplayName("Should throw IOException on non-retryable PSQLException")
    void testUpsertThrowsOnNonRetryablePSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // Throw a non-retryable PSQLException
      PSQLException psqlException = createPSQLException(PSQLState.UNIQUE_VIOLATION);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.upsert(key, document));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException on generic SQLException")
    void testUpsertThrowsOnSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // Throw a generic SQLException (not PSQLException)
      SQLException sqlException = new SQLException("Connection lost");
      when(mockPreparedStatement.executeQuery()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.upsert(key, document));

      assertEquals(sqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException when retry also fails")
    void testUpsertThrowsWhenRetryFails() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // Both calls throw UNDEFINED_COLUMN - first triggers retry, second should throw
      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeQuery()).thenThrow(psqlException);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.upsert(key, document));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeQuery();
    }
  }

  @Nested
  @DisplayName("bulkUpdate Tests")
  class BulkUpdateTests {

    @Test
    @DisplayName("Should throw IllegalArgumentException for null options")
    void testBulkUpdateThrowsOnNullOptions() {
      Query query = Query.builder().build();
      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));

      assertThrows(
          IllegalArgumentException.class,
          () -> flatPostgresCollection.bulkUpdate(query, updates, null));
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for empty updates")
    void testBulkUpdateThrowsOnEmptyUpdates() {
      Query query = Query.builder().build();
      List<SubDocumentUpdate> emptyUpdates = Collections.emptyList();
      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      assertThrows(
          IllegalArgumentException.class,
          () -> flatPostgresCollection.bulkUpdate(query, emptyUpdates, options));
    }

    @Test
    @DisplayName("Should throw IOException for unsupported operator")
    void testBulkUpdateThrowsOnUnsupportedOperator() {
      Query query = Query.builder().build();
      // UNSET is not supported
      List<SubDocumentUpdate> updates =
          List.of(
              SubDocumentUpdate.builder()
                  .subDocument("price")
                  .operator(org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET)
                  .build());
      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      // No stubbing needed - operator check happens before schema lookup
      assertThrows(
          IOException.class, () -> flatPostgresCollection.bulkUpdate(query, updates, options));
    }

    @Test
    @DisplayName("Should throw IOException on SQLException and log SQLState and ErrorCode")
    void testBulkUpdateThrowsOnSQLException() throws Exception {
      Query query = Query.builder().build();
      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));
      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.AFTER_UPDATE).build();

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      when(mockSchemaRegistry.getColumnOrRefresh(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String columnName = invocation.getArgument(1);
                return Optional.ofNullable(schema.get(columnName));
              });

      SQLException sqlException = new SQLException("Connection failed", "08001", 1001);
      when(mockClient.getPooledConnection()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(
              IOException.class, () -> flatPostgresCollection.bulkUpdate(query, updates, options));

      assertEquals(sqlException, thrown.getCause());
    }

    @Test
    @DisplayName("Should close beforeIterator on exception when BEFORE_UPDATE")
    @SuppressWarnings("unchecked")
    void testBulkUpdateClosesIteratorOnException() throws Exception {
      // Create spy to mock find() while testing rest of bulkUpdate
      FlatPostgresCollection spyCollection = spy(flatPostgresCollection);

      CloseableIterator<Document> mockIterator = mock(CloseableIterator.class);
      doReturn(mockIterator).when(spyCollection).find(any(Query.class));

      Query query = Query.builder().build();
      List<SubDocumentUpdate> updates = List.of(SubDocumentUpdate.of("price", 100));
      UpdateOptions options =
          UpdateOptions.builder().returnDocumentType(ReturnDocumentType.BEFORE_UPDATE).build();

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      when(mockSchemaRegistry.getColumnOrRefresh(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String columnName = invocation.getArgument(1);
                return Optional.ofNullable(schema.get(columnName));
              });

      RuntimeException runtimeException = new RuntimeException("Connection pool exhausted");
      when(mockClient.getPooledConnection()).thenThrow(runtimeException);

      assertThrows(IOException.class, () -> spyCollection.bulkUpdate(query, updates, options));

      verify(mockIterator).close();
    }

    @Test
    @DisplayName("Map bulkUpdate: empty updates map -> zero updated, no failures")
    void testMapBulkUpdateEmptyMap() throws Exception {
      DeepBulkUpdateResult result =
          flatPostgresCollection.bulkUpdate(
              Collections.emptyMap(), UpdateOptions.DEFAULT_UPDATE_OPTIONS);

      assertEquals(0, result.getUpdatedCount());
      assertFalse(result.hasFailures());
      assertTrue(result.getFailedKeys().isEmpty());
    }

    @Test
    @DisplayName("Map bulkUpdate: null updates map -> zero updated, no failures")
    void testMapBulkUpdateNullMap() throws Exception {
      DeepBulkUpdateResult result =
          flatPostgresCollection.bulkUpdate(null, UpdateOptions.DEFAULT_UPDATE_OPTIONS);

      assertEquals(0, result.getUpdatedCount());
      assertFalse(result.hasFailures());
    }

    @Test
    @DisplayName("Map bulkUpdate: null UpdateOptions throws IllegalArgumentException")
    void testMapBulkUpdateThrowsOnNullOptions() {
      Map<Key, Collection<SubDocumentUpdate>> updates = new LinkedHashMap<>();
      updates.put(Key.from("k1"), List.of(SubDocumentUpdate.of("price", 100)));

      assertThrows(
          IllegalArgumentException.class, () -> flatPostgresCollection.bulkUpdate(updates, null));
    }

    @Test
    @DisplayName("Map bulkUpdate: happy path - all keys updated, no failures")
    void testMapBulkUpdateAllSuccess() throws Exception {
      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // Every SET on "price" lands in one shape group; two keys => two batch entries.
      when(mockPreparedStatement.executeBatch()).thenReturn(new int[] {1, 1});

      Map<Key, Collection<SubDocumentUpdate>> updates = new LinkedHashMap<>();
      updates.put(Key.from("k1"), List.of(SubDocumentUpdate.of("price", 100)));
      updates.put(Key.from("k2"), List.of(SubDocumentUpdate.of("price", 200)));

      DeepBulkUpdateResult result =
          flatPostgresCollection.bulkUpdate(updates, UpdateOptions.DEFAULT_UPDATE_OPTIONS);

      assertEquals(2, result.getUpdatedCount());
      assertFalse(result.hasFailures());
      assertTrue(result.getFailedKeys().isEmpty());
      verify(mockPreparedStatement, times(1)).executeBatch();
    }

    @Test
    @DisplayName(
        "Map bulkUpdate: one group's executeBatch throws deadlock - only its keys marked failed")
    void testMapBulkUpdatePartialGroupFailure() throws Exception {
      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      // Two distinct SET columns => two shape groups. Group ordering follows input insertion
      // order via LinkedHashMap; both groupings happen before either executeBatch call.
      // First executeBatch (group for "price") succeeds; second (group for "item") aborts with
      // Postgres deadlock_detected (40P01) via BatchUpdateException.
      BatchUpdateException deadlock =
          new BatchUpdateException(
              "Batch entry ... was aborted: ERROR: deadlock detected",
              "40P01",
              0,
              new int[] {Statement.EXECUTE_FAILED},
              new SQLException("deadlock detected", "40P01"));
      when(mockPreparedStatement.executeBatch())
          .thenReturn(new int[] {1, 1}) // group A: SET "price" for k1, k2
          .thenThrow(deadlock); // group B: SET "item" for k3, k4

      Map<Key, Collection<SubDocumentUpdate>> updates = new LinkedHashMap<>();
      Key k1 = Key.from("k1");
      Key k2 = Key.from("k2");
      Key k3 = Key.from("k3");
      Key k4 = Key.from("k4");
      updates.put(k1, List.of(SubDocumentUpdate.of("price", 100)));
      updates.put(k2, List.of(SubDocumentUpdate.of("price", 200)));
      updates.put(k3, List.of(SubDocumentUpdate.of("item", "Alpha")));
      updates.put(k4, List.of(SubDocumentUpdate.of("item", "Bravo")));

      DeepBulkUpdateResult result =
          flatPostgresCollection.bulkUpdate(updates, UpdateOptions.DEFAULT_UPDATE_OPTIONS);

      assertEquals(2, result.getUpdatedCount(), "only price group's rows should count as updated");
      assertTrue(result.hasFailures());
      assertEquals(
          Set.of(k3, k4),
          result.getFailedKeys(),
          "failedKeys must contain exactly the keys from the aborted batch group");
      verify(mockPreparedStatement, times(2)).executeBatch();
    }

    @Test
    @DisplayName("Map bulkUpdate: connection acquisition failure -> IOException")
    void testMapBulkUpdateConnectionAcquisitionFails() throws Exception {
      // Only stubs actually consumed before the connection acquisition are required.
      when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
      SQLException sqlException = new SQLException("pool exhausted", "08001");
      when(mockClient.getPooledConnection()).thenThrow(sqlException);

      Map<Key, Collection<SubDocumentUpdate>> updates = new LinkedHashMap<>();
      updates.put(Key.from("k1"), List.of(SubDocumentUpdate.of("price", 100)));

      IOException thrown =
          assertThrows(
              IOException.class,
              () ->
                  flatPostgresCollection.bulkUpdate(updates, UpdateOptions.DEFAULT_UPDATE_OPTIONS));
      assertEquals(sqlException, thrown.getCause());
    }
  }

  @Nested
  @DisplayName("delete(Key) Exception Handling Tests")
  class DeleteKeyExceptionTests {

    @Test
    @DisplayName("Should return false when prepareStatement throws SQLException")
    void testDeleteReturnsFalseWhenPrepareStatementFails() throws Exception {
      Key key = Key.from("test-key");

      when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
      when(mockClient.getConnection()).thenReturn(mockConnection);
      when(mockConnection.prepareStatement(anyString()))
          .thenThrow(new SQLException("Statement preparation failed"));

      boolean result = flatPostgresCollection.delete(key);

      assertFalse(result);
    }

    @Test
    @DisplayName("Should return false when executeUpdate throws SQLException")
    void testDeleteReturnsFalseWhenExecuteUpdateFails() throws Exception {
      Key key = Key.from("test-key");

      when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
      when(mockClient.getConnection()).thenReturn(mockConnection);
      when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
      when(mockPreparedStatement.executeUpdate()).thenThrow(new SQLException("Execute failed"));

      boolean result = flatPostgresCollection.delete(key);

      assertFalse(result);
    }
  }

  @Nested
  @DisplayName("bulkCreateOrReplace Exception Handling Tests")
  class BulkCreateOrReplaceExceptionTests {

    @Test
    @DisplayName("Should return false on BatchUpdateException")
    void testBulkCreateOrReplaceReturnsFalseOnBatchUpdateException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      java.sql.BatchUpdateException batchException =
          new java.sql.BatchUpdateException("Batch failed", new int[] {});
      when(mockPreparedStatement.executeBatch()).thenThrow(batchException);

      boolean result = flatPostgresCollection.bulkCreateOrReplace(documents);

      assertFalse(result);
    }

    @Test
    @DisplayName("Should return false on SQLException")
    void testBulkCreateOrReplaceReturnsFalseOnSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      SQLException sqlException = new SQLException("Connection failed", "08001", 1001);
      when(mockPreparedStatement.executeBatch()).thenThrow(sqlException);

      boolean result = flatPostgresCollection.bulkCreateOrReplace(documents);

      assertFalse(result);
    }

    @Test
    @DisplayName("Should return false on generic Exception")
    void testBulkCreateOrReplaceReturnsFalseOnGenericException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCreateOrReplaceMocks(schema);

      RuntimeException runtimeException = new RuntimeException("Unexpected error");
      when(mockPreparedStatement.executeBatch()).thenThrow(runtimeException);

      boolean result = flatPostgresCollection.bulkCreateOrReplace(documents);

      assertFalse(result);
    }
  }

  @Nested
  @DisplayName("bulkCreateOrReplaceReturnOlderDocuments Exception Handling Tests")
  class BulkCreateOrReplaceReturnOlderDocumentsExceptionTests {

    @Mock private java.sql.Array mockSqlArray;

    private void setupBulkCreateOrReplaceReturnOlderDocsMocks(
        Map<String, PostgresColumnMetadata> schema) throws SQLException {
      when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
      when(mockClient.getPooledConnection()).thenReturn(mockConnection);
      when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
      when(mockConnection.createArrayOf(anyString(), any())).thenReturn(mockSqlArray);
    }

    @Test
    @DisplayName("Should throw IOException on SQLException from getPooledConnection")
    void testBulkCreateOrReplaceReturnOlderDocumentsThrowsOnSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      when(mockSchemaRegistry.getPrimaryKeyColumn(COLLECTION_NAME)).thenReturn(Optional.of("id"));
      SQLException sqlException = new SQLException("Connection failed", "08001", 1001);
      when(mockClient.getPooledConnection()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(
              IOException.class,
              () -> flatPostgresCollection.bulkCreateOrReplaceReturnOlderDocuments(documents));

      assertTrue(thrown.getMessage().contains("Could not bulk createOrReplace"));
      assertEquals(sqlException, thrown.getCause());
    }

    @Test
    @DisplayName("Should throw IOException on SQLException from executeQuery")
    void testBulkCreateOrReplaceReturnOlderDocumentsThrowsOnExecuteQuerySQLException()
        throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupBulkCreateOrReplaceReturnOlderDocsMocks(schema);

      SQLException sqlException = new SQLException("Query failed", "42000", 1002);
      when(mockPreparedStatement.executeQuery()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(
              IOException.class,
              () -> flatPostgresCollection.bulkCreateOrReplaceReturnOlderDocuments(documents));

      assertTrue(thrown.getMessage().contains("Could not bulk createOrReplace"));
      assertEquals(sqlException, thrown.getCause());
      verify(mockConnection).close();
    }

    @Test
    @DisplayName("Should throw IOException on generic Exception")
    void testBulkCreateOrReplaceReturnOlderDocumentsThrowsOnGenericException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");
      Map<Key, Document> documents = Map.of(key, document);

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupBulkCreateOrReplaceReturnOlderDocsMocks(schema);

      RuntimeException runtimeException = new RuntimeException("Unexpected error");
      when(mockPreparedStatement.executeQuery()).thenThrow(runtimeException);

      IOException thrown =
          assertThrows(
              IOException.class,
              () -> flatPostgresCollection.bulkCreateOrReplaceReturnOlderDocuments(documents));

      assertTrue(thrown.getMessage().contains("Could not bulk createOrReplace"));
      assertEquals(runtimeException, thrown.getCause());
      verify(mockConnection).close();
    }
  }

  @Nested
  @DisplayName("convertTimestampForType Tests")
  class TimestampCoversionTests {

    private static final long TEST_EPOCH_MILLIS = 1707465494818L; // 2024-02-09T06:58:14.818Z

    @Test
    @DisplayName("BIGINT should return epoch millis as-is")
    void testBigintReturnsEpochMillis() {
      Object result =
          flatPostgresCollection.convertTimestampForType(
              TEST_EPOCH_MILLIS, PostgresDataType.BIGINT);

      assertInstanceOf(Long.class, result);
      assertEquals(TEST_EPOCH_MILLIS, result);
    }

    @Test
    @DisplayName("INTEGER should return epoch seconds (millis / 1000)")
    void testIntegerReturnsEpochSeconds() {
      Object result =
          flatPostgresCollection.convertTimestampForType(
              TEST_EPOCH_MILLIS, PostgresDataType.INTEGER);

      assertInstanceOf(Integer.class, result);
      assertEquals((int) (TEST_EPOCH_MILLIS / 1000), result);
    }

    @Test
    @DisplayName("TIMESTAMPTZ should return java.sql.Timestamp")
    void testTimestamptzReturnsSqlTimestamp() {
      Object result =
          flatPostgresCollection.convertTimestampForType(
              TEST_EPOCH_MILLIS, PostgresDataType.TIMESTAMPTZ);

      assertInstanceOf(Timestamp.class, result);
      Timestamp timestamp = (Timestamp) result;
      assertEquals(TEST_EPOCH_MILLIS, timestamp.getTime());
    }

    @Test
    @DisplayName("DATE should return java.sql.Date")
    void testDateReturnsSqlDate() {
      Object result =
          flatPostgresCollection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.DATE);

      assertInstanceOf(Date.class, result);
      Date date = (Date) result;
      assertEquals(TEST_EPOCH_MILLIS, date.getTime());
    }

    @Test
    @DisplayName("TEXT should return ISO-8601 formatted string")
    void testTextReturnsIso8601String() {
      Object result =
          flatPostgresCollection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.TEXT);

      assertInstanceOf(String.class, result);
      String isoString = (String) result;
      assertEquals(Instant.ofEpochMilli(TEST_EPOCH_MILLIS).toString(), isoString);
      assertTrue(isoString.contains("2024-02-09"));
    }

    @Test
    @DisplayName("Unsupported type should return epoch millis as string (fallback)")
    void testUnsupportedTypeReturnsStringFallback() {
      // REAL is not a typical timestamp type, should fall through to default
      Object result =
          flatPostgresCollection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.REAL);

      assertInstanceOf(String.class, result);
      assertEquals(String.valueOf(TEST_EPOCH_MILLIS), result);
    }

    @Test
    @DisplayName("JSONB type should return epoch millis as string (fallback)")
    void testJsonbTypeReturnsStringFallback() {
      Object result =
          flatPostgresCollection.convertTimestampForType(TEST_EPOCH_MILLIS, PostgresDataType.JSONB);

      assertInstanceOf(String.class, result);
      assertEquals(String.valueOf(TEST_EPOCH_MILLIS), result);
    }
  }
}
