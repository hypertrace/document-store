package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
      when(mockResultSet.getBoolean("is_insert")).thenReturn(true);

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
      when(mockResultSet.getBoolean("is_insert")).thenReturn(false);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      boolean result = flatPostgresCollection.upsert(key, document);

      assertFalse(result);
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
  }
}
