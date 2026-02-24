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
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
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
    when(mockSchemaRegistry.getColumnOrRefresh(anyString(), anyString()))
        .thenAnswer(
            invocation -> {
              String columnName = invocation.getArgument(1);
              return Optional.ofNullable(schema.get(columnName));
            });
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
  @DisplayName("update Exception Handling Tests")
  class UpdateExceptionTests {

    @Test
    @DisplayName("Should return UpdateResult(0) when all fields are unknown/skipped (L511-512)")
    void testUpdateReturnsZeroWhenAllFieldsSkipped() throws Exception {
      Key key = Key.from("test-key");
      Document document =
          new JSONDocument("{\"unknown_field1\": \"value\", \"unknown_field2\": 123}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      when(mockSchemaRegistry.getColumnOrRefresh(anyString(), anyString()))
          .thenReturn(Optional.empty());

      UpdateResult result = flatPostgresCollection.update(key, document, null);

      assertEquals(0, result.getUpdatedCount());
      verify(mockClient, never()).getPooledConnection();
    }

    @Test
    @DisplayName("Should retry on UNDEFINED_COLUMN PSQLException and succeed")
    void testUpdateRetriesOnUndefinedColumn() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeUpdate()).thenThrow(psqlException).thenReturn(1);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      UpdateResult result = flatPostgresCollection.update(key, document, null);

      assertEquals(1, result.getUpdatedCount());
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeUpdate();
    }

    @Test
    @DisplayName("Should retry on DATATYPE_MISMATCH PSQLException and succeed")
    void testUpdateRetriesOnDatatypeMismatch() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      PSQLException psqlException = createPSQLException(PSQLState.DATATYPE_MISMATCH);
      when(mockPreparedStatement.executeUpdate()).thenThrow(psqlException).thenReturn(1);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      UpdateResult result = flatPostgresCollection.update(key, document, null);

      assertEquals(1, result.getUpdatedCount());
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeUpdate();
    }

    @Test
    @DisplayName("Should throw IOException on non-retryable PSQLException")
    void testUpdateThrowsOnNonRetryablePSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      PSQLException psqlException = createPSQLException(PSQLState.UNIQUE_VIOLATION);
      when(mockPreparedStatement.executeUpdate()).thenThrow(psqlException);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.update(key, document, null));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException on generic SQLException (L535-537)")
    void testUpdateThrowsOnSQLException() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      SQLException sqlException = new SQLException("Connection lost");
      when(mockPreparedStatement.executeUpdate()).thenThrow(sqlException);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.update(key, document, null));

      assertEquals(sqlException, thrown.getCause());
      verify(mockSchemaRegistry, never()).invalidate(anyString());
    }

    @Test
    @DisplayName("Should throw IOException when retry also fails (L533-534)")
    void testUpdateThrowsWhenRetryFails() throws Exception {
      Key key = Key.from("test-key");
      Document document = new JSONDocument("{\"item\": \"Test\", \"price\": 100}");

      Map<String, PostgresColumnMetadata> schema = createBasicSchema();
      setupCommonMocks(schema);

      PSQLException psqlException = createPSQLException(PSQLState.UNDEFINED_COLUMN);
      when(mockPreparedStatement.executeUpdate()).thenThrow(psqlException);

      doNothing().when(mockSchemaRegistry).invalidate(COLLECTION_NAME);

      IOException thrown =
          assertThrows(IOException.class, () -> flatPostgresCollection.update(key, document, null));

      assertEquals(psqlException, thrown.getCause());
      verify(mockSchemaRegistry, times(1)).invalidate(COLLECTION_NAME);
      verify(mockPreparedStatement, times(2)).executeUpdate();
    }
  }
}
