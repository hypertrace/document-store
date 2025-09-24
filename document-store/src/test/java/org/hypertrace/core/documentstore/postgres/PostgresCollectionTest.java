package org.hypertrace.core.documentstore.postgres;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.emptyList;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.LT;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;
import static org.hypertrace.core.documentstore.expression.operators.SortOrder.DESC;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.util.TestUtil.readDocument;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresCollectionTest {
  private static final String COLLECTION_NAME = "test_collection";
  private static final long currentTime = 1658956123L;

  @Mock private PostgresClient mockClient;
  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockSelectPreparedStatement;
  @Mock private PreparedStatement mockUpdatePreparedStatement;
  @Mock private ResultSet mockResultSet;
  @Mock private ResultSetMetaData mockResultSetMetaData;
  @Mock private Clock mockClock;

  private PostgresCollection postgresCollection;

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @BeforeEach
  void setUp() {
    try (final MockedStatic<Clock> clockMock = Mockito.mockStatic(Clock.class)) {
      clockMock.when(Clock::systemUTC).thenReturn(mockClock);
      postgresCollection = new NestedPostgresCollection(mockClient, COLLECTION_NAME);
    }
  }

  @Test
  void testCreateOrReplace() throws Exception {
    final Key key = Key.from("some_key");
    final Document document = new JSONDocument("{\"planet\": \"Mars\"}");

    when(mockClient.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(
            String.format(
                "INSERT INTO \"%s\" (id, document, created_at) "
                    + "VALUES (?, ?::jsonb, NOW()) "
                    + "ON CONFLICT(id) DO UPDATE SET "
                    + "document = jsonb_set(?::jsonb, '{createdTime}', \"%s\".document->'createdTime'), "
                    + "updated_at = NOW() "
                    + "RETURNING created_at = NOW() AS created_now_alias",
                COLLECTION_NAME, COLLECTION_NAME)))
        .thenReturn(mockUpdatePreparedStatement);
    when(mockUpdatePreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getBoolean("created_now_alias")).thenReturn(true);
    final boolean result = postgresCollection.createOrReplace(key, document);

    assertTrue(result);

    final ArgumentCaptor<String> documentJsonCaptor = ArgumentCaptor.forClass(String.class);

    verify(mockUpdatePreparedStatement, times(1)).setString(1, key.toString());
    verify(mockUpdatePreparedStatement, times(1)).setString(eq(2), documentJsonCaptor.capture());
    verify(mockUpdatePreparedStatement, times(1)).setString(eq(3), documentJsonCaptor.capture());

    assertEquals(2, documentJsonCaptor.getAllValues().size());
    assertEquals(1, documentJsonCaptor.getAllValues().stream().distinct().count());
  }

  @Test
  void testUpdateAtomicWithFilter() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String id = UUID.randomUUID().toString();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\", "
                + "id AS _implicit_id "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST "
                + "LIMIT 1 "
                + "FOR UPDATE",
            COLLECTION_NAME);
    when(mockClient.getPooledConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    mockResultSetMetadata(id);

    final Document document = readDocument("atomic_read_and_update/response.json");

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document FROM \"%s\" AS t0 WHERE id = ?) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);
    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);

    when(mockClock.millis()).thenReturn(currentTime);

    final Optional<Document> oldDocument =
        postgresCollection.update(
            query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

    assertTrue(oldDocument.isPresent());
    assertEquals(document, oldDocument.get());
    assertEquals(DocumentType.NESTED, document.getDocumentType());

    verify(mockClient, times(1)).getPooledConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, id);
    // Ensure the transaction is committed
    verify(mockConnection, times(1)).commit();

    // Ensure the resources are closed
    verify(mockResultSet, times(1)).close();
    verify(mockSelectPreparedStatement, times(1)).close();
    verify(mockUpdatePreparedStatement, times(1)).close();
    verify(mockConnection, times(1)).close();
  }

  @Test
  void testUpdateAtomicWithFilter_getNone() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String id = UUID.randomUUID().toString();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\", "
                + "id AS _implicit_id "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST "
                + "LIMIT 1 "
                + "FOR UPDATE",
            COLLECTION_NAME);

    when(mockClient.getPooledConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    mockResultSetMetadata(id);

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document FROM \"%s\" AS t0 WHERE id = ?) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);
    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);

    when(mockClock.millis()).thenReturn(currentTime);

    final Optional<Document> oldDocument =
        postgresCollection.update(
            query, updates, UpdateOptions.builder().returnDocumentType(NONE).build());

    assertFalse(oldDocument.isPresent());

    verify(mockClient, times(1)).getPooledConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, id);
    // Ensure the transaction is committed
    verify(mockConnection, times(1)).commit();

    // Ensure the resources are closed
    verify(mockResultSet, times(1)).close();
    verify(mockSelectPreparedStatement, times(1)).close();
    verify(mockUpdatePreparedStatement, times(1)).close();
    verify(mockConnection, times(1)).close();
  }

  @Test
  void testUpdateAtomicWithFilter_emptyResults() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\", "
                + "id AS _implicit_id "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST "
                + "LIMIT 1 "
                + "FOR UPDATE",
            COLLECTION_NAME);
    when(mockClient.getPooledConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);
    when(mockResultSet.isClosed()).thenReturn(false, true);

    final Optional<Document> oldDocument =
        postgresCollection.update(
            query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

    assertTrue(oldDocument.isEmpty());

    verify(mockClient, times(1)).getPooledConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    // Ensure the transaction is committed
    verify(mockConnection, times(1)).commit();

    // Ensure the resources are closed
    verify(mockResultSet, times(1)).close();
    verify(mockSelectPreparedStatement, times(1)).close();
    verify(mockConnection, times(1)).close();
  }

  @Test
  void testUpdateAtomicWithFilter_throwsException() throws Exception {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String id = UUID.randomUUID().toString();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\", "
                + "id AS _implicit_id "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST "
                + "LIMIT 1 "
                + "FOR UPDATE",
            COLLECTION_NAME);
    when(mockClient.getPooledConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    mockResultSetMetadata(id);

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM (SELECT id, document FROM \"%s\" AS t0 WHERE id = ?)"
                + " AS t1) AS t2) AS t3) AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);
    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);

    when(mockClock.millis()).thenReturn(currentTime);

    when(mockUpdatePreparedStatement.executeUpdate()).thenThrow(SQLException.class);

    assertThrows(
        IOException.class,
        () ->
            postgresCollection.update(
                query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build()));

    verify(mockClient, times(1)).getPooledConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, id);

    // Ensure the transaction is rolled back
    verify(mockConnection, times(1)).rollback();

    // Ensure the resources are closed
    verify(mockResultSet, times(1)).close();
    verify(mockSelectPreparedStatement, times(1)).close();
    verify(mockUpdatePreparedStatement, times(1)).close();
    verify(mockConnection, times(1)).close();
  }

  @Test
  void testAtomicUpdateWithoutUpdates() {
    assertThrows(
        IOException.class,
        () ->
            postgresCollection.update(
                org.hypertrace.core.documentstore.query.Query.builder().build(),
                emptyList(),
                UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build()));
  }

  @Test
  void testUpdateBulkWithFilter() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\" "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST",
            COLLECTION_NAME);

    when(mockClient.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(true).thenReturn(false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSet.isClosed()).thenReturn(false, true);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(4);
    mockResultSetMetadata();

    final Document document = readDocument("atomic_read_and_update/response.json");

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document "
                + "FROM \"%s\" AS t0 "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?)) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);

    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);
    when(mockClock.millis()).thenReturn(currentTime);

    final CloseableIterator<Document> oldDocument =
        postgresCollection.bulkUpdate(
            query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

    assertTrue(oldDocument.hasNext());
    assertEquals(document, oldDocument.next());
    assertFalse(oldDocument.hasNext());

    // Obtain 2 connections: One for update and one for selecting
    verify(mockClient, times(2)).getConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, "Soap");
    verify(mockUpdatePreparedStatement).setObject(10, "2022-08-09T18:53:17Z");

    // Ensure the resources are closed
    oldDocument.close();
    verify(mockResultSet, times(1)).close();
    verify(mockUpdatePreparedStatement, times(1)).close();
  }

  @Test
  void testUpdateBulkWithFilter_getNone() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    when(mockClient.getConnection()).thenReturn(mockConnection);

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document "
                + "FROM \"%s\" AS t0 "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?)) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);

    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);
    when(mockClock.millis()).thenReturn(currentTime);

    final CloseableIterator<Document> oldDocument =
        postgresCollection.bulkUpdate(
            query, updates, UpdateOptions.builder().returnDocumentType(NONE).build());

    assertFalse(oldDocument.hasNext());

    verify(mockClient, times(1)).getConnection();
    verifyNoMoreInteractions(mockSelectPreparedStatement);

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, "Soap");
    verify(mockUpdatePreparedStatement).setObject(10, "2022-08-09T18:53:17Z");

    // Ensure the resources are closed
    verify(mockUpdatePreparedStatement, times(1)).close();
  }

  @Test
  void testUpdateBulkWithFilter_emptyResults() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\" "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST",
            COLLECTION_NAME);

    when(mockClient.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenReturn(mockResultSet);
    when(mockResultSet.next()).thenReturn(false);
    when(mockResultSet.isClosed()).thenReturn(false, true);

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document "
                + "FROM \"%s\" AS t0 "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?)) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);

    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);
    when(mockClock.millis()).thenReturn(currentTime);

    final CloseableIterator<Document> oldDocument =
        postgresCollection.bulkUpdate(
            query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build());

    assertFalse(oldDocument.hasNext());

    // Obtain 2 connections: One for update and one for selecting
    verify(mockClient, times(2)).getConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, "Soap");
    verify(mockUpdatePreparedStatement).setObject(10, "2022-08-09T18:53:17Z");

    // Ensure the resources are closed
    oldDocument.close();
    verify(mockResultSet, times(1)).close();
    verify(mockUpdatePreparedStatement, times(1)).close();
  }

  @Test
  void testUpdateBulkWithFilter_throwsExceptionBeforeUpdate() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\" "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST",
            COLLECTION_NAME);

    when(mockClient.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenThrow(SQLException.class);

    assertThrows(
        IOException.class,
        () ->
            postgresCollection.bulkUpdate(
                query, updates, UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build()));

    verify(mockClient, times(1)).getConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verifyNoMoreInteractions(mockUpdatePreparedStatement);
  }

  @Test
  void testUpdateBulkWithFilter_throwsExceptionAfterUpdate() throws IOException, SQLException {
    final Query query = buildQueryWithFilterSortAndProjection();
    final List<SubDocumentUpdate> updates = buildUpdates();

    final String selectQuery =
        String.format(
            "SELECT "
                + "document->'quantity' AS \"quantity\", "
                + "document->'price' AS \"price\", "
                + "document->'date' AS \"date\", "
                + "document->'props' AS \"props\" "
                + "FROM \"%s\" "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?) "
                + "ORDER BY "
                + "document->'price' ASC NULLS FIRST,"
                + "document->'date' DESC NULLS LAST",
            COLLECTION_NAME);

    when(mockClient.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareStatement(selectQuery)).thenReturn(mockSelectPreparedStatement);
    when(mockSelectPreparedStatement.executeQuery()).thenThrow(SQLException.class);

    final String updateQuery =
        String.format(
            "WITH concatenated AS "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t4.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t3.document, '{}'), ?::text[], ?::jsonb) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t2.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT "
                + "id, "
                + "jsonb_set(COALESCE(t1.document, '{}'), ?::text[], to_jsonb(?)) AS document "
                + "FROM "
                + "(SELECT id, document "
                + "FROM \"%s\" AS t0 "
                + "WHERE (document->>'item' = ?) "
                + "AND (document->>'date' < ?)) "
                + "AS t1) "
                + "AS t2) "
                + "AS t3) "
                + "AS t4) "
                + "UPDATE \"%s\" "
                + "SET document=concatenated.document "
                + "FROM concatenated "
                + "WHERE \"%s\".id=concatenated.id",
            COLLECTION_NAME, COLLECTION_NAME, COLLECTION_NAME);

    when(mockConnection.prepareStatement(updateQuery)).thenReturn(mockUpdatePreparedStatement);
    when(mockClock.millis()).thenReturn(currentTime);

    assertThrows(
        IOException.class,
        () ->
            postgresCollection.bulkUpdate(
                query, updates, UpdateOptions.builder().returnDocumentType(AFTER_UPDATE).build()));

    // Obtain 2 connections: One for update and one for selecting
    verify(mockClient, times(2)).getConnection();
    verify(mockConnection, times(1)).prepareStatement(selectQuery);
    verify(mockSelectPreparedStatement, times(1)).setObject(1, "Soap");
    verify(mockSelectPreparedStatement, times(1)).setObject(2, "2022-08-09T18:53:17Z");
    verify(mockSelectPreparedStatement, times(1)).executeQuery();

    verify(mockConnection, times(1)).prepareStatement(updateQuery);

    verify(mockUpdatePreparedStatement).setObject(1, "{lastUpdatedTime}");
    verify(mockUpdatePreparedStatement).setObject(2, currentTime);
    verify(mockUpdatePreparedStatement).setObject(3, "{props}");
    verify(mockUpdatePreparedStatement).setObject(4, "{\"brand\":\"Dettol\"}");
    verify(mockUpdatePreparedStatement).setObject(5, "{quantity}");
    verify(mockUpdatePreparedStatement).setObject(6, 1000);
    verify(mockUpdatePreparedStatement).setObject(7, "{date}");
    verify(mockUpdatePreparedStatement).setObject(8, "2022-08-09T18:53:17Z");
    verify(mockUpdatePreparedStatement).setObject(9, "Soap");
    verify(mockUpdatePreparedStatement).setObject(10, "2022-08-09T18:53:17Z");

    // Ensure the resources are closed
    verify(mockUpdatePreparedStatement, times(1)).close();
  }

  @Test
  void testBulkUpdateWithoutUpdates() {
    assertThrows(
        IOException.class,
        () ->
            postgresCollection.bulkUpdate(
                org.hypertrace.core.documentstore.query.Query.builder().build(),
                emptyList(),
                UpdateOptions.builder().returnDocumentType(BEFORE_UPDATE).build()));
  }

  @Test
  void testNonCompositeFilterUnsupportedException() throws SQLException {
    when(mockClient.getConnection()).thenReturn(mockConnection);
    final PreparedStatement preparedStatement = mock(PreparedStatement.class);
    when(mockConnection.prepareStatement(any())).thenReturn(preparedStatement);

    final Filter filterEq = new Filter(Filter.Op.EQ, "key1", List.of("a", "b"));
    org.hypertrace.core.documentstore.Query eqFilterQuery =
        new org.hypertrace.core.documentstore.Query();
    eqFilterQuery.setFilter(filterEq);

    assertThrows(
        UnsupportedOperationException.class, () -> postgresCollection.search(eqFilterQuery));

    final Filter filterNotEq = new Filter(Filter.Op.NEQ, "key1", List.of("a", "b"));
    org.hypertrace.core.documentstore.Query notEqFilterQuery =
        new org.hypertrace.core.documentstore.Query();
    notEqFilterQuery.setFilter(filterNotEq);

    assertThrows(
        UnsupportedOperationException.class, () -> postgresCollection.search(notEqFilterQuery));
  }

  private Query buildQueryWithFilterSortAndProjection() {
    return Query.builder()
        .setFilter(
            LogicalExpression.builder()
                .operator(AND)
                .operand(
                    RelationalExpression.of(
                        IdentifierExpression.of("item"), EQ, ConstantExpression.of("Soap")))
                .operand(
                    RelationalExpression.of(
                        IdentifierExpression.of("date"),
                        LT,
                        ConstantExpression.of("2022-08-09T18:53:17Z")))
                .build())
        .addSort(SortingSpec.of(IdentifierExpression.of("price"), ASC))
        .addSort(SortingSpec.of(IdentifierExpression.of("date"), DESC))
        .addSelection(IdentifierExpression.of("quantity"))
        .addSelection(IdentifierExpression.of("price"))
        .addSelection(IdentifierExpression.of("date"))
        .addSelection(IdentifierExpression.of("props"))
        .build();
  }

  private List<SubDocumentUpdate> buildUpdates() throws IOException {
    final SubDocumentUpdate dateUpdate = SubDocumentUpdate.of("date", "2022-08-09T18:53:17Z");
    final SubDocumentUpdate quantityUpdate = SubDocumentUpdate.of("quantity", 1000);
    final SubDocumentUpdate propsUpdate =
        SubDocumentUpdate.of(
            "props", SubDocumentValue.of(new JSONDocument("{\"brand\": \"Dettol\"}")));
    return List.of(dateUpdate, quantityUpdate, propsUpdate);
  }

  private void mockResultSetMetadata(final String id) throws SQLException {
    when(mockResultSetMetaData.getColumnCount()).thenReturn(5);

    mockResultSetMetadata();

    when(mockResultSetMetaData.getColumnName(5)).thenReturn("_implicit_id");
    when(mockResultSetMetaData.getColumnType(5)).thenReturn(VARCHAR);
    when(mockResultSet.getString(5)).thenReturn(id);
  }

  @Test
  void testPostgresResultIteratorWithBasicTypesUsage() throws SQLException, IOException {
    // Test that PostgresResultIteratorWithBasicTypes can handle various column types
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(6);

    // Setup different column types to test the addColumnToJsonNode method
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("boolean_field");
    when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("bool");
    when(mockResultSet.getBoolean(1)).thenReturn(true);
    when(mockResultSet.wasNull()).thenReturn(false);

    when(mockResultSetMetaData.getColumnName(2)).thenReturn("integer_field");
    when(mockResultSetMetaData.getColumnTypeName(2)).thenReturn("int4");
    when(mockResultSet.getInt(2)).thenReturn(42);

    when(mockResultSetMetaData.getColumnName(3)).thenReturn("bigint_field");
    when(mockResultSetMetaData.getColumnTypeName(3)).thenReturn("int8");
    when(mockResultSet.getLong(3)).thenReturn(123456789L);

    when(mockResultSetMetaData.getColumnName(4)).thenReturn("double_field");
    when(mockResultSetMetaData.getColumnTypeName(4)).thenReturn("float8");
    when(mockResultSet.getDouble(4)).thenReturn(3.14159);

    when(mockResultSetMetaData.getColumnName(5)).thenReturn("text_field");
    when(mockResultSetMetaData.getColumnTypeName(5)).thenReturn("text");
    when(mockResultSet.getString(5)).thenReturn("sample text");

    when(mockResultSetMetaData.getColumnName(6)).thenReturn("jsonb_field");
    when(mockResultSetMetaData.getColumnTypeName(6)).thenReturn("jsonb");
    when(mockResultSet.getString(6)).thenReturn("{\"nested\":\"value\"}");

    // Create and test the iterator directly
    PostgresCollection.PostgresResultIteratorWithBasicTypes iterator =
        new PostgresCollection.PostgresResultIteratorWithBasicTypes(
            mockResultSet, DocumentType.FLAT);

    assertTrue(iterator.hasNext());
    Document result = iterator.next();

    assertNotNull(result);
    assertEquals(DocumentType.FLAT, result.getDocumentType());

    String json = result.toJson();
    assertTrue(json.contains("\"boolean_field\":true"));
    assertTrue(json.contains("\"integer_field\":42"));
    assertTrue(json.contains("\"bigint_field\":123456789"));
    assertTrue(json.contains("\"double_field\":3.14159"));
    assertTrue(json.contains("\"text_field\":\"sample text\""));
    assertTrue(json.contains("\"jsonb_field\":{\"nested\":\"value\"}"));

    assertFalse(iterator.hasNext());
    iterator.close();
  }

  @Test
  void testPostgresResultIteratorWithBasicTypesNullHandling() throws SQLException, IOException {
    // Test null value handling in PostgresResultIteratorWithBasicTypes
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(3);

    when(mockResultSetMetaData.getColumnName(1)).thenReturn("nullable_int");
    when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("int4");
    when(mockResultSet.getInt(1)).thenReturn(0);
    when(mockResultSet.wasNull()).thenReturn(true);

    when(mockResultSetMetaData.getColumnName(2)).thenReturn("nullable_text");
    when(mockResultSetMetaData.getColumnTypeName(2)).thenReturn("text");
    when(mockResultSet.getString(2)).thenReturn(null);

    when(mockResultSetMetaData.getColumnName(3)).thenReturn("valid_text");
    when(mockResultSetMetaData.getColumnTypeName(3)).thenReturn("text");
    when(mockResultSet.getString(3)).thenReturn("not null");

    PostgresCollection.PostgresResultIteratorWithBasicTypes iterator =
        new PostgresCollection.PostgresResultIteratorWithBasicTypes(mockResultSet);

    assertTrue(iterator.hasNext());
    Document result = iterator.next();

    assertNotNull(result);
    String json = result.toJson();

    // Null values should not appear in the JSON
    assertFalse(json.contains("nullable_int"));
    assertFalse(json.contains("nullable_text"));
    assertTrue(json.contains("\"valid_text\":\"not null\""));

    iterator.close();
  }

  @Test
  void testPostgresResultIteratorWithBasicTypesArrayColumn() throws SQLException, IOException {
    // Test array column handling
    when(mockResultSet.next()).thenReturn(true, false);
    when(mockResultSet.getMetaData()).thenReturn(mockResultSetMetaData);
    when(mockResultSetMetaData.getColumnCount()).thenReturn(1);

    when(mockResultSetMetaData.getColumnName(1)).thenReturn("array_field");
    when(mockResultSetMetaData.getColumnTypeName(1)).thenReturn("_text");

    Array mockArray = mock(Array.class);
    String[] arrayData = {"item1", "item2", "item3"};
    when(mockResultSet.getArray(1)).thenReturn(mockArray);
    when(mockArray.getArray()).thenReturn(arrayData);

    PostgresCollection.PostgresResultIteratorWithBasicTypes iterator =
        new PostgresCollection.PostgresResultIteratorWithBasicTypes(mockResultSet);

    assertTrue(iterator.hasNext());
    Document result = iterator.next();

    assertNotNull(result);
    String json = result.toJson();
    assertTrue(json.contains("\"array_field\":[\"item1\",\"item2\",\"item3\"]"));

    iterator.close();
  }

  private void mockResultSetMetadata() throws SQLException {
    when(mockResultSetMetaData.getColumnName(1)).thenReturn("quantity");
    when(mockResultSetMetaData.getColumnType(1)).thenReturn(INTEGER);
    when(mockResultSet.getString(1)).thenReturn("5");

    when(mockResultSetMetaData.getColumnName(2)).thenReturn("price");
    when(mockResultSetMetaData.getColumnType(2)).thenReturn(INTEGER);
    when(mockResultSet.getString(2)).thenReturn("10");

    when(mockResultSetMetaData.getColumnName(3)).thenReturn("date");
    when(mockResultSetMetaData.getColumnType(3)).thenReturn(VARCHAR);
    when(mockResultSet.getString(3)).thenReturn("\"2016-02-06T20:20:13Z\"");

    when(mockResultSetMetaData.getColumnName(4)).thenReturn("props");
    when(mockResultSetMetaData.getColumnType(4)).thenReturn(VARCHAR);
    when(mockResultSet.getString(4)).thenReturn(null);
  }
}
