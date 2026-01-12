package org.hypertrace.core.documentstore.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresMetadataFetcherTest {

  private static final String TEST_TABLE = "test_table";

  @Mock private PostgresClient client;
  @Mock private Connection connection;
  @Mock private PreparedStatement preparedStatement;
  @Mock private ResultSet resultSet;

  private PostgresMetadataFetcher fetcher;

  @BeforeEach
  void setUp() throws SQLException {
    when(client.getPooledConnection()).thenReturn(connection);
    when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    when(preparedStatement.executeQuery()).thenReturn(resultSet);
    fetcher = new PostgresMetadataFetcher(client);
  }

  @Test
  void fetchReturnsEmptyMapForTableWithNoColumns() throws SQLException {
    when(resultSet.next()).thenReturn(false);

    Map<String, PostgresColumnMetadata> result = fetcher.fetch(TEST_TABLE);

    assertTrue(result.isEmpty());
    verify(preparedStatement).setString(1, TEST_TABLE);
  }

  @Test
  void fetchReturnsSingleColumn() throws SQLException {
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("column_name")).thenReturn("id");
    when(resultSet.getString("udt_name")).thenReturn("int4");
    when(resultSet.getString("is_nullable")).thenReturn("NO");

    Map<String, PostgresColumnMetadata> result = fetcher.fetch(TEST_TABLE);

    assertEquals(1, result.size());
    PostgresColumnMetadata metadata = result.get("id");
    assertNotNull(metadata);
    assertEquals("id", metadata.getName());
    assertEquals(DataType.INTEGER, metadata.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, metadata.getPostgresType());
    assertFalse(metadata.isNullable());
  }

  @Test
  void fetchReturnsMultipleColumns() throws SQLException {
    when(resultSet.next()).thenReturn(true, true, true, false);
    when(resultSet.getString("column_name")).thenReturn("id", "name", "price");
    when(resultSet.getString("udt_name")).thenReturn("int8", "text", "float8");
    when(resultSet.getString("is_nullable")).thenReturn("NO", "YES", "YES");

    Map<String, PostgresColumnMetadata> result = fetcher.fetch(TEST_TABLE);

    assertEquals(3, result.size());

    // Verify id column
    PostgresColumnMetadata idMeta = result.get("id");
    assertEquals(DataType.LONG, idMeta.getCanonicalType());
    assertEquals(PostgresDataType.BIGINT, idMeta.getPostgresType());
    assertFalse(idMeta.isNullable());

    // Verify name column
    PostgresColumnMetadata nameMeta = result.get("name");
    assertEquals(DataType.STRING, nameMeta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, nameMeta.getPostgresType());
    assertTrue(nameMeta.isNullable());

    // Verify price column
    PostgresColumnMetadata priceMeta = result.get("price");
    assertEquals(DataType.DOUBLE, priceMeta.getCanonicalType());
    assertEquals(PostgresDataType.DOUBLE_PRECISION, priceMeta.getPostgresType());
    assertTrue(priceMeta.isNullable());
  }

  @Test
  void fetchMapsInt4ToInteger() throws SQLException {
    setupSingleColumnResult("col", "int4", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.INTEGER, meta.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, meta.getPostgresType());
  }

  @Test
  void fetchMapsInt2ToInteger() throws SQLException {
    setupSingleColumnResult("col", "int2", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.INTEGER, meta.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, meta.getPostgresType());
  }

  @Test
  void fetchMapsInt8ToLong() throws SQLException {
    setupSingleColumnResult("col", "int8", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.LONG, meta.getCanonicalType());
    assertEquals(PostgresDataType.BIGINT, meta.getPostgresType());
  }

  @Test
  void fetchMapsFloat4ToFloat() throws SQLException {
    setupSingleColumnResult("col", "float4", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.FLOAT, meta.getCanonicalType());
    assertEquals(PostgresDataType.REAL, meta.getPostgresType());
  }

  @Test
  void fetchMapsFloat8ToDouble() throws SQLException {
    setupSingleColumnResult("col", "float8", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.DOUBLE, meta.getCanonicalType());
    assertEquals(PostgresDataType.DOUBLE_PRECISION, meta.getPostgresType());
  }

  @Test
  void fetchMapsNumericToDouble() throws SQLException {
    setupSingleColumnResult("col", "numeric", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.DOUBLE, meta.getCanonicalType());
    assertEquals(PostgresDataType.DOUBLE_PRECISION, meta.getPostgresType());
  }

  @Test
  void fetchMapsBoolToBoolean() throws SQLException {
    setupSingleColumnResult("col", "bool", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.BOOLEAN, meta.getCanonicalType());
    assertEquals(PostgresDataType.BOOLEAN, meta.getPostgresType());
  }

  @Test
  void fetchMapsTextToString() throws SQLException {
    setupSingleColumnResult("col", "text", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.STRING, meta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, meta.getPostgresType());
  }

  @Test
  void fetchMapsVarcharToString() throws SQLException {
    setupSingleColumnResult("col", "varchar", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.STRING, meta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, meta.getPostgresType());
  }

  @Test
  void fetchMapsBpcharToString() throws SQLException {
    setupSingleColumnResult("col", "bpchar", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.STRING, meta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, meta.getPostgresType());
  }

  @Test
  void fetchMapsUuidToString() throws SQLException {
    setupSingleColumnResult("col", "uuid", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.STRING, meta.getCanonicalType());
    assertEquals(PostgresDataType.TEXT, meta.getPostgresType());
  }

  @Test
  void fetchMapsJsonbToJson() throws SQLException {
    setupSingleColumnResult("col", "jsonb", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.JSON, meta.getCanonicalType());
    assertEquals(PostgresDataType.JSONB, meta.getPostgresType());
  }

  @Test
  void fetchMapsTimestamptzToTimestamptz() throws SQLException {
    setupSingleColumnResult("col", "timestamptz", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.TIMESTAMPTZ, meta.getCanonicalType());
    assertEquals(PostgresDataType.TIMESTAMPTZ, meta.getPostgresType());
  }

  @Test
  void fetchMapsDateToDate() throws SQLException {
    setupSingleColumnResult("col", "date", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.DATE, meta.getCanonicalType());
    assertEquals(PostgresDataType.DATE, meta.getPostgresType());
  }

  @Test
  void fetchMapsUnknownTypeToUnspecified() throws SQLException {
    setupSingleColumnResult("col", "unknown_type", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.UNSPECIFIED, meta.getCanonicalType());
    assertEquals(PostgresDataType.UNKNOWN, meta.getPostgresType());
  }

  @Test
  void fetchMapsNullUdtNameToUnspecified() throws SQLException {
    setupSingleColumnResult("col", null, "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.UNSPECIFIED, meta.getCanonicalType());
    assertEquals(PostgresDataType.UNKNOWN, meta.getPostgresType());
  }

  @Test
  void fetchHandlesNullableColumn() throws SQLException {
    setupSingleColumnResult("col", "text", "YES");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertTrue(meta.isNullable());
  }

  @Test
  void fetchHandlesNonNullableColumn() throws SQLException {
    setupSingleColumnResult("col", "text", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertFalse(meta.isNullable());
  }

  @Test
  void fetchHandlesCaseInsensitiveUdtName() throws SQLException {
    setupSingleColumnResult("col", "INT4", "NO");

    PostgresColumnMetadata meta = fetcher.fetch(TEST_TABLE).get("col");

    assertEquals(DataType.INTEGER, meta.getCanonicalType());
    assertEquals(PostgresDataType.INTEGER, meta.getPostgresType());
  }

  @Test
  void fetchThrowsRuntimeExceptionOnSqlException() throws SQLException {
    when(preparedStatement.executeQuery()).thenThrow(new SQLException("Connection failed"));

    RuntimeException exception =
        assertThrows(RuntimeException.class, () -> fetcher.fetch(TEST_TABLE));

    assertTrue(exception.getMessage().contains(TEST_TABLE));
    assertTrue(exception.getCause() instanceof SQLException);
  }

  private void setupSingleColumnResult(String colName, String udtName, String isNullable)
      throws SQLException {
    when(resultSet.next()).thenReturn(true, false);
    when(resultSet.getString("column_name")).thenReturn(colName);
    when(resultSet.getString("udt_name")).thenReturn(udtName);
    when(resultSet.getString("is_nullable")).thenReturn(isNullable);
  }
}
