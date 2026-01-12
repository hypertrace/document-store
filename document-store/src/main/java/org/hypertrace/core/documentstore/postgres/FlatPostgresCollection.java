package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FlatPostgresFieldTransformer;
import org.hypertrace.core.documentstore.query.Query;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL collection implementation for flat documents. All fields are stored as top-level
 * PostgreSQL columns.
 *
 * <p>Write operations are not supported for flat collections. All write methods throw {@link
 * UnsupportedOperationException}.
 */
public class FlatPostgresCollection extends PostgresCollection {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlatPostgresCollection.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String WRITE_NOT_SUPPORTED =
      "Write operations are not supported for flat collections yet!";

  private final PostgresLazyilyLoadedSchemaRegistry schemaRegistry;

  FlatPostgresCollection(
      final PostgresClient client,
      final String collectionName,
      final PostgresLazyilyLoadedSchemaRegistry schemaRegistry) {
    super(client, collectionName);
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public CloseableIterator<Document> query(
      final org.hypertrace.core.documentstore.query.Query query, final QueryOptions queryOptions) {
    PostgresQueryParser queryParser = createParser(query);
    return queryWithParser(query, queryParser);
  }

  @Override
  public CloseableIterator<Document> find(
      final org.hypertrace.core.documentstore.query.Query query) {
    PostgresQueryParser queryParser = createParser(query);
    return queryWithParser(query, queryParser);
  }

  @Override
  public long count(
      org.hypertrace.core.documentstore.query.Query query, QueryOptions queryOptions) {
    PostgresQueryParser queryParser =
        new PostgresQueryParser(
            tableIdentifier,
            query,
            new org.hypertrace.core.documentstore.postgres.query.v1.transformer
                .FlatPostgresFieldTransformer());
    return countWithParser(query, queryParser);
  }

  private PostgresQueryParser createParser(Query query) {
    return new PostgresQueryParser(
        tableIdentifier,
        PostgresQueryExecutor.transformAndLog(query),
        new FlatPostgresFieldTransformer());
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public BulkUpdateResult bulkUpdateSubDocs(Map<Key, Map<String, Document>> documents) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean delete(Key key) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean delete(Filter filter) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public BulkDeleteResult delete(Set<Key> keys) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean deleteAll() {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean deleteSubDoc(Key key, String subDocPath) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public void drop() {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public CreateResult create(Key key, Document document) throws IOException {
    return createWithRetry(key, document, false);
  }

  @Override
  public boolean createOrReplace(Key key, Document document) throws IOException {
    return createOrReplaceWithRetry(key, document, false);
  }

  @Override
  public Document createOrReplaceAndReturn(Key key, Document document) throws IOException {
    return createOrReplaceAndReturnWithRetry(key, document, false);
  }

  @Override
  public BulkUpdateResult bulkUpdate(List<BulkUpdateRequest> bulkUpdateRequests) {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public UpdateResult update(Key key, Document document, Filter condition) throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public Optional<Document> update(
      org.hypertrace.core.documentstore.query.Query query,
      java.util.Collection<SubDocumentUpdate> updates,
      UpdateOptions updateOptions)
      throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public CloseableIterator<Document> bulkUpdate(
      org.hypertrace.core.documentstore.query.Query query,
      java.util.Collection<SubDocumentUpdate> updates,
      UpdateOptions updateOptions)
      throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  private CreateResult createWithRetry(Key key, Document document, boolean isRetry)
      throws IOException {
    String tableName = tableIdentifier.getTableName();

    try {
      TypedDocument parsed = parseDocument(document, tableName);
      // if there are no valid columns in the document
      if (parsed.isEmpty()) {
        LOGGER.warn("No valid columns found in the document for table: {}", tableName);
        return new CreateResult(false);
      }

      String sql = buildInsertSql(parsed.getColumns());
      LOGGER.debug("Insert SQL: {}", sql);

      int result = executeUpdate(sql, parsed);
      LOGGER.debug("Create result: {}", result);
      return new CreateResult(result > 0);

    } catch (PSQLException e) {
      if (PSQLState.UNIQUE_VIOLATION.getState().equals(e.getSQLState())) {
        throw new DuplicateDocumentException();
      }
      return handlePSQLExceptionForCreate(e, key, document, tableName, isRetry);
    } catch (SQLException e) {
      LOGGER.error("SQLException creating document. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  private boolean createOrReplaceWithRetry(Key key, Document document, boolean isRetry)
      throws IOException {
    String tableName = tableIdentifier.getTableName();

    try {
      TypedDocument parsed = parseDocument(document, tableName);
      if (parsed.isEmpty()) {
        LOGGER.warn("No valid columns found in the document for table: {}", tableName);
        return false;
      }

      String sql = buildUpsertSql(parsed.getColumns());
      LOGGER.debug("Upsert SQL: {}", sql);

      int result = executeUpdate(sql, parsed);
      LOGGER.debug("CreateOrReplace result: {}", result);
      return result > 0;

    } catch (PSQLException e) {
      return handlePSQLExceptionForCreateOrReplace(e, key, document, tableName, isRetry);
    } catch (SQLException e) {
      LOGGER.error("SQLException in createOrReplace. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  private Document createOrReplaceAndReturnWithRetry(Key key, Document document, boolean isRetry)
      throws IOException {
    String tableName = tableIdentifier.getTableName();

    try {
      TypedDocument parsed = parseDocument(document, tableName);
      if (parsed.isEmpty()) {
        LOGGER.warn("No valid columns found in the document for table: {}", tableName);
        return null;
      }

      String sql = buildUpsertSqlWithReturning(parsed.getColumns());
      LOGGER.debug("Upsert with RETURNING SQL: {}", sql);

      return executeQueryAndReturn(sql, parsed);

    } catch (PSQLException e) {
      return handlePSQLExceptionForCreateOrReplaceAndReturn(e, key, document, tableName, isRetry);
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException in createOrReplaceAndReturn. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  private TypedDocument parseDocument(Document document, String tableName) throws IOException {
    JsonNode jsonNode = MAPPER.readTree(document.toJson());
    List<ColumnEntry> entries = new ArrayList<>();

    Iterator<Entry<String, JsonNode>> fields = jsonNode.fields();
    while (fields.hasNext()) {
      Entry<String, JsonNode> field = fields.next();
      String fieldName = field.getKey();
      JsonNode fieldValue = field.getValue();

      Optional<PostgresColumnMetadata> columnMetadata =
          schemaRegistry.getColumnOrRefresh(tableName, fieldName);

      if (columnMetadata.isEmpty()) {
        LOGGER.warn("Could not find column metadata for column: {}, skipping it", fieldName);
        continue;
      }

      PostgresDataType type = columnMetadata.get().getPostgresType();
      entries.add(new ColumnEntry("\"" + fieldName + "\"", extractValue(fieldValue, type), type));
    }

    return new TypedDocument(entries);
  }

  private int executeUpdate(String sql, TypedDocument parsed) throws SQLException {
    try (Connection conn = client.getPooledConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      int index = 1;
      for (ColumnEntry entry : parsed.entries) {
        setParameter(ps, index++, entry.value, entry.type);
      }
      return ps.executeUpdate();
    }
  }

  private Document executeQueryAndReturn(String sql, TypedDocument parsed)
      throws SQLException, IOException {
    try (Connection conn = client.getPooledConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      int index = 1;
      for (ColumnEntry entry : parsed.entries) {
        setParameter(ps, index++, entry.value, entry.type);
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return resultSetToDocument(rs);
        }
        return null;
      }
    }
  }

  private CreateResult handlePSQLExceptionForCreate(
      PSQLException e, Key key, Document document, String tableName, boolean isRetry)
      throws IOException {
    if (!isRetry && shouldRefreshSchemaAndRetry(e.getSQLState())) {
      LOGGER.info(
          "Schema mismatch detected (SQLState: {}), refreshing schema and retrying. key: {}",
          e.getSQLState(),
          key);
      schemaRegistry.invalidate(tableName);
      return createWithRetry(key, document, true);
    }
    LOGGER.error("SQLException creating document. key: {} content: {}", key, document, e);
    throw new IOException(e);
  }

  private boolean handlePSQLExceptionForCreateOrReplace(
      PSQLException e, Key key, Document document, String tableName, boolean isRetry)
      throws IOException {
    if (!isRetry && shouldRefreshSchemaAndRetry(e.getSQLState())) {
      LOGGER.info(
          "Schema mismatch detected (SQLState: {}), refreshing schema and retrying. key: {}",
          e.getSQLState(),
          key);
      schemaRegistry.invalidate(tableName);
      return createOrReplaceWithRetry(key, document, true);
    }
    LOGGER.error("SQLException in createOrReplace. key: {} content: {}", key, document, e);
    throw new IOException(e);
  }

  private Document handlePSQLExceptionForCreateOrReplaceAndReturn(
      PSQLException e, Key key, Document document, String tableName, boolean isRetry)
      throws IOException {
    if (!isRetry && shouldRefreshSchemaAndRetry(e.getSQLState())) {
      LOGGER.info(
          "Schema mismatch detected (SQLState: {}), refreshing schema and retrying. key: {}",
          e.getSQLState(),
          key);
      schemaRegistry.invalidate(tableName);
      return createOrReplaceAndReturnWithRetry(key, document, true);
    }
    LOGGER.error("SQLException in createOrReplaceAndReturn. key: {} content: {}", key, document, e);
    throw new IOException(e);
  }

  private boolean shouldRefreshSchemaAndRetry(String sqlState) {
    return PSQLState.UNDEFINED_COLUMN.getState().equals(sqlState)
        || PSQLState.DATATYPE_MISMATCH.getState().equals(sqlState);
  }

  private static class ColumnEntry {
    final String column;
    final Object value;
    final PostgresDataType type;

    ColumnEntry(String column, Object value, PostgresDataType type) {
      this.column = column;
      this.value = value;
      this.type = type;
    }
  }

  private static class TypedDocument {
    final List<ColumnEntry> entries;

    TypedDocument(List<ColumnEntry> entries) {
      this.entries = entries;
    }

    boolean isEmpty() {
      return entries.isEmpty();
    }

    List<String> getColumns() {
      return entries.stream().map(e -> e.column).collect(Collectors.toList());
    }
  }

  private String buildUpsertSql(List<String> columns) {
    String columnList = String.join(", ", columns);
    String placeholders = String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new));
    String updateSet =
        columns.stream()
            .filter(col -> !"\"id\"".equals(col))
            .map(col -> col + " = EXCLUDED." + col)
            .collect(Collectors.joining(", "));

    if (updateSet.isEmpty()) {
      return String.format(
          "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (\"id\") DO NOTHING",
          tableIdentifier, columnList, placeholders);
    }

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (\"id\") DO UPDATE SET %s",
        tableIdentifier, columnList, placeholders, updateSet);
  }

  private String buildUpsertSqlWithReturning(List<String> columns) {
    return buildUpsertSql(columns) + " RETURNING *";
  }

  private Document resultSetToDocument(ResultSet rs) throws SQLException, IOException {
    int columnCount = rs.getMetaData().getColumnCount();
    com.fasterxml.jackson.databind.node.ObjectNode objectNode = MAPPER.createObjectNode();

    for (int i = 1; i <= columnCount; i++) {
      String columnName = rs.getMetaData().getColumnName(i);
      Object value = rs.getObject(i);

      if (value == null) {
        objectNode.putNull(columnName);
      } else if (value instanceof Integer) {
        objectNode.put(columnName, (Integer) value);
      } else if (value instanceof Long) {
        objectNode.put(columnName, (Long) value);
      } else if (value instanceof Double) {
        objectNode.put(columnName, (Double) value);
      } else if (value instanceof Float) {
        objectNode.put(columnName, (Float) value);
      } else if (value instanceof Boolean) {
        objectNode.put(columnName, (Boolean) value);
      } else if (value instanceof Timestamp) {
        objectNode.put(columnName, ((Timestamp) value).toInstant().toString());
      } else if (value instanceof java.sql.Date) {
        objectNode.put(columnName, value.toString());
      } else if (value instanceof org.postgresql.util.PGobject) {
        // Handle JSONB
        String jsonValue = ((org.postgresql.util.PGobject) value).getValue();
        if (jsonValue != null) {
          objectNode.set(columnName, MAPPER.readTree(jsonValue));
        } else {
          objectNode.putNull(columnName);
        }
      } else {
        objectNode.put(columnName, value.toString());
      }
    }

    return new JSONDocument(objectNode);
  }

  private String buildInsertSql(List<String> columns) {
    String columnList = String.join(", ", columns);
    String placeholders = String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new));
    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)", tableIdentifier, columnList, placeholders);
  }

  private Object extractValue(JsonNode node, PostgresDataType type) {
    if (node == null || node.isNull()) {
      return null;
    }

    switch (type) {
      case INTEGER:
        return node.isNumber() ? node.intValue() : Integer.parseInt(node.asText());
      case BIGINT:
        return node.isNumber() ? node.longValue() : Long.parseLong(node.asText());
      case REAL:
        return node.isNumber() ? node.floatValue() : Float.parseFloat(node.asText());
      case DOUBLE_PRECISION:
        return node.isNumber() ? node.doubleValue() : Double.parseDouble(node.asText());
      case BOOLEAN:
        return node.isBoolean() ? node.booleanValue() : Boolean.parseBoolean(node.asText());
      case TIMESTAMPTZ:
        if (node.isTextual()) {
          return Timestamp.from(Instant.parse(node.asText()));
        } else if (node.isNumber()) {
          return new Timestamp(node.longValue());
        }
        return null;
      case DATE:
        if (node.isTextual()) {
          return Date.valueOf(node.asText());
        }
        return null;
      case JSONB:
        return node.toString();
      default:
        return node.asText();
    }
  }

  private void setParameter(PreparedStatement ps, int index, Object value, PostgresDataType type)
      throws SQLException {
    if (value == null) {
      ps.setObject(index, null);
      return;
    }

    switch (type) {
      case INTEGER:
        ps.setInt(index, (Integer) value);
        break;
      case BIGINT:
        ps.setLong(index, (Long) value);
        break;
      case REAL:
        ps.setFloat(index, (Float) value);
        break;
      case DOUBLE_PRECISION:
        ps.setDouble(index, (Double) value);
        break;
      case BOOLEAN:
        ps.setBoolean(index, (Boolean) value);
        break;
      case TEXT:
        ps.setString(index, (String) value);
        break;
      case TIMESTAMPTZ:
        ps.setTimestamp(index, (Timestamp) value);
        break;
      case DATE:
        ps.setDate(index, (java.sql.Date) value);
        break;
      case JSONB:
        ps.setObject(index, value, Types.OTHER);
        break;
      default:
        ps.setString(index, value.toString());
    }
  }
}
