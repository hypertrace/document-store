package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
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
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  @Override
  public Document createOrReplaceAndReturn(Key key, Document document) throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
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

  private TypedDocument parseDocument(Document document, String tableName) throws IOException {
    JsonNode jsonNode = MAPPER.readTree(document.toJson());
    TypedDocument typedDocument = new TypedDocument();

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
      boolean isArray = columnMetadata.get().isArray();
      typedDocument.add(
          "\"" + fieldName + "\"", extractValue(fieldValue, type, isArray), type, isArray);
    }

    return typedDocument;
  }

  private int executeUpdate(String sql, TypedDocument parsed) throws SQLException {
    try (Connection conn = client.getPooledConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      int index = 1;
      for (String column : parsed.getColumns()) {
        setParameter(
            conn,
            ps,
            index++,
            parsed.getValue(column),
            parsed.getType(column),
            parsed.isArray(column));
      }
      return ps.executeUpdate();
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

  private boolean shouldRefreshSchemaAndRetry(String sqlState) {
    return PSQLState.UNDEFINED_COLUMN.getState().equals(sqlState)
        || PSQLState.DATATYPE_MISMATCH.getState().equals(sqlState);
  }

  /**
   * Typed document contains field information along with the field type. Uses LinkedHashMaps keyed
   * by column name. LinkedHashMap preserves insertion order for consistent parameter binding.
   */
  private static class TypedDocument {
    private final Map<String, Object> values = new HashMap<>();
    private final Map<String, PostgresDataType> types = new HashMap<>();
    private final Map<String, Boolean> arrays = new HashMap<>();

    void add(String column, Object value, PostgresDataType type, boolean isArray) {
      values.put(column, value);
      types.put(column, type);
      arrays.put(column, isArray);
    }

    boolean isEmpty() {
      return values.isEmpty();
    }

    List<String> getColumns() {
      return new ArrayList<>(values.keySet());
    }

    Object getValue(String column) {
      return values.get(column);
    }

    PostgresDataType getType(String column) {
      return types.get(column);
    }

    boolean isArray(String column) {
      return arrays.getOrDefault(column, false);
    }
  }

  private String buildInsertSql(List<String> columns) {
    String columnList = String.join(", ", columns);
    String placeholders = String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new));
    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)", tableIdentifier, columnList, placeholders);
  }

  private Object extractValue(JsonNode node, PostgresDataType type, boolean isArray) {
    if (node == null || node.isNull()) {
      return null;
    }

    if (isArray) {
      if (!node.isArray()) {
        node = MAPPER.createArrayNode().add(node);
      }
      List<Object> values = new ArrayList<>();
      for (JsonNode element : node) {
        values.add(extractScalarValue(element, type));
      }
      return values.toArray();
    }

    return extractScalarValue(node, type);
  }

  private Object extractScalarValue(JsonNode node, PostgresDataType type) {
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

  private void setParameter(
      Connection conn,
      PreparedStatement ps,
      int index,
      Object value,
      PostgresDataType type,
      boolean isArray)
      throws SQLException {
    if (value == null) {
      ps.setObject(index, null);
      return;
    }

    if (isArray) {
      Object[] arrayValues = (value instanceof Object[]) ? (Object[]) value : new Object[] {value};
      java.sql.Array sqlArray = conn.createArrayOf(type.getSqlType(), arrayValues);
      ps.setArray(index, sqlArray);
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
