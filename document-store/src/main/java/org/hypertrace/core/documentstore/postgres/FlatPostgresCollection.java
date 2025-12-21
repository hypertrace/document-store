package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
import org.hypertrace.core.documentstore.TypedDocument;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FlatPostgresFieldTransformer;
import org.hypertrace.core.documentstore.query.Query;
import org.postgresql.util.PGobject;
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
  private static final String ID_COLUMN = "id";

  FlatPostgresCollection(final PostgresClient client, final String collectionName) {
    super(client, collectionName);
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
    if (!(document instanceof TypedDocument)) {
      throw new IllegalArgumentException(
          "FlatPostgresCollection.upsert requires a TypedDocument. "
              + "Use TypedDocument.builder(doc).field(...).build()");
    }

    TypedDocument typedDoc = (TypedDocument) document;
    return upsertTypedDocument(key, typedDoc);
  }

  private boolean upsertTypedDocument(Key key, TypedDocument typedDoc) throws IOException {
    try {
      List<String> columns = new ArrayList<>();
      List<TypedDocument.TypedField> typedFields = new ArrayList<>();

      // Add the id column
      columns.add(ID_COLUMN);
      typedFields.add(TypedDocument.TypedField.scalar(key.toString(), DataType.STRING));

      // Extract fields from TypedDocument
      for (Map.Entry<String, TypedDocument.TypedField> entry : typedDoc.getFields().entrySet()) {
        String fieldName = entry.getKey();

        // Skip id as we already added it
        if (ID_COLUMN.equals(fieldName)) {
          continue;
        }

        columns.add(fieldName);
        typedFields.add(entry.getValue());
      }

      // Build and execute the upsert SQL
      String sql = buildUpsertSql(columns);
      return executeUpsert(sql, columns, typedFields);

    } catch (SQLException | JsonProcessingException e) {
      LOGGER.error("Exception during upsert for key: {}", key, e);
      throw new IOException(e);
    }
  }

  private String buildUpsertSql(List<String> columns) {
    StringBuilder sql = new StringBuilder();
    sql.append("INSERT INTO ").append(tableIdentifier).append(" (");

    // Column names
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) sql.append(", ");
      sql.append("\"").append(columns.get(i)).append("\"");
    }

    sql.append(") VALUES (");

    // Placeholders
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) sql.append(", ");
      sql.append("?");
    }

    sql.append(") ON CONFLICT (\"").append(ID_COLUMN).append("\") DO UPDATE SET ");

    // Update clause (skip _id)
    boolean first = true;
    for (int i = 1; i < columns.size(); i++) {
      if (!first) sql.append(", ");
      sql.append("\"")
          .append(columns.get(i))
          .append("\" = EXCLUDED.\"")
          .append(columns.get(i))
          .append("\"");
      first = false;
    }

    return sql.toString();
  }

  private boolean executeUpsert(
      String sql, List<String> columns, List<TypedDocument.TypedField> typedFields)
      throws SQLException, JsonProcessingException {

    Connection connection = client.getConnection();
    try (PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

      for (int i = 0; i < typedFields.size(); i++) {
        int paramIndex = i + 1;
        TypedDocument.TypedField field = typedFields.get(i);
        Object value = field.getValue();

        if (value == null) {
          ps.setObject(paramIndex, null);
        } else if (field.isJsonb()) {
          // JSONB column - serialize value to JSON string
          PGobject jsonObj = new PGobject();
          jsonObj.setType("jsonb");
          String jsonValue =
              (value instanceof String) ? (String) value : MAPPER.writeValueAsString(value);
          jsonObj.setValue(jsonValue);
          ps.setObject(paramIndex, jsonObj);
        } else if (field.isArray()) {
          // Array column
          DataType elementType = field.getDataType();
          PostgresDataType pgType = PostgresDataType.fromDataType(elementType);
          Object[] arrayValues = ((Collection<?>) value).toArray();
          Array sqlArray = connection.createArrayOf(pgType.getSqlType(), arrayValues);
          ps.setArray(paramIndex, sqlArray);
        } else {
          // Scalar column
          setScalarParameter(ps, paramIndex, value, field.getDataType());
        }
      }

      int result = ps.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Upsert result: {}, SQL: {}", result, sql);
      }
      return result > 0;
    }
  }

  private void setScalarParameter(PreparedStatement ps, int index, Object value, DataType type)
      throws SQLException {
    if (value == null) {
      ps.setObject(index, null);
      return;
    }

    if (type == null) {
      // No type specified, let JDBC infer
      ps.setObject(index, value);
      return;
    }

    switch (type) {
      case STRING:
        ps.setString(index, value.toString());
        break;
      case INTEGER:
        ps.setInt(index, ((Number) value).intValue());
        break;
      case LONG:
        ps.setLong(index, ((Number) value).longValue());
        break;
      case FLOAT:
        ps.setFloat(index, ((Number) value).floatValue());
        break;
      case DOUBLE:
        ps.setDouble(index, ((Number) value).doubleValue());
        break;
      case BOOLEAN:
        ps.setBoolean(index, (Boolean) value);
        break;
      case TIMESTAMPTZ:
        if (value instanceof Timestamp) {
          ps.setTimestamp(index, (Timestamp) value);
        } else {
          ps.setObject(index, value);
        }
        break;
      case DATE:
        if (value instanceof java.sql.Date) {
          ps.setDate(index, (java.sql.Date) value);
        } else {
          ps.setObject(index, value);
        }
        break;
      default:
        ps.setObject(index, value);
    }
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
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
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
}
