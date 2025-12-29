package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FlatPostgresFieldTransformer;
import org.hypertrace.core.documentstore.query.Query;
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String WRITE_NOT_SUPPORTED =
      "Write operations are not supported for flat collections yet!";

  private final PostgresSchemaRegistry schemaRegistry;

  FlatPostgresCollection(
      final PostgresClient client,
      final String collectionName,
      final PostgresSchemaRegistry schemaRegistry) {
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
    String tableName = tableIdentifier.getTableName();
    Map<String, PostgresColumnMetadata> schema = schemaRegistry.getSchema(tableName);

    if (schema.isEmpty()) {
      throw new IOException("No schema found for table: " + tableName);
    }

    try {
      JsonNode docJson = OBJECT_MAPPER.readTree(document.toJson());
      List<String> columns = new ArrayList<>();
      List<Object> values = new ArrayList<>();

      // Extract fields from document that exist in schema
      Iterator<Map.Entry<String, JsonNode>> fields = docJson.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> field = fields.next();
        String colName = field.getKey();
        if (schemaRegistry.getColumnOrRefresh(tableName, colName).isPresent()) {
          columns.add(colName);
          values.add(extractValue(field.getValue()));
        }
      }

      if (columns.isEmpty()) {
        throw new IOException("No matching columns found in schema for document");
      }

      // Build UPSERT SQL: INSERT ... ON CONFLICT DO UPDATE
      String columnList = String.join(", ", columns);
      String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
      String updateSet =
          columns.stream().map(c -> c + " = EXCLUDED." + c).collect(Collectors.joining(", "));

      // Determine primary key column (assume first column or 'id')
      String pkColumn = schema.containsKey("id") ? "id" : columns.get(0);

      String sql =
          String.format(
              "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING *",
              tableIdentifier, columnList, placeholders, pkColumn, updateSet);

      try (PreparedStatement ps = client.getConnection().prepareStatement(sql)) {
        for (int i = 0; i < values.size(); i++) {
          ps.setObject(i + 1, values.get(i));
        }

        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            return resultSetToDocument(rs, columns);
          }
        }
      }
      return document;
    } catch (SQLException e) {
      LOGGER.error("SQLException in upsertAndReturn. key: {} document: {}", key, document, e);
      throw new IOException(e);
    }
  }

  private Object extractValue(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.booleanValue();
    } else if (node.isInt()) {
      return node.intValue();
    } else if (node.isLong()) {
      return node.longValue();
    } else if (node.isDouble() || node.isFloat()) {
      return node.doubleValue();
    } else if (node.isTextual()) {
      return node.textValue();
    } else {
      return node.toString();
    }
  }

  private Document resultSetToDocument(ResultSet rs, List<String> columns)
      throws SQLException, IOException {
    StringBuilder json = new StringBuilder("{");
    for (int i = 0; i < columns.size(); i++) {
      if (i > 0) {
        json.append(",");
      }
      String col = columns.get(i);
      Object value = rs.getObject(col);
      json.append("\"").append(col).append("\":");
      if (value == null) {
        json.append("null");
      } else if (value instanceof String) {
        json.append("\"").append(value).append("\"");
      } else if (value instanceof Boolean) {
        json.append(value);
      } else {
        json.append(value);
      }
    }
    json.append("}");
    return new org.hypertrace.core.documentstore.JSONDocument(json.toString());
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
