package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.UpdateResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides {@link Collection} implementation on Postgres using jsonb format */
public class PostgresCollection implements Collection {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCollection.class);
  public static final String ID = "id";
  public static final String DOCUMENT_ID = "_id";
  public static final String DOCUMENT = "document";
  public static final String UPDATED_AT = "updated_at";
  public static final String CREATED_AT = "created_at";
  public static final String DOC_PATH_SEPARATOR = "\\.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final Connection client;
  private final String collectionName;

  public PostgresCollection(Connection client, String collectionName) {
    this.client = client;
    this.collectionName = collectionName;
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try (PreparedStatement preparedStatement =
        client.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      String jsonString = prepareDocument(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", result);
      }
      return result >= 0;
    } catch (SQLException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw new IOException(e);
    }
  }

  /**
   * Update an existing document if condition is evaluated to true. Conditional will help in
   * providing optimistic locking support for concurrency update.
   */
  @Override
  public UpdateResult update(Key key, Document document, Filter condition) throws IOException {
    StringBuilder upsertQueryBuilder = new StringBuilder(getUpdateSQL());

    String jsonString = prepareDocument(key, document);
    Params.Builder paramsBuilder = Params.newBuilder();
    paramsBuilder.addObjectParam(key.toString());
    paramsBuilder.addObjectParam(jsonString);

    if (condition != null) {
      String conditionQuery = PostgresQueryParser.parseFilter(condition, paramsBuilder);
      if (conditionQuery != null) {
        upsertQueryBuilder.append(" WHERE ").append(conditionQuery);
      }
    }

    try (PreparedStatement preparedStatement =
        buildPreparedStatement(upsertQueryBuilder.toString(), paramsBuilder.build())) {
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", result);
      }
      return new UpdateResult(result);
    } catch (SQLException e) {
      LOGGER.error("SQLException inserting document. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  /** create a new document if one doesn't exists with key */
  @Override
  public CreateResult create(Key key, Document document) throws IOException {
    try (PreparedStatement preparedStatement =
        client.prepareStatement(getInsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      String jsonString = prepareDocument(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Create result: {}", result);
      }
      return new CreateResult(result > 0);
    } catch (SQLException e) {
      LOGGER.error("SQLException creating document. key: {} content:{}", key, document, e);
      throw new IOException(e);
    }
  }

  @Override
  public BulkUpdateResult bulkUpdate(List<BulkUpdateRequest> bulkUpdateRequests) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * For Postgres upsertAndReturn functionality is not supported directly. So using direct upsert
   * and then return document.
   */
  @Override
  public Document upsertAndReturn(Key key, Document document) throws IOException {
    try {
      boolean upsert = upsert(key, document);
      return upsert ? document : null;
    } catch (IOException e) {
      LOGGER.error("SQLException inserting document. key: {} content:{}", key, document, e);
      throw e;
    }
  }

  /**
   * Update the sub document based on subDocPath based on longest key match.
   *
   * <p>As an e.g { "first" : "name", "last" : "lastname" "address" : { "street" : "long street"
   * "pin" : "00000" } }
   *
   * <p>Following subDocPath will match, first address.street address address.pin
   *
   * <p>Following creates new sub-document for matching subDocPath address.street.longitude (here
   * address.street matches)
   *
   * <p>Following subDocPath will not match any sub document, address.street.longitude.degree
   */
  @Override
  public boolean updateSubDoc(Key key, String subDocPath, Document subDocument) {
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s=?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);
    String jsonString = subDocument.toJson();

    try (PreparedStatement preparedStatement =
        client.prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, key.toString());
      int resultSet = preparedStatement.executeUpdate();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", resultSet);
      }

      return true;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException updating sub document. key: {} subDocPath: {} content:{}",
          key,
          subDocPath,
          subDocument,
          e);
    }
    return false;
  }

  @Override
  public BulkUpdateResult bulkUpdateSubDocs(Map<Key, Map<String, Document>> documents)
      throws Exception {
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s = ?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    try {
      PreparedStatement preparedStatement = client.prepareStatement(updateSubDocSQL);
      for (Key key : documents.keySet()) {
        Map<String, Document> subDocuments = documents.get(key);
        for (String subDocPath : subDocuments.keySet()) {
          Document subDocument = subDocuments.get(subDocPath);
          String jsonSubDocPath = getJsonSubDocPath(subDocPath);
          String jsonString = subDocument.toJson();
          preparedStatement.setString(1, jsonSubDocPath);
          preparedStatement.setString(2, jsonString);
          preparedStatement.setString(3, key.toString());
          preparedStatement.addBatch();
        }
      }
      int[] updateCounts = preparedStatement.executeBatch();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", updateCounts);
      }
      int totalUpdateCount = 0;
      for (int update : updateCounts) {
        totalUpdateCount += update;
      }
      return new BulkUpdateResult(totalUpdateCount);
    } catch (SQLException e) {
      LOGGER.error("SQLException updating sub document.", e);
      throw e;
    }
  }

  @Override
  public BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Document> search(Query query) {
    String filters = null;
    StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM ").append(collectionName);
    Params.Builder paramsBuilder = Params.newBuilder();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      filters = PostgresQueryParser.parseFilter(query.getFilter(), paramsBuilder);
    }

    LOGGER.debug("Sending query to PostgresSQL: {} : {}", collectionName, filters);

    if (filters != null) {
      sqlBuilder.append(" WHERE ").append(filters);
    }

    if (!query.getOrderBys().isEmpty()) {
      String orderBySQL = PostgresQueryParser.parseOrderBys(query.getOrderBys());
      sqlBuilder.append(" ORDER BY ").append(orderBySQL);
    }

    Integer limit = query.getLimit();
    if (limit != null && limit >= 0) {
      sqlBuilder.append(" LIMIT ").append(limit);
    }

    Integer offset = query.getOffset();
    if (offset != null && offset >= 0) {
      sqlBuilder.append(" OFFSET ").append(offset);
    }

    try {
      PreparedStatement preparedStatement =
          buildPreparedStatement(sqlBuilder.toString(), paramsBuilder.build());
      ResultSet resultSet = preparedStatement.executeQuery();
      return new PostgresResultIterator(resultSet);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }

    return Collections.emptyIterator();
  }

  @Override
  public Iterator<Document> aggregate(Query query) {
    throw new UnsupportedOperationException();
  }

  @VisibleForTesting
  protected PreparedStatement buildPreparedStatement(String sqlQuery, Params params)
      throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = client.prepareStatement(sqlQuery);
    params
        .getObjectParams()
        .forEach(
            (k, v) -> {
              try {
                if (isValidType(v)) {
                  preparedStatement.setObject(k, v);
                } else {
                  throw new UnsupportedOperationException("Un-supported object types in filter");
                }
              } catch (SQLException e) {
                LOGGER.error("SQLException setting Param. key: {}, value: {}", k, v);
              }
            });
    return preparedStatement;
  }

  private boolean isValidType(Object v) {
    Set<Class<?>> validClassez =
        new HashSet<>() {
          {
            add(Double.class);
            add(Float.class);
            add(Integer.class);
            add(Long.class);
            add(String.class);
            add(Boolean.class);
            add(Number.class);
          }
        };
    return validClassez.contains(v.getClass());
  }

  @VisibleForTesting
  private String getJsonSubDocPath(String subDocPath) {
    return "{" + subDocPath.replaceAll(DOC_PATH_SEPARATOR, ",") + "}";
  }

  @Override
  public boolean delete(Key key) {
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", collectionName, ID);
    try (PreparedStatement preparedStatement = client.prepareStatement(deleteSQL)) {
      preparedStatement.setString(1, key.toString());
      preparedStatement.executeUpdate();
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting document. key: {}", key, e);
    }
    return false;
  }

  @Override
  public boolean deleteSubDoc(Key key, String subDocPath) {
    String deleteSubDocSQL =
        String.format(
            "UPDATE %s SET %s=%s #- ?::text[] WHERE %s=?", collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);

    try (PreparedStatement preparedStatement =
        client.prepareStatement(deleteSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
      preparedStatement.setString(1, jsonSubDocPath);
      preparedStatement.setString(2, key.toString());
      int resultSet = preparedStatement.executeUpdate();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", resultSet);
      }

      return true;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException updating sub document. key: {} subDocPath: {}", key, subDocPath, e);
    }
    return false;
  }

  @Override
  public boolean deleteAll() {
    String deleteSQL = String.format("DELETE FROM %s", collectionName);
    try (PreparedStatement preparedStatement = client.prepareStatement(deleteSQL)) {
      preparedStatement.executeUpdate();
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting all documents.", e);
    }
    return false;
  }

  @Override
  public long count() {
    String countSQL = String.format("SELECT COUNT(*) FROM %s", collectionName);
    long count = -1;
    try (PreparedStatement preparedStatement = client.prepareStatement(countSQL)) {
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        count = resultSet.getLong(1);
      }
    } catch (SQLException e) {
      LOGGER.error("SQLException counting all documents.", e);
    }
    return count;
  }

  @Override
  public long total(Query query) {
    StringBuilder totalSQLBuilder =
        new StringBuilder("SELECT COUNT(*) FROM ").append(collectionName);
    Params.Builder paramsBuilder = Params.newBuilder();

    long count = -1;
    // on any in-correct filter input, it will return total without filtering
    if (query.getFilter() != null) {
      String parsedQuery = PostgresQueryParser.parseFilter(query.getFilter(), paramsBuilder);
      if (parsedQuery != null) {
        totalSQLBuilder.append(" WHERE ").append(parsedQuery);
      }
    }

    try (PreparedStatement preparedStatement =
        buildPreparedStatement(totalSQLBuilder.toString(), paramsBuilder.build())) {
      ResultSet resultSet = preparedStatement.executeQuery();
      while (resultSet.next()) {
        count = resultSet.getLong(1);
      }
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }
    return count;
  }

  @Override
  public boolean bulkUpsert(Map<Key, Document> documents) {
    try {
      int[] updateCounts = bulkUpsertImpl(documents);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", Arrays.toString(updateCounts));
      }

      return true;
    } catch (BatchUpdateException e) {
      LOGGER.error("BatchUpdateException bulk inserting documents.", e);
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException bulk inserting documents. SQLState: {} Error Code:{}",
          e.getSQLState(),
          e.getErrorCode(),
          e);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    }

    return false;
  }

  private int[] bulkUpsertImpl(Map<Key, Document> documents) throws SQLException, IOException {
    try (PreparedStatement preparedStatement =
        client.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      for (Map.Entry<Key, Document> entry : documents.entrySet()) {

        Key key = entry.getKey();
        String jsonString = prepareDocument(key, entry.getValue());

        preparedStatement.setString(1, key.toString());
        preparedStatement.setString(2, jsonString);
        preparedStatement.setString(3, jsonString);

        preparedStatement.addBatch();
      }

      return preparedStatement.executeBatch();
    }
  }

  @Override
  public Iterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    String query = null;
    try {
      String collect =
          documents.keySet().stream()
              .map(val -> "'" + val.toString() + "'")
              .collect(Collectors.joining(", "));

      String space = " ";
      query =
          new StringBuilder("SELECT * FROM")
              .append(space)
              .append(collectionName)
              .append(" WHERE ")
              .append(ID)
              .append(" IN ")
              .append("(")
              .append(collect)
              .append(")")
              .toString();

      PreparedStatement preparedStatement = client.prepareStatement(query);
      ResultSet resultSet = preparedStatement.executeQuery();

      // Now go ahead and bulk upsert the documents.
      int[] updateCounts = bulkUpsertImpl(documents);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", Arrays.toString(updateCounts));
      }

      return new PostgresResultIterator(resultSet);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }

    throw new IOException("Could not bulk upsert the documents.");
  }

  private String prepareDocument(Key key, Document document) throws IOException {
    String jsonString = document.toJson();

    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(jsonString);
    jsonNode.put(DOCUMENT_ID, key.toString());

    return MAPPER.writeValueAsString(jsonNode);
  }

  private String getInsertSQL() {
    return String.format(
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb)",
        collectionName, ID, DOCUMENT, ID, DOCUMENT);
  }

  private String getUpdateSQL() {
    return String.format(
        "UPDATE %s SET (%s, %s) = ( ?, ? :: jsonb) ", collectionName, ID, DOCUMENT, ID, DOCUMENT);
  }

  private String getUpsertSQL() {
    return String.format(
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb) ON CONFLICT(%s) DO UPDATE SET %s = ?::jsonb ",
        collectionName, ID, DOCUMENT, ID, DOCUMENT);
  }

  @Override
  public void drop() {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try (PreparedStatement preparedStatement = client.prepareStatement(dropTableSQL)) {
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
  }

  static class PostgresResultIterator implements Iterator {

    private final ObjectMapper MAPPER = new ObjectMapper();
    private ResultSet resultSet;
    private boolean cursorMovedForward = false;
    private boolean hasNext = false;

    public PostgresResultIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    @Override
    public boolean hasNext() {
      try {
        if (!cursorMovedForward) {
          hasNext = resultSet.next();
          cursorMovedForward = true;
        }
        return hasNext;
      } catch (SQLException e) {
        LOGGER.error("SQLException iterating documents.", e);
      }
      return false;
    }

    @Override
    public Document next() {
      try {
        if (!cursorMovedForward) {
          resultSet.next();
        }
        // reset the cursorMovedForward state, if it was forwarded in hasNext.
        cursorMovedForward = false;
        return prepareDocument();
      } catch (IOException | SQLException e) {
        return JSONDocument.errorDocument(e.getMessage());
      }
    }

    private Document prepareDocument() throws SQLException, JsonProcessingException, IOException {
      String documentString = resultSet.getString(DOCUMENT);
      ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(documentString);
      jsonNode.remove(DOCUMENT_ID);
      // Add Timestamps to Document
      Timestamp createdAt = resultSet.getTimestamp(CREATED_AT);
      Timestamp updatedAt = resultSet.getTimestamp(UPDATED_AT);
      jsonNode.put(CREATED_AT, String.valueOf(createdAt));
      jsonNode.put(UPDATED_AT, String.valueOf(updatedAt));

      return new JSONDocument(MAPPER.writeValueAsString(jsonNode));
    }

    private String prepareNumericBlock(String fieldName, Object value) {
      if (value instanceof Number) {
        String fmt = "case jsonb_typeof(%s)\n" + "WHEN ‘number’ THEN (%s)::numeric > ?\n" + "end";
      } else if (value instanceof Boolean) {
        String fmtBoolean =
            "case jsonb_typeof(<field>)\n"
                + "WHEN 'boolean'  THEN (<field>)::boolean  > ?\n"
                + "end";
      }
      return null;
    }
  }
}
