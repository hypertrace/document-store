package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest.Operation;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides {@link Collection} implementation on Postgres using jsonb format
 */
public class PostgresCollection implements Collection {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresCollection.class);
  public static final String ID = "id";
  public static final String DOCUMENT_ID = "_id";
  public static final String DOCUMENT = "document";
  public static final String UPDATED_AT = "updated_at";
  public static final String CREATED_AT = "created_at";
  public static final String DOC_PATH_SEPARATOR = "\\.";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final CloseableIterator<Document> EMPTY_ITERATOR = createEmptyIterator();

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

  /**
   * create a new document if one doesn't exists with key
   */
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

    List<BulkUpdateRequest> requestsWithFilter =
        bulkUpdateRequests.stream()
            .filter(req -> req.getFilter() != null)
            .collect(Collectors.toList());

    List<BulkUpdateRequest> requestsWithoutFilter =
        bulkUpdateRequests.stream()
            .filter(req -> req.getFilter() == null)
            .collect(Collectors.toList());

    try {
      long totalUpdateCountA = bulkUpdateRequestsWithFilter(requestsWithFilter);

      long totalUpdateCountB = bulkUpdateRequestsWithoutFilter(requestsWithoutFilter);

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Write results for whole bulkUpdate {}", totalUpdateCountA + totalUpdateCountB);
      }

      return new BulkUpdateResult(totalUpdateCountA + totalUpdateCountB);

    } catch (IOException e) {
      LOGGER.error(
          "Exception while executing bulk update, total docs updated {}",
          e.getMessage(),
          e.getCause());
      throw e;
    }
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
  public BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request)
      throws IOException {
    Operation operation = request.getOperation();
    String jsonSubDocPath = getJsonSubDocPath(request.getSubDocPath());
    switch (operation) {
      case ADD:
        return addImpl(request, jsonSubDocPath);
      case SET:
        return setImpl(request, jsonSubDocPath);
      case REMOVE:
        return removeImpl(request, jsonSubDocPath);
      default:
        throw new UnsupportedOperationException("Not supported!");
    }
  }

  private BulkUpdateResult removeImpl(BulkArrayValueUpdateRequest request, String jsonSubDocPath) {
    //implementation
    return null;
  }

  private BulkUpdateResult addImpl(BulkArrayValueUpdateRequest request, String jsonSubDocPath)
      throws IOException {
    int totalUpdates = 0;
    String query = getBulkArrayUpdateAddQuery(request);
    jsonSubDocPath = getJsonSubDocPathForArrayInsertion(jsonSubDocPath, 0);
    try (PreparedStatement preparedStatement =
        client.prepareStatement(query,
            Statement.RETURN_GENERATED_KEYS)) {
      for (Key key : request.getKeys()) {
        int paramCount = 0;
        for (Document subDoc : request.getSubDocuments()) {
          preparedStatement.setString(++paramCount, jsonSubDocPath);
          preparedStatement.setString(++paramCount, subDoc.toJson());
        }
        preparedStatement.setString(++paramCount, key.toString());
        preparedStatement.addBatch();
      }
      int[] res = preparedStatement.executeBatch();

      totalUpdates = Arrays.stream(res).sum();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write results, total updates: {}", totalUpdates);
      }

      return new BulkUpdateResult(totalUpdates);
    } catch (BatchUpdateException e) {
      totalUpdates = Arrays.stream(e.getUpdateCounts()).filter(count -> count >= 0).sum();
      throw new IOException(
          "Exception occurred while executing batch update, total updates: " + totalUpdates, e);
    } catch (SQLException e) {
      throw new IOException(
          "Exception occurred while executing batch update, total updates: " + totalUpdates, e);
    }
  }

  private String getJsonSubDocPathForArrayInsertion(String jsonSubDocPath, int indexToInsertAt) {
    return jsonSubDocPath.substring(0, jsonSubDocPath.length() - 1) + "," + indexToInsertAt + "}";
  }

  private String getBulkArrayUpdateAddQuery(BulkArrayValueUpdateRequest request)
      throws JsonProcessingException {
    //we basically created a nested query, one query for each document
    StringBuilder sb = new StringBuilder("UPDATE %s SET %s=");
    //the outermost insert
    String finalClause = "jsonb_insert(%s, ?::text[], ?::jsonb)";
    String nestedClause;

    String[] paths = request.getSubDocPath().split("\\.");
    // if the JSON field is NULL, then SET the entire path
    StringBuilder sb2 =
        new StringBuilder("jsonb_insert(CASE WHEN ")
            .append("document")
            .append(" is NULL THEN '")
            .append(buildNestedJsonDocFromPath(paths, 0))
            .append("'");
    for (int i = 0; i < paths.length; i++) {
      // Now do the same for each nested field. If it is NULL, create it. Do it till the entire path
      // is created.
      sb2.append(" WHEN ")
          .append("document")
          .append("->")
          .append(getSubPathTillIdxForDereference(paths, i + 1))
          .append(" IS NULL THEN jsonb_set(")
          .append("document")
          .append(",")
          .append("'{")
          .append(getCSVSubPathTillIdx(paths, i + 1))
          .append("}'")
          .append(",")
          .append("'")
          .append(buildNestedJsonDocFromPath(paths, i + 1))
          .append("'")
          .append(")");
    }
    // finally, once we are sure that the path exists, set the new values
    sb2.append(" ELSE ")
        .append("document")
        .append(" END ")
        .append(",")
        .append("?::text[], ?::jsonb)");

    nestedClause = String.format(sb2.toString(), collectionName, ID);

    for (int i = 0; i < request.getSubDocuments().size() - 1; i++) {
      finalClause = String.format(finalClause, nestedClause);
    }
    return String.format(
        sb.append(finalClause).append(" WHERE %s=?").toString(),
        collectionName,
        DOCUMENT,
        ID);
  }

  private BulkUpdateResult setImpl(BulkArrayValueUpdateRequest request, String jsonSubDocPath)
      throws IOException {
    int totalUpdates = 0;
    // get documents as JSON array
    String query = getBulkArrayUpdateSetQuery(request.getSubDocPath(), DOCUMENT);
    try (PreparedStatement ps = client.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
      String docsAsJSONArray = getDocsAsJSONArray(request.getSubDocuments());
      for (Key key : request.getKeys()) {
        ps.setString(1, jsonSubDocPath);
        ps.setString(2, docsAsJSONArray);
        ps.setString(3, key.toString());
        ps.addBatch();
      }

      int[] res = ps.executeBatch();

      totalUpdates = Arrays.stream(res).sum();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write results, total updates: {}", totalUpdates);
      }

      return new BulkUpdateResult(totalUpdates);

    } catch (BatchUpdateException e) {
      totalUpdates = Arrays.stream(e.getUpdateCounts()).filter(count -> count >= 0).sum();
      throw new IOException(
          "Exception occurred while executing batch update, total updates: " + totalUpdates, e);
    } catch (SQLException e) {
      throw new IOException(
          "Exception occurred while executing batch update, total updates: " + totalUpdates, e);
    }
  }

  @Override
  public CloseableIterator<Document> search(Query query) {
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

    return EMPTY_ITERATOR;
  }

  @Override
  public CloseableIterator<Document> find(
      final org.hypertrace.core.documentstore.query.Query query) {
    return executeQueryV1(query);
  }

  @Override
  public CloseableIterator<Document> aggregate(
      final org.hypertrace.core.documentstore.query.Query query) {
    return executeQueryV1(query);
  }

  @Override
  public long count(org.hypertrace.core.documentstore.query.Query query) {
    throw new UnsupportedOperationException();
  }

  private CloseableIterator<Document> executeQueryV1(
      final org.hypertrace.core.documentstore.query.Query query) {
    org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(collectionName);
    String sqlQuery = queryParser.parse(query);
    try {
      PreparedStatement preparedStatement =
          buildPreparedStatement(sqlQuery, queryParser.getParamsBuilder().build());
      ResultSet resultSet = preparedStatement.executeQuery();
      CloseableIterator closeableIterator =
          query.getSelections().size() > 0
              ? new PostgresResultIteratorWithMetaData(resultSet)
              : new PostgresResultIterator(resultSet);
      return closeableIterator;
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }
    return EMPTY_ITERATOR;
  }

  @VisibleForTesting
  protected PreparedStatement buildPreparedStatement(String sqlQuery, Params params)
      throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = client.prepareStatement(sqlQuery);
    enrichPreparedStatementWithParams(preparedStatement, params);
    return preparedStatement;
  }

  @VisibleForTesting
  protected void enrichPreparedStatementWithParams(
      PreparedStatement preparedStatement, Params params) throws RuntimeException {
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

  @VisibleForTesting
  private String getBulkArrayUpdateSetQuery(String path, String field)
      throws JsonProcessingException {
    //The query generation logic takes care of missing paths by first creating the missing path, and then doing a json_set
    String[] paths = path.split("\\.");
    // if the JSON field is NULL, then SET the entire path
    StringBuilder sb =
        new StringBuilder("UPDATE %s SET ")
            .append(field)
            .append("=jsonb_set(CASE WHEN ")
            .append(field)
            .append(" is NULL THEN '")
            .append(buildNestedJsonDocFromPath(paths, 0))
            .append("'");
    for (int i = 0; i < paths.length; i++) {
      // Now do the same for each nested field. If it is NULL, create it. Do it till the entire path
      // is created.
      sb.append(" WHEN ")
          .append(field)
          .append("->")
          .append(getSubPathTillIdxForDereference(paths, i + 1))
          .append(" IS NULL THEN jsonb_set(")
          .append(field)
          .append(",")
          //json path
          .append("'{")
          .append(getCSVSubPathTillIdx(paths, i + 1))
          .append("}'")
          .append(",")
          //actual json doc
          .append("'")
          .append(buildNestedJsonDocFromPath(paths, i + 1))
          .append("'")
          .append(")");
    }
    // finally, once we are sure that the path exists, set the new values
    sb.append(" ELSE ")
        .append("document")
        .append(" END ")
        .append(",")
        .append("?::text[], ?::jsonb) WHERE %s=?");
    return String.format(sb.toString(), collectionName, ID);
  }

  //Returns path as a CSV till the passed index
  private String getCSVSubPathTillIdx(String[] paths, int till) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < till; i++) {
      sb.append(paths[i]).append(",");
    }
    return sb.substring(0, sb.length() - 1);
  }

  // For a path: ["attributes","labels","valueList","values"], creates a path like:
  // 'attributes'->'labels'->... till the passed index
  private String getSubPathTillIdxForDereference(String[] paths, int till) {
    StringBuilder path = new StringBuilder("'" + paths[0] + "'");
    for (int i = 1; i < till; i++) {
      path.append("->").append("'").append(paths[i]).append("'");
    }
    return path.toString();
  }

  @VisibleForTesting
  private String buildNestedJsonDocFromPath(String[] path, int rootNodeIdx)
      throws JsonProcessingException {
    // starts with the root node
    ObjectNode currNode = MAPPER.createObjectNode();
    ObjectNode rootNode = currNode;
    for (int i = rootNodeIdx; i < path.length - 1; i++) {
      ObjectNode childNode = MAPPER.createObjectNode();
      String nodeName = path[i];
      currNode.put(nodeName, childNode);
      currNode = childNode;
    }
    currNode.set(path[path.length - 1], MAPPER.createArrayNode());
    return MAPPER.writeValueAsString(rootNode);
  }

  @VisibleForTesting
  private String getDocsAsJSONArray(List<Document> docs) throws JsonProcessingException {
    ArrayNode arrNode = MAPPER.createArrayNode();
    for (Document doc : docs) {
      JsonNode jsonNode = MAPPER.readTree(doc.toJson());
      arrNode.add(jsonNode);
    }
    return MAPPER.writeValueAsString(arrNode);
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
  public boolean delete(Filter filter) {
    if (filter == null) {
      throw new UnsupportedOperationException("Filter must be provided");
    }
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(collectionName);
    Params.Builder paramsBuilder = Params.newBuilder();
    String filters = PostgresQueryParser.parseFilter(filter, paramsBuilder);
    LOGGER.debug("Sending query to PostgresSQL: {} : {}", collectionName, filters);
    if (filters == null) {
      throw new UnsupportedOperationException("Parsed filter is invalid");
    }
    sqlBuilder.append(" WHERE ").append(filters);
    try {
      PreparedStatement preparedStatement =
          buildPreparedStatement(sqlBuilder.toString(), paramsBuilder.build());
      int deletedCount = preparedStatement.executeUpdate();
      return deletedCount > 0;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting documents. filter: {}", filter, e);
    }
    return false;
  }

  @Override
  public BulkDeleteResult delete(Set<Key> keys) {
    String ids =
        keys.stream().map(key -> "'" + key.toString() + "'").collect(Collectors.joining(", "));
    String space = " ";
    String deleteSQL =
        new StringBuilder("DELETE FROM")
            .append(space)
            .append(collectionName)
            .append(" WHERE ")
            .append(ID)
            .append(" IN ")
            .append("(")
            .append(ids)
            .append(")")
            .toString();
    try (PreparedStatement preparedStatement = client.prepareStatement(deleteSQL)) {
      int deletedCount = preparedStatement.executeUpdate();
      return new BulkDeleteResult(deletedCount);
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting documents. keys: {}", keys, e);
    }
    return new BulkDeleteResult(0);
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

  private long bulkUpdateRequestsWithFilter(List<BulkUpdateRequest> requests) throws IOException {
    // Note: We cannot batch statements as the filter clause can be difference for each request. So
    // we need one PreparedStatement for each request. We try to update the batch on a best-effort
    // basis. That is, if any update fails, then we still try the other ones.
    long totalRowsUpdated = 0;
    Exception sampleException = null;

    for (BulkUpdateRequest request : requests) {
      Key key = request.getKey();
      Document document = request.getDocument();
      Filter filter = request.getFilter();

      try {
        totalRowsUpdated += update(key, document, filter).getUpdatedCount();
      } catch (IOException e) {
        sampleException = e;
        LOGGER.error("SQLException updating document. key: {} content: {}", key, document, e);
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Write result for bulkUpdateWithoutFilter {}", totalRowsUpdated);
    }
    if (sampleException != null) {
      throw new IOException(String.valueOf(totalRowsUpdated), sampleException);
    }
    return totalRowsUpdated;
  }

  private long bulkUpdateRequestsWithoutFilter(List<BulkUpdateRequest> requestsWithoutFilter)
      throws IOException {
    // We can batch all requests here since the query is the same.
    long totalRowsUpdated = 0;
    try {

      PreparedStatement ps = client.prepareStatement(getUpdateSQL());

      for (BulkUpdateRequest req : requestsWithoutFilter) {
        Key key = req.getKey();
        Document document = req.getDocument();

        String jsonString = prepareDocument(key, document);
        Params.Builder paramsBuilder = Params.newBuilder();
        paramsBuilder.addObjectParam(key.toString());
        paramsBuilder.addObjectParam(jsonString);

        enrichPreparedStatementWithParams(ps, paramsBuilder.build());

        ps.addBatch();
      }

      int[] updateCounts = ps.executeBatch();

      totalRowsUpdated = Arrays.stream(updateCounts).filter(updateCount -> updateCount >= 0).sum();

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", totalRowsUpdated);
      }

      return totalRowsUpdated;

    } catch (BatchUpdateException e) {
      totalRowsUpdated =
          Arrays.stream(e.getUpdateCounts()).filter(updateCount -> updateCount >= 0).sum();
      LOGGER.error(
          "BatchUpdateException while executing batch. Total rows updated: {}",
          totalRowsUpdated,
          e);
      throw new IOException(String.valueOf(totalRowsUpdated), e);

    } catch (SQLException e) {
      LOGGER.error(
          "SQLException bulk updating documents (without filters). SQLState: {} Error Code:{}",
          e.getSQLState(),
          e.getErrorCode(),
          e);
      throw new IOException(String.valueOf(totalRowsUpdated), e);

    } catch (IOException e) {
      LOGGER.error("IOException during bulk update requests without filter", e);
      throw new IOException(String.valueOf(totalRowsUpdated), e);
    }
  }

  @Override
  public CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
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
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb) ON CONFLICT(%s) DO UPDATE SET %s = "
            + "?::jsonb ",
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

  static class PostgresResultIterator implements CloseableIterator {

    protected final ObjectMapper MAPPER = new ObjectMapper();
    protected ResultSet resultSet;
    protected boolean cursorMovedForward = false;
    protected boolean hasNext = false;

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

    protected Document prepareDocument() throws SQLException, IOException {
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

    @SneakyThrows
    @Override
    public void close() {
      resultSet.close();
    }
  }

  static class PostgresResultIteratorWithMetaData extends PostgresResultIterator {

    public PostgresResultIteratorWithMetaData(ResultSet resultSet) {
      super(resultSet);
    }

    @Override
    protected Document prepareDocument() throws SQLException, IOException {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      Map<String, Object> jsonNode = new HashMap();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = resultSetMetaData.getColumnName(i);
        String columnValue = resultSet.getString(i);
        if (StringUtils.isNotEmpty(columnValue)) {
          JsonNode leafNodeValue = MAPPER.readTree(columnValue);
          if (PostgresUtils.isEncodedNestedField(columnName)) {
            handleNestedField(
                PostgresUtils.decodeAliasForNestedField(columnName), jsonNode, leafNodeValue);
          } else {
            jsonNode.put(columnName, leafNodeValue);
          }
        }
      }
      return new JSONDocument(MAPPER.writeValueAsString(jsonNode));
    }

    private void handleNestedField(
        String columnName, Map<String, Object> rootNode, JsonNode leafNodeValue) {
      List<String> keys = PostgresUtils.splitNestedField(columnName);
      // find the leaf node or create one for adding property value
      Map<String, Object> curNode = rootNode;
      for (int l = 0; l < keys.size() - 1; l++) {
        String key = keys.get(l);
        Map<String, Object> node = (Map<String, Object>) curNode.get(key);
        if (node == null) {
          node = new HashMap<>();
          curNode.put(key, node);
        }
        curNode = node;
      }
      String leafKey = keys.get(keys.size() - 1);
      curNode.put(leafKey, leafNodeValue);
    }
  }

  private static CloseableIterator<Document> createEmptyIterator() {
    return new CloseableIterator<>() {
      @Override
      public void close() {
        // empty iterator
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Document next() {
        throw new NoSuchElementException();
      }
    };
  }
}
