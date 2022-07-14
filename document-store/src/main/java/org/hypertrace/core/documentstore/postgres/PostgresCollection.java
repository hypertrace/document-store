package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.core.JsonParseException;
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
import java.util.Iterator;
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
import org.hypertrace.core.documentstore.SingleValueKey;
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
      throws Exception {
    Operation operation = request.getOperation();
    switch (operation) {
      // todo: All of these implementations do a GET -> in memory update -> UPSERT which is not
      // optimal. Changes this logic to use a single query using jsob_set and json_insert
      case ADD:
        return bulkAddOnArrayValue(request);
      case SET:
        return bulkSetOnArrayValue(request);
      case REMOVE:
        return bulkRemoveOnArrayValue(request);
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + operation);
    }
  }

  private BulkUpdateResult bulkRemoveOnArrayValue(BulkArrayValueUpdateRequest request)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    Map<String, String> idToTenantIdMap = getDocIdToTenantIdMap(request);
    HashSet<JsonNode> subDocs = new HashSet<>();
    for (Document subDoc : request.getSubDocuments()) {
      subDocs.add(getDocAsJSON(subDoc));
    }
    Iterator<Document> docs = searchDocsForKeys(request.getKeys());
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      JsonNode currNode;
      currNode = getJsonNodeAtPath(request.getSubDocPath(), docRoot, false);
      if (currNode == null) {
        LOGGER.warn(
            "Removal path does not exist for {}, continuing with other documents",
            docRoot.get(ID).textValue());
        continue;
      }
      HashSet<JsonNode> existingItems = new HashSet<>();
      ArrayNode candidateArray = (ArrayNode) currNode;
      candidateArray.forEach(existingItems::add);
      existingItems.removeAll(subDocs);
      candidateArray.removeAll();
      candidateArray.addAll(existingItems);
      String id = docRoot.findValue(ID).asText();
      upsertMap.put(new SingleValueKey(idToTenantIdMap.get(id), id), new JSONDocument(docRoot));
    }
    return upsertDocs(upsertMap);
  }

  private Map<String, String> getDocIdToTenantIdMap(BulkArrayValueUpdateRequest request) {
    return request.getKeys().stream()
        .collect(
            Collectors.toMap(
                key -> key.toString().split(":")[1], key -> key.toString().split(":")[0]));
  }

  private BulkUpdateResult bulkAddOnArrayValue(BulkArrayValueUpdateRequest request)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    Map<String, String> idToTenantIdMap = getDocIdToTenantIdMap(request);
    Set<JsonNode> subDocs = new HashSet<>();
    for (Document subDoc : request.getSubDocuments()) {
      subDocs.add(getDocAsJSON(subDoc));
    }
    Iterator<Document> docs = searchDocsForKeys(request.getKeys());
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      // create path if missing
      JsonNode arrayNode = getJsonNodeAtPath(request.getSubDocPath(), docRoot, true);
      ArrayNode candidateArray = (ArrayNode) arrayNode;
      assert candidateArray != null;
      Iterator<JsonNode> iterator = candidateArray.iterator();
      Set<JsonNode> existingElems = new HashSet<>();
      while (iterator.hasNext()) {
        existingElems.add(iterator.next());
      }
      existingElems.addAll(subDocs);
      candidateArray.removeAll();
      candidateArray.addAll(existingElems);
      String id = docRoot.findValue(ID).asText();
      upsertMap.put(new SingleValueKey(idToTenantIdMap.get(id), id), new JSONDocument(docRoot));
    }
    return upsertDocs(upsertMap);
  }

  private CloseableIterator<Document> searchDocsForKeys(Set<Key> keys) {
    List<String> keysAsStr = keys.stream().map(Key::toString).collect(Collectors.toList());
    Query query =
        new Query().withSelection("*").withFilter(new Filter(Filter.Op.IN, ID, keysAsStr));
    return search(query);
  }

  private JsonNode getDocAsJSON(Document subDoc) throws IOException {
    JsonNode subDocAsJSON;
    try {
      subDocAsJSON = MAPPER.readTree(subDoc.toJson());
    } catch (JsonParseException e) {
      LOGGER.error("Malformed subDoc JSON, cannot deserialize");
      throw e;
    }
    return subDocAsJSON;
  }

  private BulkUpdateResult upsertDocs(Map<Key, Document> docs) throws IOException {
    try {
      int[] res = bulkUpsertImpl(docs);
      return new BulkUpdateResult(Arrays.stream(res).sum());
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException bulk updating documents (without filters). SQLState: {} Error Code:{}",
          e.getSQLState(),
          e.getErrorCode(),
          e);
      throw new IOException(e);
    }
  }

  private BulkUpdateResult bulkSetOnArrayValue(BulkArrayValueUpdateRequest request)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    Map<String, String> idToTenantIdMap = getDocIdToTenantIdMap(request);
    Set<JsonNode> subDocs = new HashSet<>();
    for (Document subDoc : request.getSubDocuments()) {
      subDocs.add(getDocAsJSON(subDoc));
    }
    Iterator<Document> docs = searchDocsForKeys(request.getKeys());
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      // create path if missing
      JsonNode arrayNode = getJsonNodeAtPath(request.getSubDocPath(), docRoot, true);
      assert arrayNode != null;
      ArrayNode candidateArray = (ArrayNode) arrayNode;
      candidateArray.removeAll();
      candidateArray.addAll(subDocs);
      String id = docRoot.findValue(ID).asText();
      upsertMap.put(new SingleValueKey(idToTenantIdMap.get(id), id), new JSONDocument(docRoot));
    }
    return upsertDocs(upsertMap);
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
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            collectionName, query);
    String sqlQuery = queryParser.parse();
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
      LOGGER.error(
          "SQLException querying documents. original query: {}, sql query:", query, sqlQuery, e);
      throw new RuntimeException(e);
    }
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

  private JsonNode getJsonNodeAtPath(String path, JsonNode rootNode, boolean createPathIfMissing) {
    String[] pathTokens = path.split("\\.");
    // create path if missing
    // attributes.labels.valueList.values
    JsonNode currNode = rootNode;
    for (int i = 0; i < pathTokens.length; i++) {
      if (currNode.path(pathTokens[i]).isMissingNode()) {
        if (createPathIfMissing) {
          if (i == pathTokens.length - 1) {
            ((ObjectNode) currNode).set(pathTokens[i], MAPPER.createArrayNode());
          } else {
            ((ObjectNode) currNode).set(pathTokens[i], MAPPER.createObjectNode());
          }
          currNode = currNode.path(pathTokens[i]);
        } else {
          return null;
        }
      } else {
        currNode = currNode.path(pathTokens[i]);
      }
    }
    return currNode;
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
