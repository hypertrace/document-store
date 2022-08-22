package org.hypertrace.core.documentstore.postgres;

import com.fasterxml.jackson.core.JsonParseException;
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
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
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
import org.hypertrace.core.documentstore.commons.DocStoreConstants;
import org.hypertrace.core.documentstore.postgres.internal.BulkUpdateSubDocsInternalResult;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
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
  private static final CloseableIterator<Document> EMPTY_ITERATOR = createEmptyIterator();

  private final PostgresClient client;
  private final String collectionName;

  public PostgresCollection(PostgresClient client, String collectionName) {
    this.client = client;
    this.collectionName = collectionName;
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
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

    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            buildPreparedStatement(
                connection, upsertQueryBuilder.toString(), paramsBuilder.build())) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getInsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
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
    long updatedCount = -1;
    boolean result = updateSubDocInternal(key, subDocPath, subDocument);
    if (result) {
      updatedCount = updateLastModifiedTime(Set.of(key));
      if (updatedCount < 1) {
        LOGGER.error("Failed in modifying lastUpdatedTime for key:{}", key);
      }
    }
    return updatedCount == 1;
  }

  private boolean updateSubDocInternal(Key key, String subDocPath, Document subDocument) {
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s=?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = getJsonSubDocPath(subDocPath);
    String jsonString = subDocument.toJson();

    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
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
    BulkUpdateSubDocsInternalResult updated = bulkUpdateSubDocsInternal(documents);
    long partiallyUpdated = updated.getTotalUpdateCount();
    long fullyUpdated = updateLastModifiedTime(updated.getUpdatedDocuments());
    if (fullyUpdated != partiallyUpdated) {
      LOGGER.error(
          "Not all documents fully updated, documents fullyUpdated:{}, partiallyUpdated:{}, expected:{}",
          fullyUpdated,
          partiallyUpdated,
          documents.size());
    }
    return new BulkUpdateResult(fullyUpdated);
  }

  private BulkUpdateSubDocsInternalResult bulkUpdateSubDocsInternal(
      Map<Key, Map<String, Document>> documents) throws Exception {
    List<Key> orderList = new ArrayList<>();
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s = ?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(updateSubDocSQL)) {
      for (Key key : documents.keySet()) {
        orderList.add(key);
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
      Set<Key> updatedDocs = new HashSet<>();
      long totalUpdateCount = 0;
      for (int i = 0; i < updateCounts.length; i++) {
        if (updateCounts[i] > 0) {
          updatedDocs.add(orderList.get(i));
        }
        totalUpdateCount += updateCounts[i];
      }
      return new BulkUpdateSubDocsInternalResult(updatedDocs, totalUpdateCount);
    } catch (SQLException e) {
      LOGGER.error("SQLException updating sub document.", e);
      throw e;
    }
  }

  @Override
  public BulkUpdateResult bulkOperationOnArrayValue(BulkArrayValueUpdateRequest request)
      throws Exception {
    Set<JsonNode> subDocs = new HashSet<>();
    for (Document subDoc : request.getSubDocuments()) {
      subDocs.add(getDocAsJSON(subDoc));
    }
    Map<String, String> idToTenantIdMap = getDocIdToTenantIdMap(request);
    CloseableIterator<Document> docs = searchDocsForKeys(request.getKeys());
    switch (request.getOperation()) {
      case ADD:
        return bulkAddOnArrayValue(request.getSubDocPath(), idToTenantIdMap, subDocs, docs);
      case SET:
        return bulkSetOnArrayValue(request.getSubDocPath(), idToTenantIdMap, subDocs, docs);
      case REMOVE:
        return bulkRemoveOnArrayValue(request.getSubDocPath(), idToTenantIdMap, subDocs, docs);
      default:
        throw new UnsupportedOperationException("Unsupported operation: " + request.getOperation());
    }
  }

  @Override
  public CloseableIterator<Document> search(Query query) {
    String selection = PostgresQueryParser.parseSelections(query.getSelections());
    StringBuilder sqlBuilder =
        new StringBuilder(String.format("SELECT %s FROM ", selection)).append(collectionName);

    String filters = null;
    Params.Builder paramsBuilder = Params.newBuilder();

    // If there is a filter in the query, parse it fully.
    if (query.getFilter() != null) {
      filters = PostgresQueryParser.parseFilter(query.getFilter(), paramsBuilder);
    }

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

    String pgSqlQuery = sqlBuilder.toString();
    try {
      Connection connection = client.getConnection();
      PreparedStatement preparedStatement =
          buildPreparedStatement(connection, pgSqlQuery, paramsBuilder.build());
      ResultSet resultSet = preparedStatement.executeQuery();
      CloseableIterator closeableIterator =
          query.getSelections().size() > 0
              ? new PostgresResultIteratorWithMetaData(connection, preparedStatement, resultSet)
              : new PostgresResultIterator(connection, preparedStatement, resultSet);
      return closeableIterator;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException in querying documents - query: {}, sqlQuery:{}", query, pgSqlQuery, e);
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
    org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            collectionName, query);
    String subQuery = queryParser.parse();
    String sqlQuery = String.format("SELECT COUNT(*) FROM (%s) p(count)", subQuery);
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            buildPreparedStatement(connection, sqlQuery, queryParser.getParamsBuilder().build());
        ResultSet resultSet = preparedStatement.executeQuery()) {
      resultSet.next();
      return resultSet.getLong(1);
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException querying documents. original query: {}, sql query: {}", query, sqlQuery, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean delete(Key key) {
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", collectionName, ID);
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL)) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            buildPreparedStatement(connection, sqlBuilder.toString(), paramsBuilder.build())) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL)) {
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

    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(deleteSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(deleteSQL)) {
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
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(countSQL);
        ResultSet resultSet = preparedStatement.executeQuery()) {
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

    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            buildPreparedStatement(connection, totalSQLBuilder.toString(), paramsBuilder.build());
        ResultSet resultSet = preparedStatement.executeQuery()) {
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

  @Override
  public CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    String query = null;
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

    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(query);
        ResultSet resultSet = preparedStatement.executeQuery()) {

      // Now go ahead and bulk upsert the documents.
      int[] updateCounts = bulkUpsertImpl(documents);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Write result: {}", Arrays.toString(updateCounts));
      }

      return new PostgresResultIterator(connection, preparedStatement, resultSet);
    } catch (IOException e) {
      LOGGER.error("SQLException bulk inserting documents. documents: {}", documents, e);
    } catch (SQLException e) {
      LOGGER.error("SQLException querying documents. query: {}", query, e);
    }

    throw new IOException("Could not bulk upsert the documents.");
  }

  @VisibleForTesting
  protected PreparedStatement buildPreparedStatement(
      Connection connection, String sqlQuery, Params params) throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
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

  @Override
  public void drop() {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", collectionName);
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement(dropTableSQL)) {
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", collectionName);
    }
  }

  private Map<String, String> getDocIdToTenantIdMap(BulkArrayValueUpdateRequest request) {
    return request.getKeys().stream()
        .collect(
            Collectors.toMap(
                key -> key.toString().split(":")[1], key -> key.toString().split(":")[0]));
  }

  private BulkUpdateResult bulkSetOnArrayValue(
      String subDocPath,
      Map<String, String> idToTenantIdMap,
      Set<JsonNode> subDocs,
      Iterator<Document> docs)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      // create path if missing
      ArrayNode candidateArray = (ArrayNode) getJsonNodeAtPath(subDocPath, docRoot, true);
      candidateArray.removeAll();
      candidateArray.addAll(subDocs);
      String id = docRoot.findValue(ID).asText();
      if (docRoot.isObject()) {
        ((ObjectNode) docRoot).put(DocStoreConstants.LAST_UPDATED_TIME, System.currentTimeMillis());
      }
      upsertMap.put(new SingleValueKey(idToTenantIdMap.get(id), id), new JSONDocument(docRoot));
    }
    return upsertDocs(upsertMap);
  }

  private BulkUpdateResult bulkRemoveOnArrayValue(
      String subDocPath,
      Map<String, String> idToTenantIdMap,
      Set<JsonNode> subDocs,
      Iterator<Document> docs)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      JsonNode currNode;
      currNode = getJsonNodeAtPath(subDocPath, docRoot, false);
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

  private BulkUpdateResult bulkAddOnArrayValue(
      String subDocPath,
      Map<String, String> idToTenantIdMap,
      Set<JsonNode> subDocs,
      Iterator<Document> docs)
      throws IOException {
    Map<Key, Document> upsertMap = new HashMap<>();
    while (docs.hasNext()) {
      JsonNode docRoot = getDocAsJSON(docs.next());
      // create path if missing
      ArrayNode candidateArray = (ArrayNode) getJsonNodeAtPath(subDocPath, docRoot, true);
      Set<JsonNode> existingElems = new HashSet<>();
      candidateArray.forEach(existingElems::add);
      existingElems.addAll(subDocs);
      candidateArray.removeAll();
      candidateArray.addAll(existingElems);
      String id = docRoot.findValue(ID).asText();
      if (docRoot.isObject()) {
        ((ObjectNode) docRoot).put(DocStoreConstants.LAST_UPDATED_TIME, System.currentTimeMillis());
      }
      upsertMap.put(new SingleValueKey(idToTenantIdMap.get(id), id), new JSONDocument(docRoot));
    }
    return upsertDocs(upsertMap);
  }

  private Optional<Long> getCreatedTime(Key key) throws IOException {
    CloseableIterator<Document> iterator = searchDocsForKeys(Set.of(key));
    if (iterator.hasNext()) {
      JsonNode existingDocument = getDocAsJSON(iterator.next());
      if (existingDocument.has(DocStoreConstants.CREATED_TIME)) {
        return Optional.of(existingDocument.get(DocStoreConstants.CREATED_TIME).asLong());
      }
    }
    return Optional.empty();
  }

  private CloseableIterator<Document> searchDocsForKeys(Set<Key> keys) {
    List<String> keysAsStr = keys.stream().map(Key::toString).collect(Collectors.toList());
    Query query = new Query().withFilter(new Filter(Filter.Op.IN, ID, keysAsStr));
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

  private CloseableIterator<Document> executeQueryV1(
      final org.hypertrace.core.documentstore.query.Query query) {
    org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            collectionName, query);
    String sqlQuery = queryParser.parse();
    try {
      Connection connection = client.getConnection();
      PreparedStatement preparedStatement =
          buildPreparedStatement(connection, sqlQuery, queryParser.getParamsBuilder().build());
      LOGGER.debug("Executing executeQueryV1 sqlQuery:{}", preparedStatement.toString());
      ResultSet resultSet = preparedStatement.executeQuery();
      CloseableIterator closeableIterator =
          query.getSelections().size() > 0
              ? new PostgresResultIteratorWithMetaData(connection, preparedStatement, resultSet)
              : new PostgresResultIterator(connection, preparedStatement, resultSet);
      return closeableIterator;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException querying documents. original query: {}, sql query:", query, sqlQuery, e);
      throw new RuntimeException(e);
    }
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

  private int[] bulkUpsertImpl(Map<Key, Document> documents) throws SQLException, IOException {
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
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

  @VisibleForTesting
  JsonNode getJsonNodeAtPath(String path, JsonNode rootNode, boolean createPathIfMissing) {
    if (StringUtils.isEmpty(path)) {
      return rootNode;
    }
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
    try (Connection connection = client.getConnection();
        PreparedStatement ps = connection.prepareStatement(getUpdateSQL())) {

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

  private String prepareDocument(Key key, Document document) throws IOException {
    String jsonString = document.toJson();

    ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(jsonString);
    jsonNode.put(DOCUMENT_ID, key.toString());

    // update time fields
    long now = System.currentTimeMillis();
    Optional<Long> existingCreatedTime = getCreatedTime(key);
    jsonNode.put(DocStoreConstants.CREATED_TIME, existingCreatedTime.orElse(now));
    jsonNode.put(DocStoreConstants.LAST_UPDATED_TIME, now);

    return MAPPER.writeValueAsString(jsonNode);
  }

  private long updateLastModifiedTime(Set<Key> keys) {
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, '{lastUpdatedTime}'::text[], ?::jsonb) WHERE %s=?",
            collectionName, DOCUMENT, DOCUMENT, ID);
    long now = System.currentTimeMillis();
    try (Connection connection = client.getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
      for (Key key : keys) {
        preparedStatement.setString(1, String.valueOf(now));
        preparedStatement.setString(2, key.toString());
        preparedStatement.addBatch();
      }

      int[] updateCounts = preparedStatement.executeBatch();
      int totalUpdateCount = 0;
      for (int update : updateCounts) {
        totalUpdateCount += update;
      }
      return totalUpdateCount;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException updating sub document. keys: {} subDocPath: {lastUpdatedTime}", keys, e);
    }
    return -1;
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

  static class PostgresResultIterator implements CloseableIterator {

    protected final ObjectMapper MAPPER = new ObjectMapper();
    private final Connection connection;
    private final PreparedStatement preparedStatement;
    protected ResultSet resultSet;
    protected boolean cursorMovedForward = false;
    protected boolean hasNext = false;

    public PostgresResultIterator(
        Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
      this.connection = connection;
      this.preparedStatement = preparedStatement;
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
      preparedStatement.close();
      connection.close();
    }
  }

  static class PostgresResultIteratorWithMetaData extends PostgresResultIterator {

    public PostgresResultIteratorWithMetaData(
        Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
      super(connection, preparedStatement, resultSet);
    }

    @Override
    protected Document prepareDocument() throws SQLException, IOException {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      Map<String, Object> jsonNode = new HashMap();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = resultSetMetaData.getColumnName(i);
        String columnValue = getColumnValue(resultSetMetaData, columnName, i);
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

    private String getColumnValue(
        ResultSetMetaData resultSetMetaData, String columnName, int columnIndex)
        throws SQLException, JsonProcessingException {
      int columnType = resultSetMetaData.getColumnType(columnIndex);
      // check for array
      if (columnType == Types.ARRAY) {
        return MAPPER.writeValueAsString(resultSet.getArray(columnIndex).getArray());
      }

      // check for ID column
      if (columnName.equals(ID)) {
        return MAPPER.writeValueAsString(resultSet.getString(columnIndex));
      }

      // rest of the columns
      return resultSet.getString(columnIndex);
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
