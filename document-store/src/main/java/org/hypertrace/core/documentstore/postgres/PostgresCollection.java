package org.hypertrace.core.documentstore.postgres;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Optional.empty;
import static org.hypertrace.core.documentstore.commons.DocStoreConstants.IMPLICIT_ID;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.OUTER_COLUMNS;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.extractAndRemoveId;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.postgresql.util.PSQLState.UNIQUE_VIOLATION;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigInteger;
import java.sql.Array;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.commons.CommonUpdateValidator;
import org.hypertrace.core.documentstore.commons.DocStoreConstants;
import org.hypertrace.core.documentstore.commons.UpdateValidator;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.internal.BulkUpdateSubDocsInternalResult;
import org.hypertrace.core.documentstore.postgres.model.DocumentAndId;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentUpdater;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.postgresql.util.PSQLException;
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
  private static final String CREATED_NOW_ALIAS = "created_now_alias";
  private static final CloseableIterator<Document> EMPTY_ITERATOR =
      CloseableIterator.emptyIterator();
  private static final String FLAT_STRUCTURE_COLLECTION_KEY = "flatStructureCollection";

  private final PostgresClient client;
  private final PostgresTableIdentifier tableIdentifier;
  private final PostgresSubDocumentUpdater subDocUpdater;
  private final PostgresQueryExecutor queryExecutor;
  private final UpdateValidator updateValidator;

  public PostgresCollection(final PostgresClient client, final String collectionName) {
    this(client, PostgresTableIdentifier.parse(collectionName));
  }

  PostgresCollection(final PostgresClient client, final PostgresTableIdentifier tableIdentifier) {
    this.client = client;
    this.tableIdentifier = tableIdentifier;
    this.subDocUpdater =
        new PostgresSubDocumentUpdater(new PostgresQueryBuilder(this.tableIdentifier));
    this.queryExecutor = new PostgresQueryExecutor(this.tableIdentifier);
    this.updateValidator = new CommonUpdateValidator();
  }

  @Override
  public boolean upsert(Key key, Document document) throws IOException {
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
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
        upsertQueryBuilder
            .append(" WHERE ")
            .append(conditionQuery)
            .append(" AND id = '")
            .append(key)
            .append("'");
      }
    }

    try (PreparedStatement preparedStatement =
        queryExecutor.buildPreparedStatement(
            upsertQueryBuilder.toString(), paramsBuilder.build(), client.getConnection())) {
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

  @Override
  public CreateResult create(Key key, Document document) throws IOException {
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(getInsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
      String jsonString = prepareDocument(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      int result = preparedStatement.executeUpdate();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Create result: {}", result);
      }
      return new CreateResult(result > 0);
    } catch (final PSQLException e) {
      if (UNIQUE_VIOLATION.getState().equals(e.getSQLState())) {
        throw new DuplicateDocumentException();
      }

      LOGGER.error("SQLException creating document. key: " + key + " content: " + document, e);
      throw new IOException(e);
    } catch (SQLException e) {
      LOGGER.error("SQLException creating document. key: " + key + " content: " + document, e);
      throw new IOException(e);
    }
  }

  @Override
  public boolean createOrReplace(final Key key, final Document document) throws IOException {
    try (final PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(getCreateOrReplaceSQL())) {
      final String jsonString = prepareDocumentForCreation(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);

      final ResultSet resultSet = preparedStatement.executeQuery();

      if (!resultSet.next()) {
        throw new IOException("Unexpected response from Postgres DB");
      }

      final boolean result = resultSet.getBoolean(CREATED_NOW_ALIAS);
      LOGGER.debug("Create or replace result: {}", result);
      return result;
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException inserting/replacing document. key: {} content:{}", key, document, e);
      throw new IOException(e);
    }
  }

  @Override
  public Document createOrReplaceAndReturn(final Key key, final Document document)
      throws IOException {
    try (final PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(getCreateOrReplaceAndReturnSQL())) {
      final String jsonString = prepareDocumentForCreation(key, document);
      preparedStatement.setString(1, key.toString());
      preparedStatement.setString(2, jsonString);
      preparedStatement.setString(3, jsonString);

      final PostgresResultIterator iterator =
          new PostgresResultIterator(preparedStatement.executeQuery());

      if (!iterator.hasNext()) {
        throw new IOException("Unexpected response from Postgres DB");
      }

      return iterator.next();
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException inserting/replacing document. key: {} content:{}", key, document, e);
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
    String updateSubDocSQL = getSubDocUpdateQuery();
    String jsonSubDocPath = formatSubDocPath(subDocPath);
    String jsonString = subDocument.toJson();

    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
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
      LOGGER.warn(
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
            tableIdentifier, DOCUMENT, DOCUMENT, ID);
    try {
      PreparedStatement preparedStatement =
          client.getConnection().prepareStatement(updateSubDocSQL);
      for (Key key : documents.keySet()) {
        Map<String, Document> subDocuments = documents.get(key);
        for (String subDocPath : subDocuments.keySet()) {
          orderList.add(key);
          Document subDocument = subDocuments.get(subDocPath);
          String jsonSubDocPath = formatSubDocPath(subDocPath);
          String jsonString = subDocument.toJson();
          preparedStatement.setString(1, jsonSubDocPath);
          preparedStatement.setString(2, jsonString);
          preparedStatement.setString(3, key.toString());
          preparedStatement.addBatch();
        }
      }
      int[] updateCounts = preparedStatement.executeBatch();
      Set<Key> updatedDocs = new HashSet<>();
      for (int i = 0; i < updateCounts.length; i++) {
        if (updateCounts[i] > 0) {
          updatedDocs.add(orderList.get(i));
        }
      }
      return new BulkUpdateSubDocsInternalResult(updatedDocs, updatedDocs.size());
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
    return search(query, true);
  }

  private CloseableIterator<Document> search(Query query, boolean removeDocumentId) {
    String selection = PostgresQueryParser.parseSelections(query.getSelections());
    StringBuilder sqlBuilder =
        new StringBuilder(String.format("SELECT %s FROM ", selection)).append(tableIdentifier);

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
      PreparedStatement preparedStatement =
          queryExecutor.buildPreparedStatement(
              pgSqlQuery, paramsBuilder.build(), client.getConnection());
      LOGGER.debug("Executing search query to PostgresSQL:{}", preparedStatement.toString());
      ResultSet resultSet = preparedStatement.executeQuery();
      CloseableIterator closeableIterator =
          query.getSelections().size() > 0
              ? new PostgresResultIteratorWithMetaData(resultSet, removeDocumentId)
              : new PostgresResultIterator(resultSet, removeDocumentId);
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
    return queryExecutor.execute(client.getConnection(), query);
  }

  @Override
  public CloseableIterator<Document> query(
      final org.hypertrace.core.documentstore.query.Query query, final QueryOptions queryOptions) {
    String flatStructureCollectionName =
        client.getCustomParameters().get(FLAT_STRUCTURE_COLLECTION_KEY);
    return queryExecutor.execute(client.getConnection(), query, flatStructureCollectionName);
  }

  @Override
  public Optional<Document> update(
      final org.hypertrace.core.documentstore.query.Query query,
      final java.util.Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException {
    updateValidator.validate(updates);

    try (final Connection connection = client.getPooledConnection()) {
      org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser parser =
          new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
              tableIdentifier, query);
      final String selectQuery = parser.buildSelectQueryForUpdate();

      try (final PreparedStatement preparedStatement =
          queryExecutor.buildPreparedStatement(
              selectQuery, parser.getParamsBuilder().build(), connection)) {
        final Optional<Document> documentOptional =
            getFirstDocument(preparedStatement.executeQuery());

        if (documentOptional.isEmpty()) {
          connection.commit();
          return empty();
        }

        final Document document = documentOptional.get();
        final DocumentAndId docAndId = extractAndRemoveId(document);
        final String id = docAndId.getId();

        subDocUpdater.executeUpdateQuery(connection, id, updates);

        final Document returnDocument;

        if (updateOptions.getReturnDocumentType() == BEFORE_UPDATE) {
          returnDocument = docAndId.getDocument();
        } else if (updateOptions.getReturnDocumentType() == AFTER_UPDATE) {
          final org.hypertrace.core.documentstore.query.Query findByIdQuery =
              org.hypertrace.core.documentstore.query.Query.builder()
                  .addSelections(query.getSelections())
                  .setFilter(
                      org.hypertrace.core.documentstore.query.Filter.builder()
                          .expression(KeyExpression.from(id))
                          .build())
                  .build();

          try (final CloseableIterator<Document> iterator =
              queryExecutor.execute(connection, findByIdQuery)) {
            returnDocument = getFirstDocument(iterator).orElseThrow();
          }
        } else if (updateOptions.getReturnDocumentType() == NONE) {
          returnDocument = null;
        } else {
          throw new UnsupportedOperationException(
              String.format(
                  "Unhandled return document type: %s", updateOptions.getReturnDocumentType()));
        }

        connection.commit();
        return Optional.ofNullable(returnDocument);
      } catch (final Exception e) {
        connection.rollback();
        throw e;
      }
    } catch (final Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public CloseableIterator<Document> bulkUpdate(
      org.hypertrace.core.documentstore.query.Query query,
      final java.util.Collection<SubDocumentUpdate> updates,
      final UpdateOptions updateOptions)
      throws IOException {
    updateValidator.validate(updates);

    CloseableIterator<Document> iterator = null;

    try {
      final ReturnDocumentType returnDocumentType = updateOptions.getReturnDocumentType();

      if (returnDocumentType == BEFORE_UPDATE) {
        iterator = aggregate(query);
      }

      final Connection connection = client.getConnection();
      subDocUpdater.executeUpdateQuery(connection, query, updates);

      switch (returnDocumentType) {
        case AFTER_UPDATE:
          return aggregate(query);

        case BEFORE_UPDATE:
          return iterator;

        case NONE:
          return CloseableIterator.emptyIterator();

        default:
          throw new UnsupportedOperationException(
              "Unsupported return document type: " + returnDocumentType);
      }
    } catch (final Exception e) {
      if (iterator != null) {
        iterator.close();
      }
      throw new IOException(e);
    }
  }

  @Override
  public long count(
      org.hypertrace.core.documentstore.query.Query query, QueryOptions queryOptions) {
    org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            tableIdentifier, query);
    String subQuery = queryParser.parse();
    String sqlQuery = String.format("SELECT COUNT(*) FROM (%s) p(count)", subQuery);
    try {
      PreparedStatement preparedStatement =
          queryExecutor.buildPreparedStatement(
              sqlQuery, queryParser.getParamsBuilder().build(), client.getConnection());
      ResultSet resultSet = preparedStatement.executeQuery();
      resultSet.next();
      return resultSet.getLong(1);
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException querying documents. original query: {}, sql query:", query, sqlQuery, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean delete(Key key) {
    String deleteSQL = String.format("DELETE FROM %s WHERE %s = ?", tableIdentifier, ID);
    try (PreparedStatement preparedStatement = client.getConnection().prepareStatement(deleteSQL)) {
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
    StringBuilder sqlBuilder = new StringBuilder("DELETE FROM ").append(tableIdentifier);
    Params.Builder paramsBuilder = Params.newBuilder();
    String filters = PostgresQueryParser.parseFilter(filter, paramsBuilder);
    LOGGER.debug("Sending query to PostgresSQL: {} : {}", tableIdentifier, filters);
    if (filters == null) {
      throw new UnsupportedOperationException("Parsed filter is invalid");
    }
    sqlBuilder.append(" WHERE ").append(filters);
    try {
      PreparedStatement preparedStatement =
          queryExecutor.buildPreparedStatement(
              sqlBuilder.toString(), paramsBuilder.build(), client.getConnection());
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
            .append(tableIdentifier)
            .append(" WHERE ")
            .append(ID)
            .append(" IN ")
            .append("(")
            .append(ids)
            .append(")")
            .toString();
    try (PreparedStatement preparedStatement = client.getConnection().prepareStatement(deleteSQL)) {
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
            "UPDATE %s SET %s=%s #- ?::text[] WHERE %s=?", tableIdentifier, DOCUMENT, DOCUMENT, ID);
    String jsonSubDocPath = formatSubDocPath(subDocPath);

    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(deleteSubDocSQL, Statement.RETURN_GENERATED_KEYS)) {
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
    String deleteSQL = String.format("DELETE FROM %s", tableIdentifier);
    try (PreparedStatement preparedStatement = client.getConnection().prepareStatement(deleteSQL)) {
      preparedStatement.executeUpdate();
      return true;
    } catch (SQLException e) {
      LOGGER.error("SQLException deleting all documents.", e);
    }
    return false;
  }

  @Override
  public long count() {
    String countSQL = String.format("SELECT COUNT(*) FROM %s", tableIdentifier);
    long count = -1;
    try (PreparedStatement preparedStatement = client.getConnection().prepareStatement(countSQL)) {
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
        new StringBuilder("SELECT COUNT(*) FROM ").append(tableIdentifier);
    Params.Builder paramsBuilder = Params.newBuilder();

    long count = -1;
    // on any in-correct filter input, it will return total without filtering
    if (query.getFilter() != null) {
      String parsedQuery = PostgresQueryParser.parseFilter(query.getFilter(), paramsBuilder);
      if (parsedQuery != null) {
        totalSQLBuilder.append(" WHERE ").append(parsedQuery);
      }
    }

    String sqlQuery = totalSQLBuilder.toString();
    Params params = paramsBuilder.build();
    LOGGER.debug("API: total, orgQuery:{}, sqlQuery:{}, params:{}", query, sqlQuery, params);

    try (PreparedStatement preparedStatement =
        queryExecutor.buildPreparedStatement(sqlQuery, params, client.getConnection())) {
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
              .append(tableIdentifier)
              .append(" WHERE ")
              .append(ID)
              .append(" IN ")
              .append("(")
              .append(collect)
              .append(")")
              .toString();

      PreparedStatement preparedStatement = client.getConnection().prepareStatement(query);
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

  @Override
  public void drop() {
    String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", tableIdentifier);
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(dropTableSQL)) {
      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Exception deleting table name: {}", tableIdentifier);
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
      String id = docRoot.findValue(DOCUMENT_ID).asText();
      if (docRoot.isObject()) {
        ((ObjectNode) docRoot).put(DocStoreConstants.LAST_UPDATED_TIME, System.currentTimeMillis());
      }
      upsertMap.put(Key.from(id), new JSONDocument(docRoot));
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
      String id = docRoot.findValue(DOCUMENT_ID).asText();
      upsertMap.put(Key.from(id), new JSONDocument(docRoot));
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
      String id = docRoot.findValue(DOCUMENT_ID).asText();
      if (docRoot.isObject()) {
        ((ObjectNode) docRoot).put(DocStoreConstants.LAST_UPDATED_TIME, System.currentTimeMillis());
      }
      upsertMap.put(Key.from(id), new JSONDocument(docRoot));
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
    return empty();
  }

  private CloseableIterator<Document> searchDocsForKeys(Set<Key> keys) {
    List<String> keysAsStr = keys.stream().map(Key::toString).collect(Collectors.toList());
    Query query = new Query().withFilter(new Filter(Filter.Op.IN, ID, keysAsStr));
    return search(query, false);
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

  private int[] bulkUpsertImpl(Map<Key, Document> documents) throws SQLException, IOException {
    try (PreparedStatement preparedStatement =
        client.getConnection().prepareStatement(getUpsertSQL(), Statement.RETURN_GENERATED_KEYS)) {
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
    try {

      PreparedStatement ps = client.getConnection().prepareStatement(getUpdateSQL());

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

  private String prepareDocumentForCreation(final Key key, final Document document)
      throws IOException {
    final String jsonString = document.toJson();

    final ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(jsonString);
    jsonNode.put(DOCUMENT_ID, key.toString());

    // update time fields
    final long now = System.currentTimeMillis();
    jsonNode.put(DocStoreConstants.CREATED_TIME, now);
    jsonNode.put(DocStoreConstants.LAST_UPDATED_TIME, now);

    return MAPPER.writeValueAsString(jsonNode);
  }

  private long updateLastModifiedTime(Set<Key> keys) {
    String updateSubDocSQL =
        String.format(
            "UPDATE %s SET %s=jsonb_set(%s, '{lastUpdatedTime}'::text[], ?::jsonb) WHERE %s=?",
            tableIdentifier, DOCUMENT, DOCUMENT, ID);
    long now = System.currentTimeMillis();
    try {
      PreparedStatement preparedStatement =
          client.getConnection().prepareStatement(updateSubDocSQL, Statement.RETURN_GENERATED_KEYS);
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

  public Optional<Document> getFirstDocument(final ResultSet resultSet) throws IOException {
    final CloseableIterator<Document> iterator = new PostgresResultIteratorWithMetaData(resultSet);
    return getFirstDocument(iterator);
  }

  public Optional<Document> getFirstDocument(final CloseableIterator<Document> iterator)
      throws IOException {
    final Optional<Document> optionalDocument =
        Optional.of(iterator).filter(Iterator::hasNext).map(Iterator::next);
    iterator.close();
    return optionalDocument;
  }

  private String getInsertSQL() {
    return String.format(
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb)", tableIdentifier, ID, DOCUMENT);
  }

  private String getUpdateSQL() {
    return String.format(
        "UPDATE %s SET (%s, %s) = ( ?, ? :: jsonb) ", tableIdentifier, ID, DOCUMENT);
  }

  private String getUpsertSQL() {
    return String.format(
        "INSERT INTO %s (%s,%s) VALUES( ?, ? :: jsonb) ON CONFLICT(%s) DO UPDATE SET %s = "
            + "?::jsonb ",
        tableIdentifier, ID, DOCUMENT, ID, DOCUMENT);
  }

  private String getCreateOrReplaceSQL() {
    return String.format(
        "INSERT INTO %s "
            + "(%s, %s, %s) "
            + "VALUES (?, ?::jsonb, NOW()) "
            + "ON CONFLICT(%s) "
            + "DO UPDATE SET "
            + "%s = jsonb_set(?::jsonb, '{%s}', %s.%s->'%s'), "
            + "%s = NOW() "
            + "RETURNING %s = NOW() AS %s",
        tableIdentifier,
        ID,
        DOCUMENT,
        CREATED_AT,
        ID,
        DOCUMENT,
        DocStoreConstants.CREATED_TIME,
        tableIdentifier,
        DOCUMENT,
        DocStoreConstants.CREATED_TIME,
        UPDATED_AT,
        CREATED_AT,
        CREATED_NOW_ALIAS);
  }

  private String getCreateOrReplaceAndReturnSQL() {
    return String.format(
        "INSERT INTO %s "
            + "(%s, %s, %s) "
            + "VALUES (?, ?::jsonb, NOW()) "
            + "ON CONFLICT(%s) "
            + "DO UPDATE SET "
            + "%s = jsonb_set(?::jsonb, '{%s}', %s.%s->'%s'), "
            + "%s = NOW() "
            + "RETURNING %s::text AS %s, %s AS %s, %s AS %s",
        tableIdentifier,
        ID,
        DOCUMENT,
        CREATED_AT,
        ID,
        DOCUMENT,
        DocStoreConstants.CREATED_TIME,
        tableIdentifier,
        DOCUMENT,
        DocStoreConstants.CREATED_TIME,
        UPDATED_AT,
        DOCUMENT,
        DOCUMENT,
        CREATED_AT,
        CREATED_AT,
        UPDATED_AT,
        UPDATED_AT);
  }

  private String getSubDocUpdateQuery() {
    return String.format(
        "UPDATE %s SET %s=jsonb_set(%s, ?::text[], ?::jsonb) WHERE %s=?",
        tableIdentifier, DOCUMENT, DOCUMENT, ID);
  }

  static class PostgresResultIteratorWithBasicTypes extends PostgresResultIterator {

    public PostgresResultIteratorWithBasicTypes(ResultSet resultSet) {
      super(resultSet);
    }

    public PostgresResultIteratorWithBasicTypes(ResultSet resultSet, DocumentType documentType) {
      super(resultSet, documentType);
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
        System.out.println("prepare document failed!");
        closeResultSet();
        return JSONDocument.errorDocument(e.getMessage());
      }
    }

    protected Document prepareDocument() throws SQLException, IOException {
      ObjectNode jsonNode = MAPPER.createObjectNode();

      // Get metadata to iterate through all columns
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        String columnType = metaData.getColumnTypeName(i);

        addColumnToJsonNode(jsonNode, columnName, columnType, i);
      }

      // Remove document ID if needed
      if (shouldRemoveDocumentId()) {
        jsonNode.remove(DOCUMENT_ID);
      }

      return new JSONDocument(MAPPER.writeValueAsString(jsonNode), documentType);
    }

    private void addColumnToJsonNode(
        ObjectNode jsonNode, String columnName, String columnType, int columnIndex)
        throws SQLException {
      switch (columnType.toLowerCase()) {
        case "bool":
        case "boolean":
          boolean boolValue = resultSet.getBoolean(columnIndex);
          if (!resultSet.wasNull()) {
            jsonNode.put(columnName, boolValue);
          }
          break;

        case "int4":
        case "integer":
          int intValue = resultSet.getInt(columnIndex);
          if (!resultSet.wasNull()) {
            jsonNode.put(columnName, intValue);
          }
          break;

        case "int8":
        case "bigint":
          long longValue = resultSet.getLong(columnIndex);
          if (!resultSet.wasNull()) {
            jsonNode.put(columnName, longValue);
          }
          break;

        case "float8":
        case "double":
          double doubleValue = resultSet.getDouble(columnIndex);
          if (!resultSet.wasNull()) {
            jsonNode.put(columnName, doubleValue);
          }
          break;

        case "text":
        case "varchar":
          String stringValue = resultSet.getString(columnIndex);
          if (stringValue != null) {
            jsonNode.put(columnName, stringValue);
          }
          break;

        case "_text": // text array
          Array array = resultSet.getArray(columnIndex);
          if (array != null) {
            String[] stringArray = (String[]) array.getArray();
            ArrayNode arrayNode = MAPPER.createArrayNode();
            for (String item : stringArray) {
              arrayNode.add(item);
            }
            jsonNode.set(columnName, arrayNode);
          }
          break;

        case "jsonb":
        case "json":
          String jsonString = resultSet.getString(columnIndex);
          if (jsonString != null) {
            try {
              JsonNode jsonValue = MAPPER.readTree(jsonString);
              jsonNode.set(columnName, jsonValue);
            } catch (IOException e) {
              // Fallback to string if JSON parsing fails
              jsonNode.put(columnName, jsonString);
            }
          }
          break;

        default:
          Object objectValue = resultSet.getObject(columnIndex);
          if (objectValue != null) {
            jsonNode.put(columnName, objectValue.toString());
          }
          break;
      }
    }
  }

  static class PostgresResultIterator implements CloseableIterator<Document> {

    protected final ObjectMapper MAPPER = new ObjectMapper();
    protected ResultSet resultSet;
    protected boolean cursorMovedForward = false;
    protected boolean hasNext = false;

    private final boolean removeDocumentId;
    protected DocumentType documentType;

    public PostgresResultIterator(ResultSet resultSet) {
      this(resultSet, true);
    }

    PostgresResultIterator(ResultSet resultSet, boolean removeDocumentId) {
      this(resultSet, removeDocumentId, DocumentType.NESTED);
    }

    public PostgresResultIterator(ResultSet resultSet, DocumentType documentType) {
      this(resultSet, true, documentType);
    }

    PostgresResultIterator(
        ResultSet resultSet, boolean removeDocumentId, DocumentType documentType) {
      this.resultSet = resultSet;
      this.removeDocumentId = removeDocumentId;
      this.documentType = documentType;
    }

    @Override
    public boolean hasNext() {
      try {
        if (!cursorMovedForward) {
          hasNext = resultSet.next();
          cursorMovedForward = true;
        }

        if (!hasNext) {
          closeResultSet();
        }

        return hasNext;
      } catch (SQLException e) {
        LOGGER.error("SQLException iterating documents.", e);
        closeResultSet();
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
        closeResultSet();
        return JSONDocument.errorDocument(e.getMessage());
      }
    }

    protected Document prepareDocument() throws SQLException, IOException {
      String documentString = resultSet.getString(DOCUMENT);
      ObjectNode jsonNode = (ObjectNode) MAPPER.readTree(documentString);
      // internal iterators may need document id
      if (shouldRemoveDocumentId()) {
        jsonNode.remove(DOCUMENT_ID);
      }
      // Add Timestamps to Document
      Timestamp createdAt = resultSet.getTimestamp(CREATED_AT);
      Timestamp updatedAt = resultSet.getTimestamp(UPDATED_AT);
      jsonNode.put(CREATED_AT, String.valueOf(createdAt));
      jsonNode.put(UPDATED_AT, String.valueOf(updatedAt));

      return new JSONDocument(MAPPER.writeValueAsString(jsonNode), documentType);
    }

    protected void closeResultSet() {
      try {
        if (resultSet != null && !resultSet.isClosed()) {
          resultSet.close();
        }
      } catch (SQLException ex) {
        LOGGER.error("Unable to close connection", ex);
      }
    }

    @Override
    public void close() {
      closeResultSet();
    }

    protected boolean shouldRemoveDocumentId() {
      return removeDocumentId;
    }
  }

  static class PostgresResultIteratorWithMetaData extends PostgresResultIterator {

    public PostgresResultIteratorWithMetaData(ResultSet resultSet) {
      super(resultSet, true);
    }

    PostgresResultIteratorWithMetaData(ResultSet resultSet, boolean removeDocumentId) {
      super(resultSet, removeDocumentId);
    }

    @Override
    protected Document prepareDocument() throws SQLException, IOException {
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int columnCount = resultSetMetaData.getColumnCount();
      Map<String, Object> jsonNode = new HashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = resultSetMetaData.getColumnName(i);
        String columnValue = getColumnValue(resultSetMetaData, columnName, i);
        if (StringUtils.isNotEmpty(columnValue)) {
          JsonNode leafNodeValue;
          try {
            leafNodeValue = MAPPER.readTree(columnValue);
          } catch (JsonParseException ex) {
            // try to handle the case of json parsing failure
            // try to find mapping of value to json node
            // if not found then throw the same exception upwards
            leafNodeValue =
                mapValueToJsonNode(resultSetMetaData.getColumnType(i), columnValue)
                    .orElseThrow(() -> ex);
          }
          if (PostgresUtils.isEncodedNestedField(columnName)) {
            handleNestedField(
                PostgresUtils.decodeAliasForNestedField(columnName), jsonNode, leafNodeValue);
          } else {
            jsonNode.put(columnName, leafNodeValue);
          }
        }
      }
      return new JSONDocument(MAPPER.writeValueAsString(jsonNode), documentType);
    }

    private String getColumnValue(
        ResultSetMetaData resultSetMetaData, String columnName, int columnIndex)
        throws SQLException, JsonProcessingException {
      int columnType = resultSetMetaData.getColumnType(columnIndex);
      // check for array
      if (columnType == Types.ARRAY) {
        return MAPPER.writeValueAsString(resultSet.getArray(columnIndex).getArray());
      }

      // check for any OUTER column including ID
      // please note below check will not work in case of alias as alias provided
      // can be different from actual column name
      if (OUTER_COLUMNS.contains(columnName) || IMPLICIT_ID.equals(columnName)) {
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

  private static Optional<JsonNode> mapValueToJsonNode(int columnType, String columnValue) {
    switch (columnType) {
      case VARCHAR:
        return Optional.of(TextNode.valueOf(columnValue));
      case BIGINT:
        return Optional.of(BigIntegerNode.valueOf(BigInteger.valueOf(Long.valueOf(columnValue))));
      case INTEGER:
        return Optional.of(IntNode.valueOf(Integer.valueOf(columnValue)));
      case BOOLEAN:
        return Optional.of(BooleanNode.valueOf(Boolean.valueOf(columnValue)));
      case DOUBLE:
        return Optional.of(DoubleNode.valueOf(Double.valueOf(columnValue)));
      case FLOAT:
        return Optional.of(FloatNode.valueOf(Float.valueOf(columnValue)));
      default:
        return Optional.empty();
    }
  }
}
