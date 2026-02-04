package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.BulkArrayValueUpdateRequest;
import org.hypertrace.core.documentstore.BulkDeleteResult;
import org.hypertrace.core.documentstore.BulkUpdateRequest;
import org.hypertrace.core.documentstore.BulkUpdateResult;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.CreateResult;
import org.hypertrace.core.documentstore.CreateStatus;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
import org.hypertrace.core.documentstore.model.exception.DuplicateDocumentException;
import org.hypertrace.core.documentstore.model.exception.SchemaMismatchException;
import org.hypertrace.core.documentstore.model.options.MissingColumnStrategy;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.model.options.ReturnDocumentType;
import org.hypertrace.core.documentstore.model.options.UpdateOptions;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.documentstore.postgres.model.PostgresColumnMetadata;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.FlatPostgresFieldTransformer;
import org.hypertrace.core.documentstore.postgres.update.FlatUpdateContext;
import org.hypertrace.core.documentstore.postgres.update.parser.FlatCollectionSubDocSetOperatorParser;
import org.hypertrace.core.documentstore.postgres.update.parser.FlatCollectionSubDocUpdateOperatorParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
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
  private static final String MISSING_COLUMN_STRATEGY_CONFIG = "missingColumnStrategy";
  private static final String DEFAULT_PRIMARY_KEY_COLUMN = "key";

  private static final Map<UpdateOperator, FlatCollectionSubDocUpdateOperatorParser>
      SUB_DOC_UPDATE_PARSERS = Map.of(SET, new FlatCollectionSubDocSetOperatorParser());

  private final PostgresLazyilyLoadedSchemaRegistry schemaRegistry;

  /**
   * Strategy for handling fields that don't match the schema. Default is SKIP (best-effort writes).
   * When THROW, all fields must be present in the schema with correct types.
   */
  private final MissingColumnStrategy missingColumnStrategy;

  FlatPostgresCollection(
      final PostgresClient client,
      final String collectionName,
      final PostgresLazyilyLoadedSchemaRegistry schemaRegistry) {
    super(client, collectionName);
    this.schemaRegistry = schemaRegistry;
    this.missingColumnStrategy = parseMissingColumnStrategy(client.getCustomParameters());
  }

  private static MissingColumnStrategy parseMissingColumnStrategy(Map<String, String> params) {
    String value = params.get(MISSING_COLUMN_STRATEGY_CONFIG);
    if (value == null || value.isEmpty()) {
      return MissingColumnStrategy.defaultStrategy();
    }
    try {
      return MissingColumnStrategy.valueOf(value.toUpperCase());
    } catch (IllegalArgumentException e) {
      LOGGER.warn(
          "Invalid missingColumnStrategy value: '{}', using default SKIP. Valid values: {}",
          value,
          Arrays.toString(MissingColumnStrategy.values()));
      return MissingColumnStrategy.defaultStrategy();
    }
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
    if (documents == null || documents.isEmpty()) {
      return true;
    }

    String tableName = tableIdentifier.getTableName();
    String pkColumn = getPKForTable(tableName);
    String quotedPkColumn = PostgresUtils.wrapFieldNamesWithDoubleQuotes(pkColumn);
    PostgresDataType pkType = getPrimaryKeyType(tableName, pkColumn);

    try {
      // Parse all documents and collect the union of all columns. This is because we can have
      // different docs with different sets of cols, so we do this to create a single upsert SQL
      Map<Key, TypedDocument> parsedDocuments = new LinkedHashMap<>();
      Set<String> allColumns = new LinkedHashSet<>();
      allColumns.add(quotedPkColumn);

      List<Key> ignoredDocuments = new ArrayList<>();
      for (Map.Entry<Key, Document> entry : documents.entrySet()) {
        List<String> skippedFields = new ArrayList<>();
        TypedDocument parsed = parseDocument(entry.getValue(), tableName, skippedFields);

        // Handle IGNORE_DOCUMENT strategy: skip docs with unknown fields
        if (missingColumnStrategy == MissingColumnStrategy.IGNORE_DOCUMENT
            && !skippedFields.isEmpty()) {
          ignoredDocuments.add(entry.getKey());
          continue;
        }

        parsed.add(quotedPkColumn, entry.getKey().toString(), pkType, false);
        parsedDocuments.put(entry.getKey(), parsed);
        allColumns.addAll(parsed.getColumns());
      }

      if (!ignoredDocuments.isEmpty()) {
        LOGGER.info(
            "bulkUpsert: Ignored {} documents due to IGNORE_DOCUMENT strategy. Keys: {}",
            ignoredDocuments.size(),
            ignoredDocuments);
      }

      // If all documents were ignored, return true (nothing to do)
      if (parsedDocuments.isEmpty()) {
        return true;
      }

      // Build the bulk upsert SQL with all columns
      List<String> columnList = new ArrayList<>(allColumns);
      String sql = buildBulkUpsertSql(columnList, quotedPkColumn);
      LOGGER.debug("Bulk upsert SQL: {}", sql);

      try (Connection conn = client.getPooledConnection();
          PreparedStatement ps = conn.prepareStatement(sql)) {

        for (Map.Entry<Key, TypedDocument> entry : parsedDocuments.entrySet()) {
          TypedDocument parsed = entry.getValue();
          int index = 1;

          for (String column : columnList) {
            if (parsed.getColumns().contains(column)) {
              setParameter(
                  conn,
                  ps,
                  index++,
                  parsed.getValue(column),
                  parsed.getType(column),
                  parsed.isArray(column));
            } else {
              ps.setObject(index++, null);
            }
          }
          ps.addBatch();
        }

        int[] results = ps.executeBatch();
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Bulk upsert results: {}", Arrays.toString(results));
        }
        return true;
      }

    } catch (BatchUpdateException e) {
      LOGGER.error("BatchUpdateException in bulkUpsert", e);
    } catch (SQLException e) {
      LOGGER.error(
          "SQLException in bulkUpsert. SQLState: {} Error Code: {}",
          e.getSQLState(),
          e.getErrorCode(),
          e);
    } catch (IOException e) {
      LOGGER.error("IOException in bulkUpsert. documents: {}", documents, e);
    }

    return false;
  }

  /**
   * Builds a PostgreSQL bulk upsert SQL statement for batch execution.
   *
   * @param columns List of quoted column names (PK should be first)
   * @param pkColumn The quoted primary key column name
   * @return The upsert SQL statement
   */
  private String buildBulkUpsertSql(List<String> columns, String pkColumn) {
    String columnList = String.join(", ", columns);
    String placeholders = String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new));

    // Build SET clause for non-PK columns: col = EXCLUDED.col (this ensures that on conflict, the
    // new value is picked)
    String setClause =
        columns.stream()
            .filter(col -> !col.equals(pkColumn))
            .map(col -> col + " = EXCLUDED." + col)
            .collect(Collectors.joining(", "));

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
        tableIdentifier, columnList, placeholders, pkColumn, setClause);
  }

  @Override
  public CloseableIterator<Document> bulkUpsertAndReturnOlderDocuments(Map<Key, Document> documents)
      throws IOException {
    if (documents == null || documents.isEmpty()) {
      return CloseableIterator.emptyIterator();
    }

    String tableName = tableIdentifier.getTableName();
    String pkColumn = getPKForTable(tableName);
    String quotedPkColumn = PostgresUtils.wrapFieldNamesWithDoubleQuotes(pkColumn);
    PostgresDataType pkType = getPrimaryKeyType(tableName, pkColumn);

    Connection connection = null;
    try {
      connection = client.getPooledConnection();

      PreparedStatement preparedStatement =
          getPreparedStatementForQuery(documents, quotedPkColumn, connection, pkType);

      ResultSet resultSet = preparedStatement.executeQuery();

      boolean upsertResult = bulkUpsert(documents);
      if (!upsertResult) {
        closeConnection(connection);
        throw new IOException("Bulk upsert failed");
      }

      // note that connection will be closed after the iterator is used by the client
      return new PostgresCollection.PostgresResultIteratorWithBasicTypes(
          resultSet, connection, DocumentType.FLAT);

    } catch (SQLException e) {
      LOGGER.error("SQLException in bulkUpsertAndReturnOlderDocuments", e);
      closeConnection(connection);
      throw new IOException("Could not bulk upsert the documents.", e);
    }
  }

  private static void closeConnection(Connection connection) {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException closeEx) {
        LOGGER.warn("Error closing connection after exception", closeEx);
      }
    }
  }

  private PreparedStatement getPreparedStatementForQuery(
      Map<Key, Document> documents,
      String quotedPkColumn,
      Connection connection,
      PostgresDataType pkType)
      throws SQLException {
    String selectQuery =
        String.format("SELECT * FROM %s WHERE %s = ANY(?)", tableIdentifier, quotedPkColumn);
    PreparedStatement preparedStatement = connection.prepareStatement(selectQuery);

    String[] keyArray = documents.keySet().stream().map(Key::toString).toArray(String[]::new);
    Array sqlArray = connection.createArrayOf(pkType.getSqlType(), keyArray);
    preparedStatement.setArray(1, sqlArray);
    return preparedStatement;
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
      Collection<SubDocumentUpdate> updates,
      UpdateOptions updateOptions)
      throws IOException {

    Preconditions.checkArgument(
        updateOptions != null && !updates.isEmpty(), "Updates collection cannot be NULL or empty");

    String tableName = tableIdentifier.getTableName();

    // Acquire a transactional connection that can be managed manually
    try (Connection connection = client.getTransactionalConnection()) {
      try {
        // 1. Validate all columns exist and operators are supported.
        Map<String, String> resolvedColumns = resolvePathsToColumns(updates, tableName);

        // 2. Get before-document if needed (only for BEFORE_UPDATE)
        Optional<Document> beforeDoc = Optional.empty();
        ReturnDocumentType returnType = updateOptions.getReturnDocumentType();
        if (returnType == BEFORE_UPDATE) {
          beforeDoc = selectFirstDocument(connection, query);
          if (beforeDoc.isEmpty()) {
            connection.commit();
            return Optional.empty();
          }
        }

        // 3. Build and execute UPDATE
        executeUpdate(connection, query, updates, tableName, resolvedColumns);

        // 4. Resolve return document based on options
        Document returnDoc = null;
        if (returnType == BEFORE_UPDATE) {
          returnDoc = beforeDoc.orElse(null);
        } else if (returnType == AFTER_UPDATE) {
          returnDoc = selectFirstDocument(connection, query).orElse(null);
        }

        connection.commit();
        return Optional.ofNullable(returnDoc);

      } catch (Exception e) {
        connection.rollback();
        throw e;
      }
    } catch (SQLException e) {
      LOGGER.error("SQLException during update operation", e);
      throw new IOException(e);
    }
  }

  /**
   * Validates all updates and resolves column names.
   *
   * @return Map of path -> columnName for all resolved columns. For example: customAttributes.props
   *     -> customAttributes (since customAttributes is the top-level JSONB col)
   */
  private Map<String, String> resolvePathsToColumns(
      Collection<SubDocumentUpdate> updates, String tableName) {
    Map<String, String> resolvedColumns = new HashMap<>();

    for (SubDocumentUpdate update : updates) {
      UpdateOperator operator = update.getOperator();

      Preconditions.checkArgument(
          SUB_DOC_UPDATE_PARSERS.containsKey(operator), "Unsupported UPDATE operator: " + operator);

      String path = update.getSubDocument().getPath();
      Optional<String> columnName = resolveColumnName(path, tableName);

      // If the column is not found and missing column strategy is configured to throw, throw an
      // exception.
      Preconditions.checkArgument(
          columnName.isPresent() || missingColumnStrategy != MissingColumnStrategy.THROW,
          "Column not found in schema for path: "
              + path
              + " and missing column strategy is configured to: "
              + missingColumnStrategy.toString());

      columnName.ifPresent(col -> resolvedColumns.put(path, col));
    }

    return resolvedColumns;
  }

  /**
   * Resolves a path to its column name, handling both dotted column names and JSONB paths.
   *
   * <p>Resolution order:
   *
   * <ol>
   *   <li>Check if full path exists as a column name (handles "customProps.something")
   *   <li>If not, progressively try shorter prefixes to find a JSONB column
   * </ol>
   *
   * @return Optional containing the column name, or empty if no valid column found
   */
  private Optional<String> resolveColumnName(String path, String tableName) {
    // First, check if the full path is a column name. If yes, then it's a top-level field. Return
    // it.
    if (schemaRegistry.getColumnOrRefresh(tableName, path).isPresent()) {
      return Optional.of(path);
    }

    // Not a direct column - try to find a JSONB column prefix
    if (!path.contains(".")) {
      return Optional.empty();
    }

    String[] parts = path.split("\\.");
    StringBuilder columnBuilder = new StringBuilder(parts[0]);

    for (int i = 0; i < parts.length - 1; i++) {
      if (i > 0) {
        columnBuilder.append(".").append(parts[i]);
      }
      String candidateColumn = columnBuilder.toString();
      Optional<PostgresColumnMetadata> colMeta =
          schemaRegistry.getColumnOrRefresh(tableName, candidateColumn);

      if (colMeta.isPresent() && colMeta.get().getPostgresType() == PostgresDataType.JSONB) {
        return Optional.of(candidateColumn);
      }
    }

    return Optional.empty();
  }

  /** Extracts the nested JSONB path from a full path given the resolved column name. */
  private String[] getNestedPath(String fullPath, String columnName) {
    if (fullPath.equals(columnName)) {
      return new String[0];
    }
    // Remove column name prefix and split the rest
    String nested = fullPath.substring(columnName.length() + 1); // +1 for the dot
    return nested.split("\\.");
  }

  private Optional<Document> selectFirstDocument(Connection connection, Query query)
      throws SQLException, IOException {
    PostgresQueryParser parser = createParser(query);
    String selectQuery = parser.buildSelectQueryForUpdate();

    try (PreparedStatement ps =
        queryExecutor.buildPreparedStatement(
            selectQuery, parser.getParamsBuilder().build(), connection)) {
      return getFirstDocumentForFlat(ps.executeQuery());
    }
  }

  private Optional<Document> getFirstDocumentForFlat(ResultSet resultSet) throws IOException {
    CloseableIterator<Document> iterator =
        new PostgresResultIteratorWithBasicTypes(resultSet, DocumentType.FLAT);
    return getFirstDocument(iterator);
  }

  private void executeUpdate(
      Connection connection,
      Query query,
      Collection<SubDocumentUpdate> updates,
      String tableName,
      Map<String, String> resolvedColumns)
      throws SQLException {

    // Build WHERE clause
    PostgresQueryParser filterParser = createParser(query);
    String filterClause = filterParser.buildFilterClause();
    Params filterParams = filterParser.getParamsBuilder().build();

    // Build SET clause fragments
    List<String> setFragments = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    for (SubDocumentUpdate update : updates) {
      String path = update.getSubDocument().getPath();
      String columnName = resolvedColumns.get(path);

      if (columnName == null) {
        LOGGER.warn("Skipping update for unresolved path: {}", path);
        continue;
      }

      PostgresColumnMetadata colMeta =
          schemaRegistry.getColumnOrRefresh(tableName, columnName).orElseThrow();

      FlatUpdateContext context =
          FlatUpdateContext.builder()
              .columnName(columnName)
              // get the nested path. So for example, if colName is `customAttr` and full path is
              // `customAttr.props`, then the nested path is `props`.
              .nestedPath(getNestedPath(path, columnName))
              .columnType(colMeta.getPostgresType())
              .value(update.getSubDocumentValue())
              .params(params)
              .build();

      FlatCollectionSubDocUpdateOperatorParser operatorParser =
          SUB_DOC_UPDATE_PARSERS.get(update.getOperator());
      String fragment = operatorParser.parse(context);
      setFragments.add(fragment);
    }

    // If all updates were skipped, nothing to do
    if (setFragments.isEmpty()) {
      LOGGER.warn("All update paths were skipped - no valid columns to update");
      return;
    }

    // Build final UPDATE SQL
    String sql =
        String.format(
            "UPDATE %s SET %s %s", tableIdentifier, String.join(", ", setFragments), filterClause);

    LOGGER.debug("Executing update SQL: {}", sql);

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      int idx = 1;
      // Add SET clause params
      for (Object param : params) {
        ps.setObject(idx++, param);
      }
      // Add WHERE clause params
      for (Object param : filterParams.getObjectParams().values()) {
        ps.setObject(idx++, param);
      }
      int rowsUpdated = ps.executeUpdate();
      LOGGER.debug("Rows updated: {}", rowsUpdated);
    } catch (SQLException e) {
      LOGGER.error("Failed to execute update. SQL: {}, SQLState: {}", sql, e.getSQLState(), e);
      throw e;
    }
  }

  @Override
  public CloseableIterator<Document> bulkUpdate(
      org.hypertrace.core.documentstore.query.Query query,
      java.util.Collection<SubDocumentUpdate> updates,
      UpdateOptions updateOptions)
      throws IOException {
    throw new UnsupportedOperationException(WRITE_NOT_SUPPORTED);
  }

  /*isRetry: Whether this is a retry attempt*/
  private CreateResult createWithRetry(Key key, Document document, boolean isRetry)
      throws IOException {
    String tableName = tableIdentifier.getTableName();

    List<String> skippedFields = new ArrayList<>();

    try {
      TypedDocument parsed = parseDocument(document, tableName, skippedFields);

      // Add the key as the primary key column
      String pkColumn = getPKForTable(tableName);
      String quotedPkColumn = PostgresUtils.wrapFieldNamesWithDoubleQuotes(pkColumn);
      PostgresDataType pkType = getPrimaryKeyType(tableName, pkColumn);
      parsed.add(quotedPkColumn, key.toString(), pkType, false);

      // If IGNORE_DOCUMENT strategy and any fields were skipped, ignore the entire document
      if (missingColumnStrategy == MissingColumnStrategy.IGNORE_DOCUMENT
          && !skippedFields.isEmpty()) {
        LOGGER.info(
            "Document ignored due to IGNORE_DOCUMENT strategy. Skipped fields: {}", skippedFields);
        return new CreateResult(CreateStatus.IGNORED, isRetry, skippedFields);
      }

      String sql = buildInsertSql(parsed.getColumns());
      LOGGER.debug("Insert SQL: {}", sql);

      int result = executeUpdate(sql, parsed);
      LOGGER.debug("Create result: {}", result);
      return new CreateResult(result > 0, isRetry, skippedFields);

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

  private TypedDocument parseDocument(
      Document document, String tableName, List<String> skippedColumns) throws IOException {
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
        if (missingColumnStrategy == MissingColumnStrategy.THROW) {
          throw new SchemaMismatchException(
              "Column '" + fieldName + "' not found in schema for table: " + tableName);
        }
        LOGGER.warn("Could not find column metadata for column: {}, skipping it", fieldName);
        skippedColumns.add(fieldName);
        continue;
      }

      if (columnMetadata.get().isPrimaryKey()) {
        // PK is added by the caller
        continue;
      }

      PostgresDataType type = columnMetadata.get().getPostgresType();
      boolean isArray = columnMetadata.get().isArray();

      try {
        Object value = extractValue(fieldValue, type, isArray);
        typedDocument.add(
            PostgresUtils.wrapFieldNamesWithDoubleQuotes(fieldName), value, type, isArray);
      } catch (Exception e) {
        if (missingColumnStrategy == MissingColumnStrategy.THROW) {
          throw new SchemaMismatchException(
              "Failed to parse value for column '"
                  + fieldName
                  + "' with type "
                  + type
                  + ": "
                  + e.getMessage(),
              e);
        }
        LOGGER.warn(
            "Could not parse value for column: {} with type: {}, skipping it. Error: {}",
            fieldName,
            type,
            e.getMessage());
        skippedColumns.add(fieldName);
      }
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

  private boolean createOrReplaceWithRetry(Key key, Document document, boolean isRetry)
      throws IOException {
    String tableName = tableIdentifier.getTableName();
    List<String> skippedFields = new ArrayList<>();

    try {
      TypedDocument parsed = parseDocument(document, tableName, skippedFields);

      // Add the key as the primary key column
      String pkColumn = getPKForTable(tableName);
      String quotedPkColumn = PostgresUtils.wrapFieldNamesWithDoubleQuotes(pkColumn);
      PostgresDataType pkType = getPrimaryKeyType(tableName, pkColumn);
      parsed.add(quotedPkColumn, key.toString(), pkType, false);

      String sql = buildUpsertSql(parsed.getColumns(), quotedPkColumn);
      LOGGER.debug("Upsert SQL: {}", sql);

      return executeUpsert(sql, parsed);

    } catch (PSQLException e) {
      return handlePSQLExceptionForUpsert(e, key, document, tableName, isRetry);
    } catch (SQLException e) {
      LOGGER.error("SQLException in createOrReplace. key: {} content: {}", key, document, e);
      throw new IOException(e);
    }
  }

  /**
   * Builds a PostgreSQL upsert (INSERT ... ON CONFLICT DO UPDATE) SQL statement.
   *
   * <p>This method constructs an atomic upsert query that:
   *
   * <ul>
   *   <li>Inserts a new row if no conflict on the primary key
   *   <li>Updates all non-PK columns if a row with the same PK already exists
   * </ul>
   *
   * <p><b>Generated SQL pattern:</b>
   *
   * <pre>{@code
   * INSERT INTO table (col1, col2, pk_col)
   * VALUES (?, ?, ?)
   * ON CONFLICT (pk_col) DO UPDATE SET col1 = EXCLUDED.col1, col2 = EXCLUDED.col2
   * RETURNING (xmax = 0) AS is_insert
   * }</pre>
   *
   * <p><b>The EXCLUDED table:</b> In PostgreSQL's ON CONFLICT clause, {@code EXCLUDED} is a special
   * table that references the row that would have been inserted (the "proposed" row). This allows
   * us to update existing rows with the new values without re-specifying them.
   *
   * <p><b>The RETURNING clause:</b> {@code (xmax = 0) AS is_insert} is a PostgreSQL trick to
   * determine if the operation was an INSERT or UPDATE:
   *
   * <ul>
   *   <li>{@code xmax} is a system column that stores the transaction ID of the deleting/updating
   *       transaction
   *   <li>For a freshly inserted row, {@code xmax = 0} (no prior transaction modified it)
   *   <li>For an updated row, {@code xmax != 0} (the UPDATE sets it to the current transaction ID)
   *   <li>Thus, {@code is_insert = true} means INSERT, {@code is_insert = false} means UPDATE
   * </ul>
   *
   * @param columns List of quoted column names to include in the upsert (including PK)
   * @param pkColumn The quoted primary key column name used for conflict detection
   * @return The complete upsert SQL statement with placeholders for values
   */
  private String buildUpsertSql(List<String> columns, String pkColumn) {
    String columnList = String.join(", ", columns);
    String placeholders = String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new));

    // Build SET clause for non-PK columns: col = EXCLUDED.col
    String setClause =
        columns.stream()
            .filter(col -> !col.equals(pkColumn))
            .map(col -> col + " = EXCLUDED." + col)
            .collect(Collectors.joining(", "));

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s RETURNING (xmax = 0) AS is_insert",
        tableIdentifier, columnList, placeholders, pkColumn, setClause);
  }

  private boolean executeUpsert(String sql, TypedDocument parsed) throws SQLException {
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
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          // is_insert is true if xmax = 0 (new row), false if updated. This helps us differentiate
          // b/w creates/upserts
          return rs.getBoolean("is_insert");
        }
      }
      return false;
    }
  }

  private boolean handlePSQLExceptionForUpsert(
      PSQLException e, Key key, Document document, String tableName, boolean isRetry)
      throws IOException {
    if (!isRetry && shouldRefreshSchemaAndRetry(e.getSQLState())) {
      LOGGER.info(
          "Schema mismatch detected during upsert (SQLState: {}), refreshing schema and retrying. key: {}",
          e.getSQLState(),
          key);
      schemaRegistry.invalidate(tableName);
      return createOrReplaceWithRetry(key, document, true);
    }
    LOGGER.error("SQLException in createOrReplace. key: {} content: {}", key, document, e);
    throw new IOException(e);
  }

  private CreateResult handlePSQLExceptionForCreate(
      PSQLException e, Key key, Document document, String tableName, boolean isRetry)
      throws IOException {
    if (!isRetry && shouldRefreshSchemaAndRetry(e.getSQLState())) {
      LOGGER.info(
          "Schema mismatch detected during create (SQLState: {}), refreshing schema and retrying. key: {}",
          e.getSQLState(),
          key);
      schemaRegistry.invalidate(tableName);
      return createWithRetry(key, document, true);
    }
    LOGGER.error("SQLException creating document. key: {} content: {}", key, document, e);
    throw new IOException(e);
  }

  /**
   * Returns true if the SQL state indicates a schema mismatch, i.e. the column does not exist or
   * the data type is mismatched.
   */
  private boolean shouldRefreshSchemaAndRetry(String sqlState) {
    return PSQLState.UNDEFINED_COLUMN.getState().equals(sqlState)
        || PSQLState.DATATYPE_MISMATCH.getState().equals(sqlState);
  }

  private String getPKForTable(String tableName) {
    return schemaRegistry.getPrimaryKeyColumn(tableName).orElse(DEFAULT_PRIMARY_KEY_COLUMN);
  }

  private PostgresDataType getPrimaryKeyType(String tableName, String pkColumn) {
    return schemaRegistry
        .getColumnOrRefresh(tableName, pkColumn)
        .map(PostgresColumnMetadata::getPostgresType)
        .orElse(PostgresDataType.TEXT);
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
      // todo: Maybe check if the value is actually an array
      Object[] arrayValues = (Object[]) value;
      Array sqlArray = conn.createArrayOf(type.getSqlType(), arrayValues);
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
