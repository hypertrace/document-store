package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.AFTER_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.BEFORE_UPDATE;
import static org.hypertrace.core.documentstore.model.options.ReturnDocumentType.NONE;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.UpdateResult;
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
  private static final String WRITE_NOT_SUPPORTED =
      "Write operations are not supported for flat collections yet!";

  private static final Map<UpdateOperator, FlatCollectionSubDocUpdateOperatorParser> OPERATOR_PARSERS =
      Map.of(SET, new FlatCollectionSubDocSetOperatorParser());

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
      Collection<SubDocumentUpdate> updates,
      UpdateOptions updateOptions)
      throws IOException {

    if (updates == null || updates.isEmpty()) {
      throw new IOException("Updates collection cannot be null or empty");
    }

    String tableName = tableIdentifier.getTableName();

    try (Connection connection = client.getTransactionalConnection()) {
      try {
        // 1. Validate all columns exist and operators are supported
        validateUpdates(updates, tableName);

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
        executeUpdate(connection, query, updates, tableName);

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

  private void validateUpdates(Collection<SubDocumentUpdate> updates, String tableName)
      throws IOException {
    for (SubDocumentUpdate update : updates) {
      UpdateOperator operator = update.getOperator();

      if (!OPERATOR_PARSERS.containsKey(operator)) {
        throw new IOException("Unsupported update operator: " + operator);
      }

      // Check column exists
      String path = update.getSubDocument().getPath();
      String rootColumn = path.contains(".") ? path.split("\\.")[0] : path;

      Optional<PostgresColumnMetadata> colMeta =
          schemaRegistry.getColumnOrRefresh(tableName, rootColumn);

      if (colMeta.isEmpty()) {
        throw new IOException("Column not found in schema: " + rootColumn);
      }

      // For nested paths, root column must be JSONB
      if (path.contains(".") && colMeta.get().getPostgresType() != PostgresDataType.JSONB) {
        throw new IOException(
            "Nested path updates require JSONB column, but column '"
                + rootColumn
                + "' is of type: "
                + colMeta.get().getPostgresType());
      }
    }
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
      Connection connection, Query query, Collection<SubDocumentUpdate> updates, String tableName)
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
      String rootColumn = path.contains(".") ? path.split("\\.")[0] : path;
      String[] nestedPath =
          path.contains(".") ? path.substring(path.indexOf(".") + 1).split("\\.") : new String[0];

      PostgresColumnMetadata colMeta =
          schemaRegistry.getColumnOrRefresh(tableName, rootColumn).orElseThrow();

      FlatUpdateContext context =
          FlatUpdateContext.builder()
              .columnName(rootColumn)
              .nestedPath(nestedPath)
              .columnType(colMeta.getPostgresType())
              .value(update.getSubDocumentValue())
              .params(params)
              .build();

      FlatCollectionSubDocUpdateOperatorParser operatorParser = OPERATOR_PARSERS.get(update.getOperator());
      String fragment = operatorParser.parse(context);
      setFragments.add(fragment);
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
}
