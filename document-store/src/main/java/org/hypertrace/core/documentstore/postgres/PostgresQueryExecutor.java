package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.postgres.PostgresCollection.PostgresResultIteratorWithMetaData;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.PostgresQueryTransformer;
import org.hypertrace.core.documentstore.postgres.registry.PostgresColumnRegistry;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public class PostgresQueryExecutor {
  private final PostgresTableIdentifier tableIdentifier;

  public CloseableIterator<Document> execute(final Connection connection, final Query query) {
    return execute(connection, query, null, null);
  }

  public CloseableIterator<Document> execute(
      final Connection connection, final Query query, String flatStructureCollectionName) {
    return execute(connection, query, flatStructureCollectionName, null);
  }

  public CloseableIterator<Document> execute(
      final Connection connection,
      final Query query,
      String flatStructureCollectionName,
      PostgresColumnRegistry columnRegistry) {
    final org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            tableIdentifier, transformAndLog(query), flatStructureCollectionName, columnRegistry);
    final String sqlQuery = queryParser.parse();
    try {
      final PreparedStatement preparedStatement =
          buildPreparedStatement(sqlQuery, queryParser.getParamsBuilder().build(), connection);
      log.debug("Executing executeQueryV1 sqlQuery:{}", preparedStatement.toString());
      final ResultSet resultSet = preparedStatement.executeQuery();

      // For queries with selections, use PostgresResultIteratorWithMetaData
      // as it properly handles nested field decoding
      if (query.getSelections().size() > 0) {
        return new PostgresResultIteratorWithMetaData(resultSet);
      }

      // For queries without selections, use a custom iterator that extracts clean documents
      // from the 'document' column instead of returning wrapped results
      return new PostgresCollection.PostgresResultIteratorCleanDocument(resultSet);
    } catch (SQLException e) {
      log.error(
          "SQLException querying documents. original query: " + query + ", sql query:" + sqlQuery,
          e);
      throw new RuntimeException(e);
    }
  }

  public PreparedStatement buildPreparedStatement(
      String sqlQuery, Params params, Connection connection) throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
    enrichPreparedStatementWithParams(preparedStatement, params);
    return preparedStatement;
  }

  private static org.hypertrace.core.documentstore.query.Query transformAndLog(
      org.hypertrace.core.documentstore.query.Query query) {
    log.debug("Original query before transformation: {}", query);
    query = PostgresQueryTransformer.transform(query);
    log.debug("Query after transformation: {}", query);
    return query;
  }
}
