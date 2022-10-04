package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.postgres.PostgresCollection.PostgresResultIterator;
import org.hypertrace.core.documentstore.postgres.PostgresCollection.PostgresResultIteratorWithMetaData;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.PostgresQueryTransformer;
import org.hypertrace.core.documentstore.query.Query;

@Slf4j
@AllArgsConstructor
public class PostgresQueryExecutor {
  private final String collectionName;

  public QueryResult execute(final Connection connection, final String query, final Params params)
      throws SQLException {
    final PreparedStatement preparedStatement = connection.prepareStatement(query);
    enrichPreparedStatementWithParams(preparedStatement, params);
    final ResultSet resultSet = preparedStatement.executeQuery();

    return new QueryResult(preparedStatement, resultSet);
  }

  public CloseableIterator<Document> execute(final Connection connection, final Query query) {
    final org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser queryParser =
        new org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser(
            collectionName, transformAndLog(query));
    final String sqlQuery = queryParser.parse();
    try {
      final PreparedStatement preparedStatement =
          buildPreparedStatement(sqlQuery, queryParser.getParamsBuilder().build(), connection);
      log.debug("Executing executeQueryV1 sqlQuery:{}", preparedStatement.toString());
      final ResultSet resultSet = preparedStatement.executeQuery();
      return query.getSelections().size() > 0
          ? new PostgresResultIteratorWithMetaData(resultSet)
          : new PostgresResultIterator(resultSet);
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

  @Value
  public static class QueryResult implements Closeable {
    PreparedStatement preparedStatement;
    ResultSet resultSet;

    @Override
    public void close() throws IOException {
      try {
        resultSet.close();
        preparedStatement.close();
      } catch (final SQLException e) {
        throw new IOException(e);
      }
    }
  }
}
