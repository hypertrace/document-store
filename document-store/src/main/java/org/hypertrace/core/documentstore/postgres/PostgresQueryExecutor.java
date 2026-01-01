package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.PostgresQueryTransformer;

@Slf4j
public class PostgresQueryExecutor {

  private final int queryTimeoutSeconds;

  public PostgresQueryExecutor(int queryTimeoutSeconds) {
    this.queryTimeoutSeconds = queryTimeoutSeconds;
  }

  static org.hypertrace.core.documentstore.query.Query transformAndLog(
      org.hypertrace.core.documentstore.query.Query query) {
    log.debug("Original query before transformation: {}", query);
    query = PostgresQueryTransformer.transform(query);
    log.debug("Query after transformation: {}", query);
    return query;
  }

  protected ResultSet execute(final Connection connection, PostgresQueryParser queryParser)
      throws SQLException {
    final String sqlQuery = queryParser.parse();
    final Params params = queryParser.getParamsBuilder().build();
    // this is closed when the corresponding ResultSet is closed in the iterators
    PreparedStatement preparedStatement =
        buildPreparedStatement(sqlQuery, params, connection, queryTimeoutSeconds);
    try {
      log.debug("Executing SQL query: {}", sqlQuery);
      return preparedStatement.executeQuery();
    } catch (SQLException e) {
      log.error(
          "SQL execution failed. Query: {}, SQLState: {}, ErrorCode: {}",
          sqlQuery,
          e.getSQLState(),
          e.getErrorCode(),
          e);
      throw e;
    }
  }

  public PreparedStatement buildPreparedStatement(
      String sqlQuery, Params params, Connection connection) throws SQLException {
    return buildPreparedStatement(sqlQuery, params, connection, this.queryTimeoutSeconds);
  }

  public PreparedStatement buildPreparedStatement(
      String sqlQuery, Params params, Connection connection, int queryTimeoutSeconds)
      throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
    if (queryTimeoutSeconds > 0) {
      preparedStatement.setQueryTimeout(queryTimeoutSeconds);
    }
    enrichPreparedStatementWithParams(preparedStatement, params);
    return preparedStatement;
  }
}
