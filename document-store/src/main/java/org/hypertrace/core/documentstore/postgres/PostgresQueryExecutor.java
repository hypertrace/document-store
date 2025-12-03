package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.PostgresQueryTransformer;

@Slf4j
@AllArgsConstructor
public class PostgresQueryExecutor {

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
    final PreparedStatement preparedStatement =
        buildPreparedStatement(sqlQuery, queryParser.getParamsBuilder().build(), connection);
    log.debug("Executing executeQueryV1 sqlQuery:{}", preparedStatement.toString());
    return preparedStatement.executeQuery();
  }

  public PreparedStatement buildPreparedStatement(
      String sqlQuery, Params params, Connection connection) throws SQLException, RuntimeException {
    PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
    enrichPreparedStatementWithParams(preparedStatement, params);
    return preparedStatement;
  }
}
