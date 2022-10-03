package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Value;

public class PostgresQueryExecutor {
  public QueryResult execute(final Connection connection, final String query, final Params params)
      throws SQLException {
    final PreparedStatement preparedStatement = connection.prepareStatement(query);
    enrichPreparedStatementWithParams(preparedStatement, params);
    final ResultSet resultSet = preparedStatement.executeQuery();

    return new QueryResult(preparedStatement, resultSet);
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
