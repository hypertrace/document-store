package org.hypertrace.core.documentstore.postgres;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PostgresQueryExecutorTest {

  @Mock private Connection mockConnection;
  @Mock private PreparedStatement mockPreparedStatement;

  @Test
  void testPreparedStatementUsesConfiguredTimeout() throws SQLException {
    int configuredTimeoutSeconds = 45;
    String sqlQuery = "SELECT * FROM test_table";
    Params params = Params.newBuilder().build();

    when(mockConnection.prepareStatement(sqlQuery)).thenReturn(mockPreparedStatement);

    PostgresQueryExecutor executor = new PostgresQueryExecutor(configuredTimeoutSeconds);
    executor.buildPreparedStatement(sqlQuery, params, mockConnection);
    verify(mockPreparedStatement).setQueryTimeout(configuredTimeoutSeconds);
  }

  @Test
  void testPreparedStatementUsesOverridenTimeout() throws SQLException {
    int defaultTimeoutSeconds = 30;
    String sqlQuery = "SELECT * FROM test_table";
    Params params = Params.newBuilder().build();

    when(mockConnection.prepareStatement(sqlQuery)).thenReturn(mockPreparedStatement);

    PostgresQueryExecutor executor = new PostgresQueryExecutor(defaultTimeoutSeconds);
    // override timeout to 45s
    executor.buildPreparedStatement(sqlQuery, params, mockConnection, 45);
    verify(mockPreparedStatement).setQueryTimeout(45);
  }
}
