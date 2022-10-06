package org.hypertrace.core.documentstore.postgres.subdoc;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.PostgresQueryBuilder;

public class PostgresSubDocumentUpdater {
  private final PostgresQueryBuilder queryBuilder;
  private final Clock clock;

  public PostgresSubDocumentUpdater(final PostgresQueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
    this.clock = Clock.systemUTC();
  }

  public void executeUpdateQuery(
      final Connection connection, final String id, final SubDocumentUpdate update)
      throws SQLException {
    final Params.Builder paramsBuilder = Params.newBuilder();
    final String updateQuery = queryBuilder.getSubDocUpdateQuery(update, id, paramsBuilder);

    try (final PreparedStatement pStatement = connection.prepareStatement(updateQuery)) {
      enrichPreparedStatementWithParams(pStatement, paramsBuilder.build());
      pStatement.executeUpdate();
    }
  }

  public void updateLastUpdatedTime(final Connection connection, final String id)
      throws SQLException {
    final SubDocumentUpdate lastUpdatedTimeUpdate =
        SubDocumentUpdate.of(SubDocument.implicitUpdatedTime(), clock.millis());
    executeUpdateQuery(connection, id, lastUpdatedTimeUpdate);
  }
}
