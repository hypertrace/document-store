package org.hypertrace.core.documentstore.postgres.subdoc;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.PostgresQueryBuilder;

public class PostgresSubDocumentUpdater {
  private final PostgresQueryBuilder queryBuilder;
  private final PostgresSubDocumentValueGetter subDocValueGetter =
      new PostgresSubDocumentValueGetter();
  private final Clock clock;

  public PostgresSubDocumentUpdater(final PostgresQueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
    this.clock = Clock.systemUTC();
  }

  public void executeUpdateQuery(
      final Connection connection, final String id, final SubDocumentUpdate update)
      throws SQLException {
    final String updateQuery = queryBuilder.getSubDocUpdateQuery(update.getSubDocumentValue());
    final String subDocPath = formatSubDocPath(update.getSubDocument().getPath());
    final Object value = update.getSubDocumentValue().accept(subDocValueGetter);

    try (final PreparedStatement pStatement = connection.prepareStatement(updateQuery)) {
      pStatement.setString(1, subDocPath);
      pStatement.setObject(2, value);
      pStatement.setString(3, id);

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
