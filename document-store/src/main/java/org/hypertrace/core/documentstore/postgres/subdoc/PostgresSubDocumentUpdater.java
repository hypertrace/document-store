package org.hypertrace.core.documentstore.postgres.subdoc;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.enrichPreparedStatementWithParams;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Clock;
import java.util.Collection;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.PostgresQueryBuilder;
import org.hypertrace.core.documentstore.query.Query;

public class PostgresSubDocumentUpdater {
  private final PostgresQueryBuilder queryBuilder;
  private final Clock clock;

  public PostgresSubDocumentUpdater(final PostgresQueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
    this.clock = Clock.systemUTC();
  }

  public void executeUpdateQuery(
      final Connection connection, final String id, final Collection<SubDocumentUpdate> updates)
      throws SQLException {
    final KeyExpression idFilter = KeyExpression.from(id);
    final Query query = Query.builder().setFilter(idFilter).build();
    executeUpdateQuery(connection, query, updates);
  }

  public void executeUpdateQuery(
      final Connection connection, final Query query, final Collection<SubDocumentUpdate> updates)
      throws SQLException {
    final SubDocumentUpdate lastUpdatedTimeUpdate =
        SubDocumentUpdate.of(SubDocument.implicitUpdatedTime(), clock.millis());
    final Collection<SubDocumentUpdate> allUpdates =
        Stream.concat(updates.stream(), Stream.of(lastUpdatedTimeUpdate))
            .collect(toUnmodifiableList());
    final Params.Builder paramsBuilder = Params.newBuilder();
    final String updateQuery = queryBuilder.getSubDocUpdateQuery(query, allUpdates, paramsBuilder);

    try (final PreparedStatement pStatement = connection.prepareStatement(updateQuery)) {
      enrichPreparedStatementWithParams(pStatement, paramsBuilder.build());
      pStatement.executeUpdate();
    }
  }
}
