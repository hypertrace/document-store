package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

@RequiredArgsConstructor
public class PostgresQueryBuilder {
  private final String collectionName;
  private final PostgresSubDocumentValueParser subDocValueParser = new PostgresSubDocumentValueParser();

  public String buildSelectQueryForUpdateSkippingLocked(
      org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser parser) {
    final String selections = ID + ", " + parser.getSelections();
    final Optional<String> optionalOrderBy = parser.parseOrderBy();
    final Optional<String> optionalFilter = parser.parseFilter();

    final StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("SELECT ").append(selections);
    queryBuilder.append(" FROM ").append(collectionName);
    optionalFilter.ifPresent(filter -> queryBuilder.append(" WHERE ").append(filter));
    optionalOrderBy.ifPresent(orderBy -> queryBuilder.append(" ORDER BY ").append(orderBy));
    queryBuilder.append(" LIMIT 1");
    queryBuilder.append(" FOR UPDATE SKIP LOCKED");

    return queryBuilder.toString();
  }

  public String getSubDocUpdateQuery(final SubDocumentValue subDocValue) {
    return String.format(
        "UPDATE %s SET %s=jsonb_set(%s, ?::text[], %s) WHERE %s=?",
        collectionName,
        DOCUMENT,
        DOCUMENT,
        subDocValue.accept(subDocValueParser),
        ID);
  }
}
