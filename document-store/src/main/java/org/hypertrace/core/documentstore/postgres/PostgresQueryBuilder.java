package org.hypertrace.core.documentstore.postgres;

import static org.hypertrace.core.documentstore.postgres.PostgresCollection.DOCUMENT;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;

import lombok.RequiredArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

@RequiredArgsConstructor
public class PostgresQueryBuilder {
  private final String collectionName;
  private final PostgresSubDocumentValueParser subDocValueParser =
      new PostgresSubDocumentValueParser();

  public String getSubDocUpdateQuery(final SubDocumentValue subDocValue) {
    return String.format(
        "UPDATE %s SET %s=jsonb_set(%s, ?::text[], %s) WHERE %s=?",
        collectionName, DOCUMENT, DOCUMENT, subDocValue.accept(subDocValueParser), ID);
  }

  public String getFindByIdQuery() {
    return String.format("SELECT * FROM %s WHERE %s=?", collectionName, ID);
  }
}
