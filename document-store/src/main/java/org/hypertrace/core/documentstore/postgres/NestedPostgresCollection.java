package org.hypertrace.core.documentstore.postgres;

import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.model.options.QueryOptions;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.transformer.NestedPostgresColTransformer;
import org.hypertrace.core.documentstore.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PostgreSQL collection implementation for legacy document storage mode. Fields are stored within
 * JSONB columns.
 */
public class NestedPostgresCollection extends PostgresCollection {

  private static final Logger LOGGER = LoggerFactory.getLogger(NestedPostgresCollection.class);

  NestedPostgresCollection(final PostgresClient client, final String collectionName) {
    super(client, collectionName);
  }

  @Override
  public CloseableIterator<Document> query(
      final org.hypertrace.core.documentstore.query.Query query, final QueryOptions queryOptions) {
    PostgresQueryParser queryParser = createParser(query);
    return queryWithParser(query, queryParser);
  }

  @Override
  public CloseableIterator<Document> find(
      final org.hypertrace.core.documentstore.query.Query query) {
    PostgresQueryParser queryParser = createParser(query);
    return queryWithParser(query, queryParser);
  }

  @Override
  public long count(
      org.hypertrace.core.documentstore.query.Query query, QueryOptions queryOptions) {
    PostgresQueryParser queryParser = createParser(query);
    return countWithParser(query, queryParser);
  }

  private PostgresQueryParser createParser(Query query) {
    return new PostgresQueryParser(
        tableIdentifier,
        PostgresQueryExecutor.transformAndLog(query),
        new NestedPostgresColTransformer());
  }
}
