package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter;

/**
 * Interface for handling IN operation filters in PostgreSQL queries. Implementations can provide
 * different strategies for handling IN operations based on the context of the query (e.g.,
 * first-class fields vs. JSON fields).
 */
public interface PostgresInRelationalFilterParserInterface extends PostgresRelationalFilterParser {
  // Interface inherits the parse method from PostgresRelationalFilterParser
  // No additional methods required at this time
}
