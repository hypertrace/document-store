package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of NOT_EXISTS operator for non-JSON fields (regular PostgreSQL typed columns).
 * Uses the PostgreSQL IS NULL operator for checking if a column value does not exist.
 *
 * <p>This class is optimized for first-class typed columns rather than JSON document fields.
 */
public class PostgresNotExistsRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    // For typed columns, NOT_EXISTS is simply IS NULL
    return String.format("%s IS NULL", parsedLhs);
  }
}
