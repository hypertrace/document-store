package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of EXISTS operator for non-JSON fields (regular PostgreSQL typed columns). Uses
 * the PostgreSQL IS NOT NULL operator for checking if a column value exists.
 *
 * <p>This class is optimized for first-class typed columns rather than JSON document fields.
 */
public class PostgresExistsRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());

    // For typed columns, EXISTS is simply IS NOT NULL
    return String.format("%s IS NOT NULL", parsedLhs);
  }
}
