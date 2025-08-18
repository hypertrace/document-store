package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of STARTS_WITH operator for non-JSON fields (regular PostgreSQL text columns).
 * Uses the PostgreSQL LIKE operator with % wildcard for prefix matching on text columns.
 *
 * <p>This class is optimized for first-class text columns rather than JSON document fields.
 */
public class PostgresStartsWithRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    // Add % wildcard to make it a prefix match pattern
    String pattern = parsedRhs + "%";
    context.getParamsBuilder().addObjectParam(pattern);

    // For typed text columns, STARTS_WITH becomes LIKE with % suffix
    return String.format("%s LIKE ?", parsedLhs);
  }
}
