package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;

/**
 * Implementation of LIKE operator for non-JSON fields (regular PostgreSQL text columns). Uses the
 * PostgreSQL LIKE operator for pattern matching on text columns.
 *
 * <p>This class is optimized for first-class text columns rather than JSON document fields.
 */
public class PostgresLikeRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());

    context.getParamsBuilder().addObjectParam(parsedRhs);

    // For typed text columns, LIKE is a direct operator
    return String.format("%s LIKE ?", parsedLhs);
  }
}
