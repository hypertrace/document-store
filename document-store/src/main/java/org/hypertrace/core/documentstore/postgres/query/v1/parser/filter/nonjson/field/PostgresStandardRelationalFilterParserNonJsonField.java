package org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field;

import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresStandardRelationalOperatorMapper;

/**
 * Implementation of standard relational operators (EQ, NEQ, GT, LT, GTE, LTE) for non-JSON fields
 * (regular PostgreSQL typed columns). Uses direct column comparison operators.
 *
 * <p>This class is optimized for first-class typed columns rather than JSON document fields.
 */
public class PostgresStandardRelationalFilterParserNonJsonField
    implements PostgresRelationalFilterParser {

  private static final PostgresStandardRelationalOperatorMapper mapper =
      new PostgresStandardRelationalOperatorMapper();

  @Override
  public String parse(
      final RelationalExpression expression,
      final PostgresRelationalFilterParser.PostgresRelationalFilterContext context) {
    final Object parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    final String operator = mapper.getMapping(expression.getOperator(), parsedRhs);

    if (parsedRhs != null) {
      context.getParamsBuilder().addObjectParam(parsedRhs);
      return String.format("%s %s ?", parsedLhs, operator);
    } else {
      return String.format("%s %s NULL", parsedLhs, operator);
    }
  }
}
