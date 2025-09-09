package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

/**
 * Note: MongoDB does not have a native "NOT IN" aggregation operator. This parser simulates "NOT
 * IN" functionality by combining the $not and $in operators.
 */
@AllArgsConstructor
public class MongoStandardExprNotInRelationalFilterParser implements MongoRelationalFilterParser {
  private static final String NOT_OP = "$not";
  private static final String IN_OP = "$in";
  private static final MongoStandardRelationalOperatorMapping mapping =
      new MongoStandardRelationalOperatorMapping();

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(NOT_OP, Map.of(IN_OP, new Object[] {parsedLhs, parsedRhs}));
  }
}
