package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoStandardNonExprRelationalFilterParser implements MongoRelationalFilterParser {
  private static final MongoStandardRelationalOperatorMapping mapping =
      new MongoStandardRelationalOperatorMapping();

  @Override
  public Map<String, Object> parse(
      RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final String operator = mapping.getOperator(expression.getOperator());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(parsedLhs, Map.of(operator, parsedRhs));
  }
}
