package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoStandardExprRelationalFilterParser implements MongoRelationalFilterParser {
  public static final String EXPR = "$expr";

  private static final MongoStandardRelationalOperatorMapping mapping =
      new MongoStandardRelationalOperatorMapping();

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final String parsedLhs = expression.getLhs().accept(context.lhsParser());
    final String operator = mapping.getOperator(expression.getOperator());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(operator, new Object[] {parsedLhs, parsedRhs});
  }
}
