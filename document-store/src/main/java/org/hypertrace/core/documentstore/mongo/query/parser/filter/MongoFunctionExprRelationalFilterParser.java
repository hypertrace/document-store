package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoStandardExprRelationalFilterParser.EXPR;

import java.util.Map;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoFunctionExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.MongoSelectTypeExpressionParser;
import org.hypertrace.core.documentstore.mongo.query.parser.filter.MongoRelationalFilterParserFactory.MongoRelationalFilterContext;

@AllArgsConstructor
public class MongoFunctionExprRelationalFilterParser implements MongoRelationalFilterParser {
  private static final MongoStandardRelationalOperatorMapping mapping =
      new MongoStandardRelationalOperatorMapping();

  private static final MongoSelectTypeExpressionParser functionParser =
      new MongoFunctionExpressionParser();
  private static final MongoStandardNonExprRelationalFilterParser
      mongoStandardNonExprRelationalFilterParser = new MongoStandardNonExprRelationalFilterParser();

  @Override
  public Map<String, Object> parse(
      final RelationalExpression expression, final MongoRelationalFilterContext context) {
    final Object parsedLhs;

    try {
      parsedLhs = expression.getLhs().accept(functionParser);
    } catch (final UnsupportedOperationException e) {
      return mongoStandardNonExprRelationalFilterParser.parse(expression, context);
    }

    final String operator = mapping.getOperator(expression.getOperator());
    final Object parsedRhs = expression.getRhs().accept(context.rhsParser());
    return Map.of(EXPR, Map.of(operator, new Object[] {parsedLhs, parsedRhs}));
  }
}
