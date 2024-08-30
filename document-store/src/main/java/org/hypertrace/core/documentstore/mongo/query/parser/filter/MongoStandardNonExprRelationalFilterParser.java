package org.hypertrace.core.documentstore.mongo.query.parser.filter;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
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

    // using HashMap instead of Map.of() as RHS value can be null
    // but Map.of() doesn't support null values
    final Map<String, Object> operatorToRhsMap = new HashMap<>();
    operatorToRhsMap.put(operator, parsedRhs);
    return Map.of(parsedLhs, unmodifiableMap(operatorToRhsMap));
  }
}
