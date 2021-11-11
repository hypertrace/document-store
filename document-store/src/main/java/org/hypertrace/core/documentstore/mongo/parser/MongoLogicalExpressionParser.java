package org.hypertrace.core.documentstore.mongo.parser;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.parser.FilteringExpressionParser;

public class MongoLogicalExpressionParser {

  static Map<String, Object> parse(final LogicalExpression expression) {
    FilteringExpressionParser parser = new MongoFilteringExpressionParser();
    List<Object> parsed =
        expression.getOperands().stream()
            .map(exp -> exp.parse(parser))
            .collect(Collectors.toList());
    String key = "$" + expression.getOperator().name().toLowerCase();
    return Map.of(key, parsed);
  }
}
