package org.hypertrace.core.documentstore.mongo.parser;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;

public class MongoFunctionExpressionParser {

  static Map<String, Object> parse(final FunctionExpression expression) {
    int numArgs = expression.getOperands().size();

    if (numArgs == 0) {
      throw new IllegalArgumentException(
          String.format("%s should have at least one operand", expression));
    }

    SelectingExpressionParser parser = new MongoSelectingExpressionParser(true);
    String key = "$" + expression.getOperator().name().toLowerCase();

    if (numArgs == 1) {
      Object value = expression.getOperands().get(0).parse(parser);
      return Map.of(key, value);
    }

    List<Object> values =
        expression.getOperands().stream().map(op -> op.parse(parser)).collect(Collectors.toList());
    return Map.of(key, values);
  }
}
