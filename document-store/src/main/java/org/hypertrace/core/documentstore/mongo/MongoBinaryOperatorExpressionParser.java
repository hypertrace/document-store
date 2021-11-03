package org.hypertrace.core.documentstore.mongo;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import org.hypertrace.core.documentstore.expression.BinaryOperatorExpression;

// Ref.: https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#operator-expressions
public class MongoBinaryOperatorExpressionParser
    implements MongoExpressionParser<BinaryOperatorExpression> {
  @Override
  public LinkedHashMap<String, Object> parseExpression(BinaryOperatorExpression expression) {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    List<Object> operands =
        Arrays.asList(
            MongoQueryParser.parseExpression(expression.getOperand1()),
            MongoQueryParser.parseExpression(expression.getOperand2()));

    map.put("$" + expression.getOperation().name().toLowerCase(), operands);
    return map;
  }
}
