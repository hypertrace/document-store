package org.hypertrace.core.documentstore.mongo;

import java.util.LinkedHashMap;
import org.hypertrace.core.documentstore.expression.UnaryOperatorExpression;

// Ref.: https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#operator-expressions
public class MongoUnaryOperatorExpressionParser
    extends MongoExpressionParser<UnaryOperatorExpression> {

  public MongoUnaryOperatorExpressionParser(UnaryOperatorExpression expression) {
    super(expression);
  }

  @Override
  public LinkedHashMap<String, Object> parse() {
    LinkedHashMap<String, Object> map = new LinkedHashMap<>();
    map.put(
        "$" + expression.getOperation().name().toLowerCase(),
        MongoQueryParser.parseExpression(expression.getOperand()));
    return map;
  }
}
