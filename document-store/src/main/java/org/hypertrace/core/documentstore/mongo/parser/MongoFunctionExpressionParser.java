package org.hypertrace.core.documentstore.mongo.parser;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.ABS;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.ADD;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.DIVIDE;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.FLOOR;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.LENGTH;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.MULTIPLY;
import static org.hypertrace.core.documentstore.expression.operators.FunctionOperator.SUBTRACT;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.operators.FunctionOperator;
import org.hypertrace.core.documentstore.parser.SelectingExpressionVisitor;
import org.hypertrace.core.documentstore.query.Query;

final class MongoFunctionExpressionParser extends MongoSelectingExpressionParser {
  private static final Map<FunctionOperator, String> KEY_MAP =
      unmodifiableMap(
          new EnumMap<>(FunctionOperator.class) {
            {
              put(ABS, "$abs");
              put(FLOOR, "$floor");
              put(LENGTH, "$size");
              put(ADD, "$add");
              put(DIVIDE, "$divide");
              put(MULTIPLY, "$multiply");
              put(SUBTRACT, "$subtract");
            }
          });

  MongoFunctionExpressionParser(final Query query) {
    super(query);
  }

  MongoFunctionExpressionParser(final MongoSelectingExpressionParser baseParser) {
    super(baseParser);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Object> visit(final FunctionExpression expression) {
    return parse(expression);
  }

  Map<String, Object> parse(final FunctionExpression expression) {
    int numArgs = expression.getOperands().size();

    if (numArgs == 0) {
      throw new IllegalArgumentException(
          String.format("%s should have at least one operand", expression));
    }

    SelectingExpressionVisitor parser =
        new MongoIdentifierPrefixingSelectingExpressionParser(
            new MongoIdentifierExpressionParser(new MongoConstantExpressionParser(query)));

    FunctionOperator operator = expression.getOperator();
    String key = KEY_MAP.get(operator);

    if (key == null) {
      throw getUnsupportedOperationException(operator);
    }

    if (numArgs == 1) {
      Object value = expression.getOperands().get(0).visit(parser);
      return Map.of(key, value);
    }

    List<Object> values =
        expression.getOperands().stream().map(op -> op.visit(parser)).collect(Collectors.toList());
    return Map.of(key, values);
  }
}
