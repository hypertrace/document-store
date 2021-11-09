package org.hypertrace.core.documentstore.expression;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;

/**
 * Expression representing either a string constant, a numeric constant or a list of string/numeric
 * constants.
 *
 * <p>Example: <code>
 *    ConstantExpression.of(5);           // Numeric constant
 *    ConstantExpression.of("const");     // String constant
 *  </code>
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ConstantExpression implements SelectingExpression {
  Object value;

  public static ConstantExpression of(String value) {
    return new ConstantExpression(value);
  }

  public static ConstantExpression of(Number value) {
    return new ConstantExpression(value);
  }

  public static ConstantExpression ofStrings(List<String> value) {
    return new ConstantExpression(value);
  }

  public static ConstantExpression ofNumbers(List<? extends Number> value) {
    return new ConstantExpression(value);
  }

  @Override
  public Object parse(SelectingExpressionParser parser) {
    return parser.parse(this);
  }
}
