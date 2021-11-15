package org.hypertrace.core.documentstore.expression.impl;

import java.util.List;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.collections4.CollectionUtils;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
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

  public static ConstantExpression of(Boolean value) {
    return new ConstantExpression(value);
  }

  public static ConstantExpression ofStrings(List<String> values) {
    return validateAndReturn(values);
  }

  public static ConstantExpression ofNumbers(List<? extends Number> values) {
    return validateAndReturn(values);
  }

  public static ConstantExpression ofBooleans(List<Boolean> values) {
    return validateAndReturn(values);
  }

  private static ConstantExpression validateAndReturn(List<?> values) {
    if (CollectionUtils.isEmpty(values)) {
      throw new IllegalArgumentException(
          "At least one value must be present in ConstantExpression");
    }

    return new ConstantExpression(values);
  }

  @Override
  public Object parse(SelectingExpressionParser parser) {
    return parser.parse(this);
  }
}
