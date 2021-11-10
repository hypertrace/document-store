package org.hypertrace.core.documentstore.expression.impl;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotBlank;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.GroupingExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;
import org.hypertrace.core.documentstore.expression.type.SortingExpression;
import org.hypertrace.core.documentstore.parser.GroupingExpressionParser;
import org.hypertrace.core.documentstore.parser.SelectingExpressionParser;
import org.hypertrace.core.documentstore.parser.SortingExpressionParser;

/**
 * Expression representing either a literal (or a column name)
 *
 * <p>Example: LiteralExpression.of("col1");
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class IdentifierExpression
    implements GroupingExpression, SelectingExpression, SortingExpression {

  @NotBlank String name;

  public static IdentifierExpression of(String name) {
    return validateAndReturn(new IdentifierExpression(name));
  }

  @Override
  public Object parse(GroupingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(SelectingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(SortingExpressionParser parser) {
    return parser.parse(this);
  }
}
