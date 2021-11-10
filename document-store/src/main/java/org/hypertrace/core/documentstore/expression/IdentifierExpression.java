package org.hypertrace.core.documentstore.expression;

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
@Value(staticConstructor = "of")
public class IdentifierExpression
    implements GroupingExpression, SelectingExpression, SortingExpression {
  String name;

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
