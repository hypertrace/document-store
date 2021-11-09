package org.hypertrace.core.documentstore.expression;

import lombok.Value;
import org.hypertrace.core.documentstore.parser.IGroupingExpressionParser;
import org.hypertrace.core.documentstore.parser.ISelectingExpressionParser;
import org.hypertrace.core.documentstore.parser.ISortingExpressionParser;

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
  public Object parse(IGroupingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(ISelectingExpressionParser parser) {
    return parser.parse(this);
  }

  @Override
  public Object parse(ISortingExpressionParser parser) {
    return parser.parse(this);
  }
}
