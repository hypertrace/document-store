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
  public void parse(IGroupingExpressionParser parser) {
    parser.parse(this);
  }

  @Override
  public void parse(ISelectingExpressionParser parser) {
    parser.parse(this);
  }

  @Override
  public void parse(ISortingExpressionParser parser) {
    parser.parse(this);
  }
}
