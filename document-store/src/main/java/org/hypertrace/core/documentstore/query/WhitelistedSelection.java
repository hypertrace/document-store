package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;

/**
 * A generic selection definition that supports expressions with optional aliases (used in the
 * response).
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class WhitelistedSelection implements Selection {

  @NotNull SelectingExpression expression;

  // Alias is optional. Handling missing aliases can be implemented in the respective parsers
  String alias;

  @Override
  public boolean isAggregation() {
    return expression instanceof AggregateExpression;
  }

  @Override
  public boolean allColumnsSelected() {
    return false;
  }

  public static Selection of(final SelectingExpression expression) {
    return WhitelistedSelection.of(expression, null);
  }

  public static Selection of(final SelectingExpression expression, final String alias) {
    return validateAndReturn(new WhitelistedSelection(expression, alias));
  }
}
