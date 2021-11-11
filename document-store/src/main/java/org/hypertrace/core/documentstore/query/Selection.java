package org.hypertrace.core.documentstore.query;

import static org.hypertrace.core.documentstore.expression.Utils.validateAndReturn;

import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;

/**
 * A generic selection definition that supports expressions with aliases (used in the response). For
 * {@link IdentifierExpression}, the alias is inferred if not supplied.
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Selection {

  // Special selection representing the selection of all the columns
  public static final Selection ALL = new Selection(ConstantExpression.of((String) null), null);

  @NotNull SelectingExpression expression;

  // Alias is optional. Handling missing aliases can be implemented in the respective parsers
  String alias;

  public boolean isAggregation() {
    return expression instanceof AggregateExpression;
  }

  public static Selection of(final SelectingExpression expression) {
    return Selection.of(expression, null);
  }

  public static Selection of(final SelectingExpression expression, final String alias) {
    return validateAndReturn(new Selection(expression, alias));
  }
}
