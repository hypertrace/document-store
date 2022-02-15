package org.hypertrace.core.documentstore.query;

import com.google.common.base.Preconditions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;

/**
 * A generic selection definition that supports expressions with optional aliases (used in the
 * response).
 */
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SelectionSpec {

  SelectTypeExpression expression;

  // Alias is optional. Handling missing aliases can be implemented in the respective parsers
  String alias;

  public static SelectionSpec of(final SelectTypeExpression expression) {
    return SelectionSpec.of(expression, null);
  }

  public static SelectionSpec of(final SelectTypeExpression expression, final String alias) {
    Preconditions.checkArgument(expression != null, "expression is null");
    return new SelectionSpec(expression, alias);
  }
}
