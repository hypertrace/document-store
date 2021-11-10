package org.hypertrace.core.documentstore.query;

import lombok.Value;
import org.hypertrace.core.documentstore.expression.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectingExpression;

/**
 * A generic selection definition that supports expressions with aliases (used in the response). For
 * {@link IdentifierExpression}, the alias is inferred if not supplied.
 */
@Value(staticConstructor = "of")
public class Selection {
  SelectingExpression expression;
  String alias;

  public static Selection of(final SelectingExpression expression) {
    String alias;

    if (expression instanceof IdentifierExpression) {
      alias = ((IdentifierExpression) expression).getName();
    } else {
      throw new IllegalArgumentException("Alias is mandatory for expression: " + expression);
    }

    return Selection.of(expression, alias);
  }
}
