package org.hypertrace.core.documentstore.query;

import lombok.Value;
import org.hypertrace.core.documentstore.expression.LiteralExpression;
import org.hypertrace.core.documentstore.expression.Projectable;

/**
 * A generic projection definition that supports expressions with aliases (used in the response).
 * For {@link LiteralExpression}, the alias is inferred if not supplied.
 */
@Value(staticConstructor = "of")
public class Projection {
  Projectable projection;
  String alias;

  public static Projection of(final Projectable projection) {
    String alias;

    if (projection instanceof LiteralExpression) {
      alias = ((LiteralExpression) projection).getName();
    } else {
      throw new IllegalArgumentException("Alias is mandatory for projection: " + projection);
    }

    return Projection.of(projection, alias);
  }
}
