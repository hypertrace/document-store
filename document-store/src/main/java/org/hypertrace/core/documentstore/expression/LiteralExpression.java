package org.hypertrace.core.documentstore.expression;

import lombok.Value;

/**
 * Expression representing either a literal (or a column name)
 *
 * <p>Example: LiteralExpression.of("col1");
 */
@Value(staticConstructor = "of")
public class LiteralExpression implements Groupable, Projectable, Sortable {
  String name;
}
