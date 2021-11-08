package org.hypertrace.core.documentstore.query;

import lombok.Value;
import org.hypertrace.core.documentstore.expression.Sortable;

/**
 * A generic ORDER BY definition that supports expressions. Note that this class is a more general
 * version of {@link org.hypertrace.core.documentstore.OrderBy}
 */
@Value(staticConstructor = "of")
public class GenericOrderBy {
  Sortable sortable;
  boolean ascending;
}
