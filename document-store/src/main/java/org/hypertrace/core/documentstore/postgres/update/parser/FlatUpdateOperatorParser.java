package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.postgres.update.FlatUpdateContext;

/**
 * Parser interface for converting SubDocumentUpdate operations to SQL fragments for flat
 * collections.
 *
 * <p>Each implementation handles a specific {@link
 * org.hypertrace.core.documentstore.model.subdoc.UpdateOperator} and generates the appropriate SQL
 * SET clause fragment.
 */
public interface FlatUpdateOperatorParser {

  /**
   * Generates SQL SET clause fragment for this operator.
   *
   * <p>For top-level columns, this typically produces: {@code "column" = ?}
   *
   * <p>For nested JSONB paths, this produces: {@code "column" = jsonb_set(...)}
   *
   * @param context The update context containing column info, value, and parameter accumulator
   * @return SQL fragment to be used in SET clause
   */
  String parse(FlatUpdateContext context);
}
