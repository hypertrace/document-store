package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.postgres.update.FlatUpdateContext;

/**
 * Parser for the SET operator in flat collections.
 *
 * <p>Handles two cases:
 *
 * <ul>
 *   <li>Top-level columns: {@code SET "column" = ?}
 *   <li>Nested JSONB paths: {@code SET "column" = jsonb_set(COALESCE("column", '{}'), '{path}',
 *       to_jsonb(?))}
 * </ul>
 */
public class FlatSetOperatorParser implements FlatUpdateOperatorParser {

  @Override
  public String parse(FlatUpdateContext context) {
    if (context.isTopLevel()) {
      return parseTopLevel(context);
    } else {
      return parseNestedJsonb(context);
    }
  }

  private String parseTopLevel(FlatUpdateContext context) {
    context.getParams().add(extractValue(context.getValue()));
    return String.format("\"%s\" = ?", context.getColumnName());
  }

  private String parseNestedJsonb(FlatUpdateContext context) {
    String jsonPath = buildJsonPath(context.getNestedPath());
    Object value = extractValue(context.getValue());

    context.getParams().add(jsonPath);
    context.getParams().add(value);

    // Use jsonb_set with COALESCE to handle null columns
    // to_jsonb(?) converts the value to proper JSONB format
    return String.format(
        "\"%s\" = jsonb_set(COALESCE(\"%s\", '{}'), ?::text[], to_jsonb(?))",
        context.getColumnName(), context.getColumnName());
  }

  /**
   * Builds a PostgreSQL text array path from nested path components. For example, ["seller",
   * "name"] becomes "{seller,name}"
   */
  private String buildJsonPath(String[] nestedPath) {
    return "{" + String.join(",", nestedPath) + "}";
  }

  /** Extracts the raw value from SubDocumentValue for use in prepared statements. */
  private Object extractValue(SubDocumentValue value) {
    if (value instanceof PrimitiveSubDocumentValue) {
      return ((PrimitiveSubDocumentValue) value).getValue();
    } else if (value instanceof MultiValuedPrimitiveSubDocumentValue) {
      return ((MultiValuedPrimitiveSubDocumentValue) value).getValues();
    } else if (value instanceof NestedSubDocumentValue) {
      return ((NestedSubDocumentValue) value).getJsonValue();
    }
    throw new UnsupportedOperationException(
        "Unsupported SubDocumentValue type: " + value.getClass().getSimpleName());
  }
}
