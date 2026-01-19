package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;
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
public class FlatCollectionSubDocSetOperatorParser implements
    FlatCollectionSubDocUpdateOperatorParser {

  /** Visitor to extract raw values from SubDocumentValue for use in prepared statements. */
  private static final SubDocumentValueVisitor<Object> VALUE_EXTRACTOR =
      new SubDocumentValueVisitor<>() {
        @Override
        public Object visit(PrimitiveSubDocumentValue value) {
          return value.getValue();
        }

        @Override
        public Object visit(MultiValuedPrimitiveSubDocumentValue value) {
          return value.getValues();
        }

        @Override
        public Object visit(NestedSubDocumentValue value) {
          return value.getJsonValue();
        }

        @Override
        public Object visit(MultiValuedNestedSubDocumentValue value) {
          return value.getJsonValues();
        }

        @Override
        public Object visit(NullSubDocumentValue value) {
          return null;
        }
      };

  @Override
  public String parse(FlatUpdateContext context) {
    if (context.isTopLevel()) {
      return parseTopLevel(context);
    } else {
      return parseNestedJsonb(context);
    }
  }

  private String parseTopLevel(FlatUpdateContext context) {
    context.getParams().add(context.getValue().accept(VALUE_EXTRACTOR));
    return String.format("\"%s\" = ?", context.getColumnName());
  }

  private String parseNestedJsonb(FlatUpdateContext context) {
    String jsonPath = buildJsonPath(context.getNestedPath());
    Object value = context.getValue().accept(VALUE_EXTRACTOR);

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
}
