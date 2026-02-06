package org.hypertrace.core.documentstore.postgres.update.parser;

import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;
import org.hypertrace.core.documentstore.postgres.update.FlatUpdateContext;

/**
 * Parser for the ADD operator in flat collections.
 *
 * <p>ADD increments a numeric field by the given value. Handles two cases:
 *
 * <ul>
 *   <li>Top-level numeric columns: {@code "column" = COALESCE("column", 0) + ?}
 *   <li>Nested JSONB paths: {@code "column" = jsonb_set(COALESCE("column", '{}'), '{path}',
 *       (COALESCE("column"->>'path', '0')::float + ?::float)::text::jsonb, true)}
 * </ul>
 */
public class FlatCollectionSubDocAddOperatorParser
    implements FlatCollectionSubDocUpdateOperatorParser {

  /** Visitor to extract numeric values from SubDocumentValue. */
  private static final SubDocumentValueVisitor<Number> NUMERIC_VALUE_EXTRACTOR =
      new SubDocumentValueVisitor<>() {
        @Override
        public Number visit(PrimitiveSubDocumentValue value) {
          Object val = value.getValue();
          if (val instanceof Number) {
            return (Number) val;
          }
          throw new IllegalArgumentException(
              "ADD operator requires a numeric value, got: " + val.getClass().getName());
        }

        @Override
        public Number visit(
            org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue
                value) {
          throw new IllegalArgumentException("ADD operator does not support multi-valued updates");
        }

        @Override
        public Number visit(
            org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue value) {
          throw new IllegalArgumentException(
              "ADD operator does not support nested document values");
        }

        @Override
        public Number visit(
            org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue
                value) {
          throw new IllegalArgumentException(
              "ADD operator does not support multi-valued nested documents");
        }

        @Override
        public Number visit(
            org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue value) {
          throw new IllegalArgumentException("ADD operator does not support null values");
        }
      };

  @Override
  public String parse(FlatUpdateContext context) {
    validateNumericValue(context.getValue());

    if (context.isTopLevel()) {
      return parseTopLevel(context);
    } else {
      return parseNestedJsonb(context);
    }
  }

  private void validateNumericValue(SubDocumentValue value) {
    // This will throw if the value is not numeric
    value.accept(NUMERIC_VALUE_EXTRACTOR);
  }

  /**
   * Generates SQL for adding to a top-level numeric column.
   *
   * <p>Output: {@code "column" = COALESCE("column", 0) + ?::type}
   */
  private String parseTopLevel(FlatUpdateContext context) {
    Number value = context.getValue().accept(NUMERIC_VALUE_EXTRACTOR);
    context.getParams().add(value);

    String typeCast = getPostgresTypeCast(context);
    return String.format(
        "\"%s\" = COALESCE(\"%s\", 0) + ?%s",
        context.getColumnName(), context.getColumnName(), typeCast);
  }

  /** Returns the PostgreSQL type cast for the column type. */
  private String getPostgresTypeCast(FlatUpdateContext context) {
    if (context.getColumnType() == null) {
      return "";
    }
    switch (context.getColumnType()) {
      case INTEGER:
        return "::integer";
      case BIGINT:
        return "::bigint";
      case REAL:
        return "::real";
      case DOUBLE_PRECISION:
        return "::double precision";
      default:
        return "";
    }
  }

  /**
   * Generates SQL for adding to a numeric field within a JSONB column. Infers the numeric type from
   * the value to preserve integer precision when possible.
   *
   * <p>Output for integers: {@code "column" = jsonb_set(COALESCE("column", '{}'), ?::text[],
   * (COALESCE("column"#>>'{path}', '0')::bigint + ?::bigint)::text::jsonb, true)}
   *
   * <p>Output for floats: {@code "column" = jsonb_set(COALESCE("column", '{}'), ?::text[],
   * (COALESCE("column"#>>'{path}', '0')::double precision + ?::double precision)::text::jsonb,
   * true)}
   */
  private String parseNestedJsonb(FlatUpdateContext context) {
    String jsonPath = buildJsonPath(context.getNestedPath());
    Number value = context.getValue().accept(NUMERIC_VALUE_EXTRACTOR);

    // Infer type from value to preserve precision
    String sqlType = inferSqlTypeFromValue(value);

    // Add params: jsonPath, value
    context.getParams().add(jsonPath);
    context.getParams().add(value);

    // Extracts nested JSONB value as text, e.g., "metrics"#>>'{sales,total}' traverses
    // metrics→sales→total
    String fieldAccessor = String.format("\"%s\"#>>'%s'", context.getColumnName(), jsonPath);

    // jsonb_set with arithmetic using inferred type
    return String.format(
        "\"%s\" = jsonb_set(COALESCE(\"%s\", '{}'), ?::text[], (COALESCE(%s, '0')::%s + ?::%s)::text::jsonb, true)",
        context.getColumnName(), context.getColumnName(), fieldAccessor, sqlType, sqlType);
  }

  /** Infers PostgreSQL type from the Java Number type. */
  private String inferSqlTypeFromValue(Number value) {
    if (value instanceof Integer || value instanceof Short || value instanceof Byte) {
      return "integer";
    } else if (value instanceof Long) {
      return "bigint";
    } else {
      // Float, Double, BigDecimal - use double precision for safety
      return "double precision";
    }
  }

  /**
   * Builds a PostgreSQL text array path from nested path components. For example, ["seller",
   * "count"] becomes "{seller,count}"
   */
  private String buildJsonPath(String[] nestedPath) {
    return "{" + String.join(",", nestedPath) + "}";
  }
}
