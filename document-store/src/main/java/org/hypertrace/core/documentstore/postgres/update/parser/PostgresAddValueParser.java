package org.hypertrace.core.documentstore.postgres.update.parser;

import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.formatSubDocPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareFieldDataAccessorExpr;

import org.hypertrace.core.documentstore.model.subdoc.MultiValuedNestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.MultiValuedPrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NestedSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.NullSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.PrimitiveSubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.visitor.SubDocumentValueVisitor;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
import org.hypertrace.core.documentstore.postgres.subdoc.PostgresSubDocumentValueParser;

public class PostgresAddValueParser implements PostgresUpdateOperationParser {

  /** Visitor to validate and extract numeric values from SubDocumentValue. */
  private static final SubDocumentValueVisitor<Number> NUMERIC_VALUE_VALIDATOR =
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
        public Number visit(MultiValuedPrimitiveSubDocumentValue value) {
          throw new IllegalArgumentException("ADD operator does not support multi-valued updates");
        }

        @Override
        public Number visit(NestedSubDocumentValue value) {
          throw new IllegalArgumentException(
              "ADD operator does not support nested document values");
        }

        @Override
        public Number visit(MultiValuedNestedSubDocumentValue value) {
          throw new IllegalArgumentException(
              "ADD operator does not support multi-valued nested documents");
        }

        @Override
        public Number visit(NullSubDocumentValue value) {
          throw new IllegalArgumentException("ADD operator does not support null values");
        }
      };

  @Override
  public String parseNonJsonbField(UpdateParserInput input) {
    // Validate that the value is numeric
    SubDocumentValue value = input.getUpdate().getSubDocumentValue();
    value.accept(NUMERIC_VALUE_VALIDATOR);

    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramsBuilder);

    // Add the numeric value to params
    value.accept(valueParser);

    // Generate: "column" = COALESCE("column", 0) + ?::type
    PostgresDataType columnType = input.getColumnType();
    String typeCast = (columnType != null) ? columnType.getTypeCast() : "";
    return String.format(
        "\"%s\" = COALESCE(\"%s\", 0) + ?%s", input.getBaseField(), input.getBaseField(), typeCast);
  }

  @Override
  public String parseInternal(UpdateParserInput input) {
    return new PostgresSetValueParser(this, 1).parseInternal(input);
  }

  @Override
  public String parseLeaf(UpdateParserInput input) {
    final Params.Builder paramsBuilder = input.getParamsBuilder();
    final PostgresSubDocumentValueParser valueParser =
        new PostgresSubDocumentValueParser(paramsBuilder);

    paramsBuilder.addObjectParam(formatSubDocPath(input.getPath()[0]));
    final String parsedValue = input.getUpdate().getSubDocumentValue().accept(valueParser);
    final String fieldAccess =
        prepareFieldDataAccessorExpr(input.getPath()[0], input.getBaseField());
    return String.format(
        "jsonb_set(COALESCE(%s, '{}'), ?::text[], (COALESCE(%s, '0')::float + %s::float)::text::jsonb)",
        input.getBaseField(), fieldAccess, parsedValue);
  }
}
