package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.NOT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.encodeAliasForNestedField;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParsedNonCompositeFilter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.DataType;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.builder.PostgresSelectExpressionParserBuilder;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.builder.PostgresSelectExpressionParserBuilderImpl;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParserFactoryImpl;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;

public class PostgresFilterTypeExpressionVisitor implements FilterTypeExpressionVisitor {

  protected PostgresQueryParser postgresQueryParser;
  @Nullable private final PostgresWrappingFilterVisitorProvider wrappingVisitorProvider;

  public PostgresFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this(postgresQueryParser, null);
  }

  public PostgresFilterTypeExpressionVisitor(
      PostgresQueryParser postgresQueryParser,
      final PostgresWrappingFilterVisitorProvider wrappingVisitorProvider) {
    this.postgresQueryParser = postgresQueryParser;
    this.wrappingVisitorProvider = wrappingVisitorProvider;
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final LogicalExpression expression) {
    if (NOT.equals(expression.getOperator())) {
      return String.format("NOT (%s)", expression.getOperands().get(0).accept(this).toString());
    }

    Collector<String, ?, String> collector =
        getCollectorForLogicalOperator(expression.getOperator());
    String childList =
        expression.getOperands().stream()
            .map(exp -> exp.accept(this))
            .filter(str -> !StringUtils.isEmpty((String) str))
            .map(str -> "(" + str + ")")
            .collect(collector);
    return !childList.isEmpty() ? childList : null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final RelationalExpression expression) {
    final PostgresSelectExpressionParserBuilder parserBuilder =
        new PostgresSelectExpressionParserBuilderImpl(postgresQueryParser);
    final PostgresSelectTypeExpressionVisitor baseVisitor = parserBuilder.build(expression);
    final PostgresSelectTypeExpressionVisitor lhsVisitor =
        Optional.ofNullable(wrappingVisitorProvider)
            .map(visitor -> visitor.getForRelational(baseVisitor, expression.getRhs()))
            .orElse(baseVisitor);

    final PostgresRelationalFilterContext context =
        PostgresRelationalFilterContext.builder()
            .lhsParser(lhsVisitor)
            .postgresQueryParser(postgresQueryParser)
            .build();

    return new PostgresRelationalFilterParserFactoryImpl()
        .parser(expression, postgresQueryParser)
        .parse(expression, context);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final KeyExpression expression) {
    return expression.getKeys().size() == 1
        ? prepareParsedNonCompositeFilter(
            ID,
            EQ.name(),
            expression.getKeys().get(0).toString(),
            postgresQueryParser.getParamsBuilder())
        : prepareParsedNonCompositeFilter(
            ID,
            IN.name(),
            expression.getKeys().stream().map(Key::toString).collect(toUnmodifiableList()),
            postgresQueryParser.getParamsBuilder());
  }

  @SuppressWarnings({"unchecked", "SwitchStatementWithTooFewBranches"})
  @Override
  public String visit(final ArrayRelationalFilterExpression expression) {
    /*
    EXISTS
     (SELECT 1
      FROM jsonb_array_elements(COALESCE(planets->'elements', '[]'::jsonb)) AS elements
      WHERE TRIM('"' FROM elements::text) = 'Oxygen'
     )
     */
    switch (expression.getOperator()) {
      case ANY:
        return getFilterStringForAnyOperator(expression);
      case ALL:
        return getFilterStringForAllOperator(expression);
      case ONE:
        return getFilterStringForOneOperator(expression);
      default:
        throw new UnsupportedOperationException(
            "Unsupported array operator: " + expression.getOperator());
    }
  }

  @SuppressWarnings({"unchecked", "SwitchStatementWithTooFewBranches"})
  @Override
  public String visit(final DocumentArrayFilterExpression expression) {
    /*
    EXISTS
    (SELECT 1
     FROM  jsonb_array_elements(COALESCE(document->'planets', '[]'::jsonb)) AS planets
     WHERE <parsed_containing_filter_with_aliased_field_names>
     )
     */
    switch (expression.getOperator()) {
      case ANY:
        return getFilterStringForAnyOperator(expression);
      case ALL:
        return getFilterStringForAllOperator(expression);
      case ONE:
        return getFilterStringForOneOperator(expression);
      default:
        throw new UnsupportedOperationException(
            "Unsupported array operator: " + expression.getOperator());
    }
  }

  public static Optional<String> getFilterClause(PostgresQueryParser postgresQueryParser) {
    return prepareFilterClause(postgresQueryParser.getQuery().getFilter(), postgresQueryParser);
  }

  public static Optional<String> prepareFilterClause(
      Optional<FilterTypeExpression> filterTypeExpression,
      PostgresQueryParser postgresQueryParser) {
    return filterTypeExpression.map(
        expression ->
            expression.accept(new PostgresFilterTypeExpressionVisitor(postgresQueryParser)));
  }

  private Collector getCollectorForLogicalOperator(LogicalOperator operator) {
    if (operator.equals(LogicalOperator.OR)) {
      return Collectors.joining(" OR ");
    } else if (operator.equals(LogicalOperator.AND)) {
      return Collectors.joining(" AND ");
    }
    throw new UnsupportedOperationException(
        String.format("Query operation:%s not supported", operator));
  }

  private String getFilterStringForAnyOperator(final ArrayRelationalFilterExpression expression) {
    // Check if this is a flat collection (native PostgreSQL columns) or nested (JSONB)
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    boolean isJsonbArray = expression.getArraySource() instanceof JsonIdentifierExpression;

    // Extract the field name
    final String identifierName =
        expression
            .getArraySource()
            .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));

    final String parsedLhs;
    if (isFlatCollection && !isJsonbArray) {
      // For flat collections with native arrays, use direct column reference
      parsedLhs = postgresQueryParser.transformField(identifierName).getPgColumn();
    } else {
      // For nested collections OR JSONB arrays in flat collections, use JSONB path accessor
      // Convert 'elements' to planets->'elements' where planets could be an alias for an upper
      // level array filter
      // For the first time (if 'elements' was not under any nested array, say a top-level field),
      // use the field identifier visitor to make it document->'elements' or props->'colors'
      final PostgresIdentifierExpressionVisitor identifierVisitor =
          new PostgresIdentifierExpressionVisitor(postgresQueryParser);
      final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
          wrappingVisitorProvider == null
              ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
              : wrappingVisitorProvider.getForNonRelational(identifierVisitor);
      parsedLhs = expression.getArraySource().accept(arrayPathVisitor);
    }

    // If the field name is 'elements.inner', alias becomes 'elements_dot_inner'
    final String alias = encodeAliasForNestedField(identifierName).toLowerCase();

    // Any LHS field name (elements) is to be prefixed with current alias (elements_dot_inner)
    final PostgresWrappingFilterVisitorProvider visitorProvider =
        new PostgresArrayRelationalWrappingFilterVisitorProvider(
            postgresQueryParser, identifierName, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(new PostgresFilterTypeExpressionVisitor(postgresQueryParser, visitorProvider));

    if (isFlatCollection && !isJsonbArray) {
      // todo: For array filters, UNNEST is not the most optimal way as it won't use the index.
      // Perhaps, we should use ANY or @> ARRAY operator

      // For flat collections with native arrays (e.g., tags), use unnest()
      // Infer array type from filter to properly cast empty array
      String arrayTypeCast = inferArrayTypeCastFromFilter(expression.getFilter());
      return String.format(
          "EXISTS (SELECT 1 FROM unnest(COALESCE(%s, ARRAY[]%s)) AS \"%s\" WHERE %s)",
          parsedLhs, arrayTypeCast, alias, parsedFilter);
    } else {
      // For nested collections OR JSONB arrays in flat collections, use jsonb_array_elements()
      return String.format(
          "EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(%s) = 'array' THEN %s ELSE '[]'::jsonb END) AS \"%s\" WHERE %s)",
          parsedLhs, parsedLhs, alias, parsedFilter);
    }
  }

  /**
   * Infers the PostgreSQL array type cast from the filter expression by examining the constant
   * value being compared.
   *
   * <p>This method addresses PostgreSQL's requirement that empty array literals (ARRAY[]) must have
   * an explicit type cast. It infers the array element type by inspecting the filter's constant
   * value.
   *
   * <p><b>Supported cases:</b>
   *
   * <ul>
   *   <li>String constants → ::text[] (e.g., "hygiene" → TEXT[])
   *   <li>Integer/Long constants → ::bigint[] (e.g., 42 → BIGINT[])
   *   <li>Double/Float constants → ::double precision[] (e.g., 3.14 → DOUBLE PRECISION[])
   *   <li>Boolean constants → ::boolean[] (e.g., true → BOOLEAN[])
   * </ul>
   *
   * <p><b>Limitations:</b> Type inference fails and defaults to ::text[] when:
   *
   * <ul>
   *   <li>Filter is a LogicalExpression (AND/OR) rather than RelationalExpression
   *   <li>Filter compares against an IdentifierExpression instead of a ConstantExpression
   *   <li>Constant value type is not recognized (e.g., custom types)
   * </ul>
   *
   * @param filter The filter expression to analyze
   * @return PostgreSQL array type cast string (e.g., "::text[]", "::bigint[]")
   */
  private String inferArrayTypeCastFromFilter(FilterTypeExpression filter) {
    // If the filter is a RelationalExpression, check the RHS for a constant value
    if (filter instanceof RelationalExpression) {
      RelationalExpression relExpr = (RelationalExpression) filter;

      // Try to get the constant value directly if it's a ConstantExpression
      if (relExpr.getRhs() instanceof ConstantExpression) {
        ConstantExpression constExpr = (ConstantExpression) relExpr.getRhs();
        Object value = constExpr.getValue();

        PostgresDataType pgType = PostgresDataType.fromJavaValue(value);
        if (pgType != PostgresDataType.UNKNOWN) {
          return pgType.getArrayTypeCast();
        }
      }
    }

    // Default to text[] if we can't infer the type
    return PostgresDataType.TEXT.getArrayTypeCast();
  }

  private String getFilterStringForAnyOperator(final DocumentArrayFilterExpression expression) {
    // Check if this is a flat collection (native PostgreSQL columns) or nested (JSONB)
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    boolean isJsonbArray = expression.getArraySource() instanceof JsonIdentifierExpression;

    // Extract the field name
    final String identifierName =
        expression
            .getArraySource()
            .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));

    final String parsedLhs;
    if (isFlatCollection && !isJsonbArray) {
      // For flat collections with native arrays, use direct column reference with double quotes
      parsedLhs = postgresQueryParser.transformField(identifierName).getPgColumn();
    } else {
      // For nested collections OR JSONB arrays in flat collections, use JSONB path accessor
      final PostgresIdentifierExpressionVisitor identifierVisitor =
          new PostgresIdentifierExpressionVisitor(postgresQueryParser);
      final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
          wrappingVisitorProvider == null
              ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
              : wrappingVisitorProvider.getForNonRelational(identifierVisitor);
      parsedLhs = expression.getArraySource().accept(arrayPathVisitor);
    }

    // If the field name is 'elements.inner', alias becomes 'elements_dot_inner'
    final String alias = encodeAliasForNestedField(identifierName);

    // Any LHS field name (elements) is to be prefixed with current alias (elements_dot_inner)
    final PostgresWrappingFilterVisitorProvider wrapper =
        new PostgresDocumentArrayWrappingFilterVisitorProvider(postgresQueryParser, alias);
    final String parsedFilter =
        expression
            .getFilter()
            .accept(new PostgresFilterTypeExpressionVisitor(postgresQueryParser, wrapper));

    if (isFlatCollection && !isJsonbArray) {
      // For flat collections with native arrays, use unnest()
      // Note: DocumentArrayFilterExpression typically works with JSONB arrays containing objects
      // For simplicity, we default to text[] type cast, though this may need refinement
      String arrayTypeCast = "::text[]";
      return String.format(
          "EXISTS (SELECT 1 FROM unnest(COALESCE(%s, ARRAY[]%s)) AS \"%s\" WHERE %s)",
          parsedLhs, arrayTypeCast, alias, parsedFilter);
    } else {
      // For nested collections OR JSONB arrays in flat collections, use jsonb_array_elements()
      // Use jsonb_typeof() to handle JSON null values - COALESCE only handles SQL NULL
      return String.format(
          "EXISTS (SELECT 1 FROM jsonb_array_elements(CASE WHEN jsonb_typeof(%s) = 'array' THEN %s ELSE '[]'::jsonb END) AS \"%s\" WHERE %s)",
          parsedLhs, parsedLhs, alias, parsedFilter);
    }
  }

  /*
  Native array (flat collection):
    tags @> ?   -- bound as a typed array param, e.g. ['Blue','Green']
    No COALESCE: a NULL array makes the predicate evaluate to NULL, which excludes the row
    anyway, and the unwrapped column reference keeps the filter GIN-indexable (SARGable).
    The element type comes from the compile-time type info on the field expression
    (ArrayIdentifierExpression), falling back to inference from the filter values.

  JSONB array (nested collection or JSONB column in a flat collection):
    (CASE WHEN jsonb_typeof(colors) = 'array' THEN colors ELSE '[]'::jsonb END) @> ?::jsonb
                                            -- bound as the JSON text '["Blue","Green"]'
    JSONB is schemaless, so the runtime jsonb_typeof guard is retained here to tolerate
    JSON null / non-array values; top-level native array columns need no such check.
   */
  private String getFilterStringForAllOperator(final ArrayFilterExpression expression) {
    final List<?> values = getArrayOperatorFilterValues(expression);
    final ArrayFieldContext fieldContext = getArrayFieldContext(expression);

    if (fieldContext.isNativeArray()) {
      final PostgresDataType dataType =
          resolvePostgresDataType(expression.getArraySource(), values);
      postgresQueryParser.getParamsBuilder().addArrayParam(values.toArray(), dataType.getSqlType());
      return String.format("%s @> ?", fieldContext.parsedLhs());
    }

    final String guardedArray =
        String.format(
            "(CASE WHEN jsonb_typeof(%s) = 'array' THEN %s ELSE '[]'::jsonb END)",
            fieldContext.parsedLhs(), fieldContext.parsedLhs());
    postgresQueryParser.getParamsBuilder().addObjectParam(toJsonArrayString(values));
    return String.format("%s @> ?::jsonb", guardedArray);
  }

  /*
  Native array (flat collection):
    array_length(tags, 1) = 1 AND tags && ?   -- bound as a typed array param
    NULL-safe without COALESCE: both predicates evaluate to NULL for NULL arrays.

  JSONB array (nested collection or JSONB column in a flat collection):
    jsonb_array_length(<guarded array>) = 1 AND <guarded array> <@ ?::jsonb
                                            -- bound as the JSON text '["Blue","Green"]'
    With exactly one element, "the element is one of the filter values" is equivalent to the
    array being contained in the filter values (<@) - one bound param, no OR chain.
   */
  private String getFilterStringForOneOperator(final ArrayFilterExpression expression) {
    final List<?> values = getArrayOperatorFilterValues(expression);
    final ArrayFieldContext fieldContext = getArrayFieldContext(expression);

    if (fieldContext.isNativeArray()) {
      final PostgresDataType dataType =
          resolvePostgresDataType(expression.getArraySource(), values);
      postgresQueryParser.getParamsBuilder().addArrayParam(values.toArray(), dataType.getSqlType());
      return String.format(
          "array_length(%s, 1) = 1 AND %s && ?",
          fieldContext.parsedLhs(), fieldContext.parsedLhs());
    }

    final String guardedArray =
        String.format(
            "(CASE WHEN jsonb_typeof(%s) = 'array' THEN %s ELSE '[]'::jsonb END)",
            fieldContext.parsedLhs(), fieldContext.parsedLhs());
    postgresQueryParser.getParamsBuilder().addObjectParam(toJsonArrayString(values));
    return String.format(
        "jsonb_array_length(%s) = 1 AND %s <@ ?::jsonb", guardedArray, guardedArray);
  }

  private ArrayFieldContext getArrayFieldContext(final ArrayFilterExpression expression) {
    final boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;
    final boolean isJsonbArray = expression.getArraySource() instanceof JsonIdentifierExpression;
    final boolean isNativeArray = isFlatCollection && !isJsonbArray;

    final String identifierName =
        expression
            .getArraySource()
            .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));

    final String parsedLhs;
    if (isNativeArray) {
      parsedLhs = postgresQueryParser.transformField(identifierName).getPgColumn();
    } else {
      final PostgresIdentifierExpressionVisitor identifierVisitor =
          new PostgresIdentifierExpressionVisitor(postgresQueryParser);
      final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
          wrappingVisitorProvider == null
              ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
              : wrappingVisitorProvider.getForNonRelational(identifierVisitor);
      parsedLhs = expression.getArraySource().accept(arrayPathVisitor);
    }

    return new ArrayFieldContext(parsedLhs, isNativeArray);
  }

  private List<?> getArrayOperatorFilterValues(final ArrayFilterExpression expression) {
    final FilterTypeExpression filter = expression.getFilter();
    if (!(filter instanceof RelationalExpression)) {
      throw new UnsupportedOperationException(
          "Array operator "
              + expression.getOperator()
              + " only supports a relational filter with a constant list of values, got: "
              + filter);
    }

    final SelectTypeExpression rhs = ((RelationalExpression) filter).getRhs();
    if (!(rhs instanceof ConstantExpression)) {
      throw new UnsupportedOperationException(
          "Array operator "
              + expression.getOperator()
              + " requires a constant list of values, got: "
              + rhs);
    }

    final Object value = ((ConstantExpression) rhs).getValue();
    return value instanceof List ? (List<?>) value : List.of(value);
  }

  /**
   * Resolves the PostgreSQL element type for a native array field, preferring the compile-time type
   * carried by the field expression ({@link ArrayIdentifierExpression#getElementDataType()} /
   * {@link IdentifierExpression#getDataType()}) and falling back to inference from the filter
   * values only when the field carries no type info.
   */
  private PostgresDataType resolvePostgresDataType(
      final SelectTypeExpression arraySource, final List<?> values) {
    final PostgresDataType fieldType = getCompileTimeFieldType(arraySource);
    return fieldType != PostgresDataType.UNKNOWN ? fieldType : resolvePostgresDataType(values);
  }

  private PostgresDataType getCompileTimeFieldType(final SelectTypeExpression arraySource) {
    final DataType dataType;
    if (arraySource instanceof ArrayIdentifierExpression) {
      dataType = ((ArrayIdentifierExpression) arraySource).getElementDataType();
    } else if (arraySource instanceof IdentifierExpression) {
      dataType = ((IdentifierExpression) arraySource).getDataType();
    } else {
      return PostgresDataType.UNKNOWN;
    }
    // JSON has no scalar Postgres mapping for native array casts; treat it like UNSPECIFIED
    if (dataType == DataType.UNSPECIFIED || dataType == DataType.JSON) {
      return PostgresDataType.UNKNOWN;
    }
    return PostgresDataType.fromDataType(dataType);
  }

  private PostgresDataType resolvePostgresDataType(final List<?> values) {
    return values.stream()
        .map(PostgresDataType::fromJavaValue)
        .filter(dataType -> dataType != PostgresDataType.UNKNOWN)
        .findFirst()
        .orElse(PostgresDataType.TEXT);
  }

  private String toJsonArrayString(final List<?> values) {
    try {
      return new ObjectMapper().writeValueAsString(values);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to serialize array filter values: " + values, e);
    }
  }

  private static class ArrayFieldContext {
    private final String parsedLhs;
    private final boolean isNativeArray;

    ArrayFieldContext(final String parsedLhs, final boolean isNativeArray) {
      this.parsedLhs = parsedLhs;
      this.isNativeArray = isNativeArray;
    }

    String parsedLhs() {
      return parsedLhs;
    }

    boolean isNativeArray() {
      return isNativeArray;
    }
  }
}
