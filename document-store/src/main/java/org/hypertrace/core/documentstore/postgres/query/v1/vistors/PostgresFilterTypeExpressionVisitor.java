package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.getLastPath;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParsedNonCompositeFilter;

import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.expression.impl.ArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.DocumentArrayFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.KeyExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.parser.FilterTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.builder.PostgresSelectExpressionParserBuilder;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.builder.PostgresSelectExpressionParserBuilderImpl;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParser.PostgresRelationalFilterContext;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.PostgresRelationalFilterParserFactoryImpl;

public class PostgresFilterTypeExpressionVisitor implements FilterTypeExpressionVisitor {
  private static final PostgresSelectExpressionParserBuilder parserBuilder =
      new PostgresSelectExpressionParserBuilderImpl();

  protected PostgresQueryParser postgresQueryParser;
  @Nullable private final UnaryOperator<PostgresSelectTypeExpressionVisitor> wrappingVisitor;

  public PostgresFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this(postgresQueryParser, null);
  }

  public PostgresFilterTypeExpressionVisitor(
      PostgresQueryParser postgresQueryParser,
      final UnaryOperator<PostgresSelectTypeExpressionVisitor> wrappingVisitor) {
    this.postgresQueryParser = postgresQueryParser;
    this.wrappingVisitor = wrappingVisitor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final LogicalExpression expression) {
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
    final PostgresSelectTypeExpressionVisitor baseVisitor =
        parserBuilder.buildFor(expression, postgresQueryParser);
    final PostgresSelectTypeExpressionVisitor lhsVisitor =
        Optional.ofNullable(wrappingVisitor)
            .map(visitor -> visitor.apply(baseVisitor))
            .orElse(baseVisitor);

    final PostgresRelationalFilterContext context =
        PostgresRelationalFilterContext.builder()
            .lhsParser(lhsVisitor)
            .postgresQueryParser(postgresQueryParser)
            .build();

    return new PostgresRelationalFilterParserFactoryImpl()
        .parser(expression, context)
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

  @SuppressWarnings({"unchecked"})
  @Override
  public String visit(final ArrayRelationalFilterExpression expression) {
    /*
    EXISTS
     (SELECT 1
      FROM jsonb_array_elements(COALESCE(planets->'elements', '[]'::jsonb)) AS elements
      WHERE TRIM('"' FROM elements::text) = 'Oxygen'
     )
     */
    final Object rhsValue =
        expression.getFilter().getRhs().accept(new PostgresConstantExpressionVisitor());
    return parseArrayFilter(
        expression, new PostgresArrayRelationalFilterParserGetter(postgresQueryParser), rhsValue);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final DocumentArrayFilterExpression expression) {
    /*
    EXISTS
    (SELECT 1
     FROM  jsonb_array_elements(COALESCE(document->'planets', '[]'::jsonb)) AS planets
     WHERE <parsed_containing_filter_with_aliased_field_names>
     )
     */
    return parseArrayFilter(expression, new PostgresDocumentArrayFilterParserGetter(), null);
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

  @SuppressWarnings("SwitchStatementWithTooFewBranches")
  private String parseArrayFilter(
      final ArrayFilterExpression arrayFilterExpression,
      final PostgresArrayFilterParserGetter postgresArrayFilterParserGetter,
      final Object rhsValue) {
    switch (arrayFilterExpression.getOperator()) {
      case ANY:
        // Convert 'elements' to planets->'elements' where planets could be an alias for an upper
        // level array filter
        // Also, for the first time (if this was not under any nesting), use the field identifier
        // visitor to make it document->'elements'
        final PostgresIdentifierExpressionVisitor identifierVisitor =
            new PostgresIdentifierExpressionVisitor(postgresQueryParser);
        final PostgresSelectTypeExpressionVisitor arrayPathVisitor =
            wrappingVisitor == null
                ? new PostgresFieldIdentifierExpressionVisitor(identifierVisitor)
                : wrappingVisitor.apply(identifierVisitor);
        final String parsedLhs = arrayFilterExpression.getArraySource().accept(arrayPathVisitor);

        // Extract the field name
        final String identifierName =
            arrayFilterExpression
                .getArraySource()
                .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));

        // If the field name is 'elements.inner', just pick the last part as the alias ('inner')
        final String alias = getLastPath(identifierName);

        // Any LHS field name (elements) is to be prefixed with current alias (inner)
        final UnaryOperator<PostgresSelectTypeExpressionVisitor> wrapper =
            postgresArrayFilterParserGetter.getParser(identifierName, alias, rhsValue);
        final String parsedFilter =
            arrayFilterExpression
                .getFilter()
                .accept(new PostgresFilterTypeExpressionVisitor(postgresQueryParser, wrapper));

        return String.format(
            "EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(%s, '[]'::jsonb)) AS \"%s\" WHERE %s)",
            parsedLhs, alias, parsedFilter);

      default:
        throw new UnsupportedOperationException(
            "Unsupported array operator: " + arrayFilterExpression.getOperator());
    }
  }
}
