package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.NOT;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.encodeAliasForNestedField;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParsedNonCompositeFilter;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.Key;
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

/**
 * Base class for Postgres filter visitors containing common logic that doesn't differ between flat
 * and nested collections.
 *
 * <p>Subclasses implement collection-specific logic for array handling.
 */
public abstract class PostgresFilterTypeExpressionVisitorBase
    implements FilterTypeExpressionVisitor {

  protected final PostgresQueryParser postgresQueryParser;
  @Nullable protected final PostgresWrappingFilterVisitorProvider wrappingVisitorProvider;

  protected PostgresFilterTypeExpressionVisitorBase(
      PostgresQueryParser postgresQueryParser,
      @Nullable PostgresWrappingFilterVisitorProvider wrappingVisitorProvider) {
    this.postgresQueryParser = postgresQueryParser;
    this.wrappingVisitorProvider = wrappingVisitorProvider;
  }

  // ========== Common Visitor Methods (Same for both Flat and Nested) ==========

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
    switch (expression.getOperator()) {
      case ANY:
        return buildArrayRelationalFilter(expression);

      default:
        throw new UnsupportedOperationException(
            "Unsupported array operator: " + expression.getOperator());
    }
  }

  @SuppressWarnings({"unchecked", "SwitchStatementWithTooFewBranches"})
  @Override
  public String visit(final DocumentArrayFilterExpression expression) {
    switch (expression.getOperator()) {
      case ANY:
        return buildDocumentArrayFilter(expression);

      default:
        throw new UnsupportedOperationException(
            "Unsupported array operator: " + expression.getOperator());
    }
  }

  // ========== Helper Methods ==========

  protected Collector getCollectorForLogicalOperator(LogicalOperator operator) {
    if (operator.equals(LogicalOperator.OR)) {
      return Collectors.joining(" OR ");
    } else if (operator.equals(LogicalOperator.AND)) {
      return Collectors.joining(" AND ");
    }
    throw new UnsupportedOperationException(
        String.format("Query operation:%s not supported", operator));
  }

  protected String getEncodedAlias(String identifierName) {
    return encodeAliasForNestedField(identifierName);
  }

  protected String getIdentifierName(ArrayRelationalFilterExpression expression) {
    return expression
        .getArraySource()
        .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));
  }

  protected String getIdentifierName(DocumentArrayFilterExpression expression) {
    return expression
        .getArraySource()
        .accept(new PostgresIdentifierExpressionVisitor(postgresQueryParser));
  }

  // ========== Abstract Methods (Collection-Specific) ==========

  /**
   * Builds the filter string for ArrayRelationalFilterExpression. Implementations differ based on
   * whether the collection uses native arrays (flat) or JSONB arrays (nested).
   */
  protected abstract String buildArrayRelationalFilter(ArrayRelationalFilterExpression expression);

  /**
   * Builds the filter string for DocumentArrayFilterExpression. Implementations differ based on
   * whether the collection uses native arrays (flat) or JSONB arrays (nested).
   */
  protected abstract String buildDocumentArrayFilter(DocumentArrayFilterExpression expression);

  /**
   * Prepares the filter clause from a FilterTypeExpression.
   *
   * @param filterTypeExpression the filter expression
   * @param postgresQueryParser the query parser
   * @return Optional filter clause string
   */
  public static Optional<String> prepareFilterClause(
      Optional<FilterTypeExpression> filterTypeExpression,
      PostgresQueryParser postgresQueryParser) {
    return filterTypeExpression.map(
        expression ->
            expression.accept(
                PostgresVisitorFactory.createFilterVisitor(postgresQueryParser, null)));
  }
}
