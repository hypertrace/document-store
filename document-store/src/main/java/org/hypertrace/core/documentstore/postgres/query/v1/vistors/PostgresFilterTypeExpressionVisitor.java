package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static java.util.stream.Collectors.toUnmodifiableList;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.EQ;
import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.IN;
import static org.hypertrace.core.documentstore.postgres.PostgresCollection.ID;
import static org.hypertrace.core.documentstore.postgres.utils.PostgresUtils.prepareParsedNonCompositeFilter;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;
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

public class PostgresFilterTypeExpressionVisitor implements FilterTypeExpressionVisitor {
  protected PostgresQueryParser postgresQueryParser;

  public PostgresFilterTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
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
    final PostgresSelectExpressionParserBuilder parserBuilder =
        new PostgresSelectExpressionParserBuilderImpl();
    final PostgresSelectTypeExpressionVisitor lhsVisitor =
        parserBuilder.buildFor(expression, postgresQueryParser);

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

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final ArrayRelationalFilterExpression expression) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String visit(final DocumentArrayFilterExpression expression) {
    throw new UnsupportedOperationException();
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
}
