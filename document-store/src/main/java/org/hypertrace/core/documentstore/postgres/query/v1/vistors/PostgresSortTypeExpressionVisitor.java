package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import static org.hypertrace.core.documentstore.expression.operators.SortOrder.ASC;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.query.SortingSpec;

public class PostgresSortTypeExpressionVisitor implements SortTypeExpressionVisitor {

  private PostgresQueryParser postgresQueryParser;
  private PostgresIdentifierExpressionVisitor identifierExpressionVisitor;
  private PostgresFieldIdentifierExpressionVisitor fieldIdentifierExpressionVisitor;

  public PostgresSortTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
    identifierExpressionVisitor = new PostgresIdentifierExpressionVisitor();
    fieldIdentifierExpressionVisitor =
        new PostgresFieldIdentifierExpressionVisitor(postgresQueryParser);
  }

  @Override
  public String visit(AggregateExpression expression) {
    throw new UnsupportedOperationException(
        "Sorting using aggregation expression is not yet supported."
            + "Use alias in selection for aggregation expression for sorting");
  }

  @Override
  public String visit(FunctionExpression expression) {
    throw new UnsupportedOperationException(
        "Sorting using function expression is not yet supported."
            + "Use alias in selection for functional expression for sorting");
  }

  @Override
  public String visit(IdentifierExpression expression) {
    // NOTE: SQL supports alias as part of ORDER BY clause.
    // So, if we have already found any user-defined alias, we will use it.
    // Otherwise, we are using a field accessor pattern.
    String fieldName = identifierExpressionVisitor.visit(expression);

    return postgresQueryParser.getPgSelections().containsKey(fieldName)
        ? PostgresUtils.wrapAliasWithDoubleQuotes(fieldName)
        : fieldIdentifierExpressionVisitor.visit(expression);
  }

  public static Optional<String> getOrderByClause(PostgresQueryParser postgresQueryParser) {
    PostgresSortTypeExpressionVisitor sortTypeExpressionVisitor =
        new PostgresSortTypeExpressionVisitor(postgresQueryParser);

    List<SortingSpec> sortingSpecs = postgresQueryParser.getQuery().getSorts();

    String childList =
        sortingSpecs.stream()
            .map(
                sortingSpec -> {
                  String sortingExp = sortingSpec.getExpression().accept(sortTypeExpressionVisitor);
                  return String.format("%s %s", sortingExp, getSortOrder(sortingSpec));
                })
            .collect(Collectors.joining(","));

    return StringUtils.isNotEmpty(childList) ? Optional.of(childList) : Optional.empty();
  }

  private static String getSortOrder(final SortingSpec sortingSpec) {
    return sortingSpec.getOrder() == ASC ? "ASC NULLS FIRST" : "DESC NULLS LAST";
  }
}
