package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SortTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.query.SortingSpec;

public class PostgresSortTypeExpressionVisitor implements SortTypeExpressionVisitor {

  private PostgresQueryParser postgresQueryParser;
  private PostgresIdentifierExpressionVisitor identifierExpressionVisitor;
  PostgresFieldIdentifierExpressionVisitor fieldIdentifierExpressionVisitor;

  public PostgresSortTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
    identifierExpressionVisitor = new PostgresIdentifierExpressionVisitor();
    fieldIdentifierExpressionVisitor = new PostgresFieldIdentifierExpressionVisitor();
  }

  @Override
  public String visit(AggregateExpression expression) {
    throw new UnsupportedOperationException(
        "Not yes supported sorting directly by aggregation expression."
            + "Use alias in selection for aggregation expression for sorting");
  }

  @Override
  public String visit(FunctionExpression expression) {
    throw new UnsupportedOperationException(
        "Not yes supported sorting directly by functional expression."
            + "Use alias in selection for functional expression for sorting");
  }

  @Override
  public String visit(IdentifierExpression expression) {
    // NOTE: alias is supported in ORDER BY clause in SQL
    String fieldName = identifierExpressionVisitor.visit(expression);

    return postgresQueryParser.getPgSelections().containsKey(fieldName)
        ? fieldName
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
                  return String.format("%s %s", sortingExp, sortingSpec.getOrder().toString());
                })
            .collect(Collectors.joining(","));

    return StringUtils.isNotEmpty(childList) ? Optional.of(childList) : Optional.empty();
  }
}
