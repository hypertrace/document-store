package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public abstract class PostgresSelectTypeExpressionVisitor implements SelectTypeExpressionVisitor {

  protected final PostgresSelectTypeExpressionVisitor baseVisitor;

  protected PostgresSelectTypeExpressionVisitor() {
    this(PostgresDefaultSelectTypeExpressionVisitor.INSTANCE);
  }

  protected PostgresSelectTypeExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor) {
    this.baseVisitor = baseVisitor;
  }

  @Override
  public <T> T visit(final AggregateExpression expression) {
    return baseVisitor.visit(expression);
  }

  @Override
  public <T> T visit(final ConstantExpression expression) {
    return baseVisitor.visit(expression);
  }

  @Override
  public <T> T visit(final FunctionExpression expression) {
    return baseVisitor.visit(expression);
  }

  @Override
  public <T> T visit(final IdentifierExpression expression) {
    return baseVisitor.visit(expression);
  }

  @NoArgsConstructor
  static class PgSelection {
    String fieldName;
    String alias;
  }

  public static String getSelections(PostgresQueryParser postgresQueryParser) {
    List<SelectionSpec> selectionSpecs = postgresQueryParser.getQuery().getSelections();

    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresAggregateExpressionVisitor(
            new PostgresFieldIdentifierExpressionVisitor(new PostgresFunctionExpressionVisitor()));

    // used for if alias is missing
    PostgresIdentifierExpressionVisitor identifierExpressionVisitor =
        new PostgresIdentifierExpressionVisitor();

    Map<String, String> pgSelections = postgresQueryParser.getPgSelections();

    String childList =
        selectionSpecs.stream()
            .map(
                selectionSpec -> {
                  PgSelection pgSelection = new PgSelection();
                  pgSelection.fieldName =
                      selectionSpec.getExpression().accept(selectTypeExpressionVisitor);
                  if (!StringUtils.isEmpty(selectionSpec.getAlias())) {
                    pgSelection.alias = selectionSpec.getAlias();
                    pgSelections.put(pgSelection.alias, pgSelection.fieldName);
                  } else {
                    pgSelection.alias =
                        PostgresUtils.encodeAliasForNestedField(
                            selectionSpec.getExpression().accept(identifierExpressionVisitor));
                  }
                  return pgSelection;
                })
            .filter(pgSelection -> StringUtils.isNotEmpty(pgSelection.fieldName))
            .map(
                pgSelection ->
                    StringUtils.isNotEmpty(pgSelection.alias)
                        ? pgSelection.fieldName + " AS " + pgSelection.alias
                        : pgSelection.fieldName)
            .collect(Collectors.joining(", "));

    return !childList.isEmpty() ? childList : null;
  }
}
