package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
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

  @AllArgsConstructor
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
                selectionSpec ->
                    new PgSelection(
                        selectionSpec.getExpression().accept(selectTypeExpressionVisitor),
                        getAlias(selectionSpec, identifierExpressionVisitor)))
            .filter(pgSelection -> StringUtils.isNotEmpty(pgSelection.fieldName))
            .map(
                pgSelection -> {
                  if (StringUtils.isNotEmpty(pgSelection.alias)) {
                    pgSelections.put(pgSelection.alias, pgSelection.fieldName);
                    return pgSelection.fieldName + " AS " + pgSelection.alias;
                  }
                  return pgSelection.fieldName;
                })
            .collect(Collectors.joining(", "));

    return !childList.isEmpty() ? childList : null;
  }

  private static String getAlias(
      SelectionSpec selectionSpec,
      PostgresIdentifierExpressionVisitor identifierExpressionVisitor) {
    return !StringUtils.isEmpty(selectionSpec.getAlias())
        ? selectionSpec.getAlias()
        : PostgresUtils.encodeAliasForNestedField(
            selectionSpec.getExpression().accept(identifierExpressionVisitor));
  }
}
