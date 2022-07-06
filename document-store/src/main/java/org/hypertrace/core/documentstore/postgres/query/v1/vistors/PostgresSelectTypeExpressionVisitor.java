package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
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

  public static String getSelections(final List<SelectionSpec> selectionSpecs) {
    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresFieldIdentifierExpressionVisitor(new PostgresFunctionExpressionVisitor());

    // used for if alias is missing
    PostgresIdentifierExpressionVisitor identifierExpressionVisitor =
        new PostgresIdentifierExpressionVisitor();

    String childList =
        selectionSpecs.stream()
            .map(
                selectionSpec ->
                    Pair.of(
                        selectionSpec.getExpression().accept(selectTypeExpressionVisitor),
                        getAlias(selectionSpec, identifierExpressionVisitor)))
            .filter(p -> !StringUtils.isEmpty((String) p.getLeft()))
            .map(
                p ->
                    StringUtils.isNotEmpty(p.getRight())
                        ? "" + p.getLeft() + " AS " + p.getRight()
                        : "" + p.getLeft() + "")
            .collect(Collectors.joining(", "));

    return !childList.isEmpty() ? childList : null;
  }

  private static String getAlias(
      SelectionSpec selectionSpec,
      PostgresIdentifierExpressionVisitor identifierExpressionVisitor) {
    return !StringUtils.isEmpty(selectionSpec.getAlias())
        ? selectionSpec.getAlias()
        : selectionSpec.getExpression().accept(identifierExpressionVisitor);
  }
}
