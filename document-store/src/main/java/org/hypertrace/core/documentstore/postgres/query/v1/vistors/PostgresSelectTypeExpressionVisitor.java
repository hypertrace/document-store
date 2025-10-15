package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.AliasedIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression.DocumentConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.FunctionExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;
import org.hypertrace.core.documentstore.query.SelectionSpec;

public abstract class PostgresSelectTypeExpressionVisitor implements SelectTypeExpressionVisitor {

  protected final PostgresSelectTypeExpressionVisitor baseVisitor;
  protected final PostgresQueryParser postgresQueryParser;

  protected PostgresSelectTypeExpressionVisitor() {
    this(PostgresDefaultSelectTypeExpressionVisitor.INSTANCE, null);
  }

  protected PostgresSelectTypeExpressionVisitor(
      final PostgresSelectTypeExpressionVisitor baseVisitor) {
    this(baseVisitor, null);
  }

  public PostgresSelectTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this(PostgresDefaultSelectTypeExpressionVisitor.INSTANCE, postgresQueryParser);
  }

  public PostgresSelectTypeExpressionVisitor(
      PostgresSelectTypeExpressionVisitor baseVisitor, PostgresQueryParser postgresQueryParser) {
    this.baseVisitor = baseVisitor;
    this.postgresQueryParser = postgresQueryParser;
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
  public <T> T visit(final DocumentConstantExpression expression) {
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

  @Override
  public <T> T visit(final JsonIdentifierExpression expression) {
    return baseVisitor.visit(expression);
  }

  @Override
  public <T> T visit(final AliasedIdentifierExpression expression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public abstract PostgresQueryParser getPostgresQueryParser();

  @AllArgsConstructor
  static class PgSelection {
    String fieldName;
    String alias;
  }

  public static String getSelections(PostgresQueryParser postgresQueryParser) {
    List<SelectionSpec> selectionSpecs = postgresQueryParser.getQuery().getSelections();

    PostgresSelectTypeExpressionVisitor selectTypeExpressionVisitor =
        new PostgresAggregateExpressionVisitor(
            new PostgresFieldIdentifierExpressionVisitor(
                new PostgresFunctionExpressionVisitor(postgresQueryParser)));

    // used for if alias is missing
    PostgresIdentifierExpressionVisitor identifierExpressionVisitor =
        new PostgresIdentifierExpressionVisitor();

    Map<String, String> pgSelections = postgresQueryParser.getPgSelections();

    String childList =
        selectionSpecs.stream()
            .map(
                selectionSpec -> {
                  PgSelection pgSelection =
                      new PgSelection(
                          selectionSpec.getExpression().accept(selectTypeExpressionVisitor),
                          getAlias(selectionSpec, identifierExpressionVisitor));
                  memorizedSelectionForUserDefinedAlias(
                      selectionSpec, pgSelection.fieldName, pgSelections);
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

  private static String getAlias(
      SelectionSpec selectionSpec,
      PostgresIdentifierExpressionVisitor identifierExpressionVisitor) {
    return PostgresUtils.wrapAliasWithDoubleQuotes(
        !StringUtils.isEmpty(selectionSpec.getAlias())
            ? selectionSpec.getAlias()
            : PostgresUtils.encodeAliasForNestedField(
                selectionSpec.getExpression().accept(identifierExpressionVisitor)));
  }

  private static void memorizedSelectionForUserDefinedAlias(
      SelectionSpec selectionSpec, String parsedFieldName, Map<String, String> pgSelections) {
    if (!StringUtils.isEmpty(selectionSpec.getAlias())) {
      pgSelections.put(selectionSpec.getAlias(), parsedFieldName);
    }
  }
}
