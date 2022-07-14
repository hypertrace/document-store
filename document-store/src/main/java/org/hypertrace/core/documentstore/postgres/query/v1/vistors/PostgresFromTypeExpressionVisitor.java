package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresFromTypeExpressionVisitor implements FromTypeExpressionVisitor {
  private static String fmtWithPreserveNullAndEmpty = "With \n%s, \n%s, \n%s \n";
  private static String fmtWithoutPreserveNullAndEmpty = "With \n%s, \n%s \n";

  private PostgresQueryParser postgresQueryParser;
  private PostgresFieldIdentifierExpressionVisitor postgresFieldIdentifierExpressionVisitor;
  @Getter private Boolean preserveNullAndEmptyArrays;

  public PostgresFromTypeExpressionVisitor(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
    this.postgresFieldIdentifierExpressionVisitor =
        new PostgresFieldIdentifierExpressionVisitor(postgresQueryParser);
    this.preserveNullAndEmptyArrays = null;
  }

  @Override
  public String visit(UnnestExpression unnestExpression) {
    if (preserveNullAndEmptyArrays == null) {
      preserveNullAndEmptyArrays = unnestExpression.isPreserveNullAndEmptyArrays();
    }

    if (!preserveNullAndEmptyArrays.equals(unnestExpression.isPreserveNullAndEmptyArrays())) {
      throw new UnsupportedOperationException(
          "Mixed boolean value for preserveNullAndEmptyArrays with "
              + "multiple unnest expressions is not supported");
    }

    String orgFieldName = unnestExpression.getIdentifierExpression().getName();
    String pgColumnName = PostgresUtils.encodeAliasForNestedField(orgFieldName);

    String transformedFieldName =
        unnestExpression.getIdentifierExpression().accept(postgresFieldIdentifierExpressionVisitor);

    postgresQueryParser.getPgColumnNames().put(orgFieldName, pgColumnName);
    int lastIndex = postgresQueryParser.getPgColumnNames().size();

    return String.format(
        "jsonb_array_elements(%s) p%s(%s)", transformedFieldName, lastIndex, pgColumnName);
  }

  public static Optional<String> getFromClause(PostgresQueryParser postgresQueryParser) {

    PostgresFromTypeExpressionVisitor postgresFromTypeExpressionVisitor =
        new PostgresFromTypeExpressionVisitor(postgresQueryParser);
    String childList =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(postgresFromTypeExpressionVisitor))
            .map(Object::toString)
            .collect(Collectors.joining(","));

    if (StringUtils.isEmpty(childList)) {
      return Optional.empty();
    }

    if (!postgresFromTypeExpressionVisitor.getPreserveNullAndEmptyArrays()) {
      postgresQueryParser.setFinalTableName("table2");
      String table1Query = prepareTable1Query(postgresQueryParser);
      String table2Query = prepareTable2Query(childList);
      return Optional.of(String.format(fmtWithoutPreserveNullAndEmpty, table1Query, table2Query));
    }

    postgresQueryParser.setFinalTableName("table3");
    String table1Query = prepareTable1Query(postgresQueryParser);
    String table2Query = prepareTable2Query(childList);
    String table3Query = prepareTable3Query(postgresQueryParser);
    return Optional.of(
        String.format(fmtWithPreserveNullAndEmpty, table1Query, table2Query, table3Query));
  }

  private static String prepareTable1Query(PostgresQueryParser postgresQueryParser) {
    String queryFmt = "table1 as (SELECT * from %s)";
    // TODO : handle original where clause here
    return String.format(queryFmt, postgresQueryParser.getCollection());
  }

  private static String prepareTable2Query(String unwindJsonArrayElementsStr) {
    String queryFmt = "table2 as (SELECT * FROM table1, %s)";
    return String.format(queryFmt, unwindJsonArrayElementsStr);
  }

  private static String prepareTable3Query(PostgresQueryParser postgresQueryParser) {
    String queryFmt = "table3 as (SELECT %s, %s from %s m " + "LEFT JOIN table2 d on(m.id = d.id)";

    List<String> orderedSet =
        Stream.concat(
                PostgresUtils.OUTER_COLUMNS.stream(),
                List.of(PostgresUtils.DOCUMENT_COLUMN).stream())
            .collect(Collectors.toList());

    String baseColumnsStr =
        orderedSet.stream().map(value -> "m." + value).collect(Collectors.joining(","));

    String unwindArrayColumnsStr =
        postgresQueryParser.getPgColumnNames().values().stream()
            .map(value -> "d." + value)
            .collect(Collectors.joining(","));

    return String.format(
        queryFmt, baseColumnsStr, unwindArrayColumnsStr, postgresQueryParser.getCollection());
  }
}
