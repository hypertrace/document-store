package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

public class PostgresFromTypeExpressionVisitor implements FromTypeExpressionVisitor {

  private static final String QUERY_FMT = "With \n%s\n%s\n";

  private static final String TABLE0_QUERY_FMT = "table0 as (SELECT * from %s),";
  private static final String TABLE0_QUERY_FMT_WHERE = "table0 as (SELECT * from %s WHERE %s),";

  // <next-table-name> as (SELECT * from <pre-table-name> <table-name-alias>, <unwind-expr>
  // <unwind-exp-column-alias>)
  private static final String WITHOUT_PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT =
      "%s as (SELECT * from %s %s, %s %s)";
  private static final String PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT =
      "%s as (SELECT * from %s %s LEFT JOIN LATERAL %s %s on TRUE)";
  private static final String JSONB_UNWIND_EXP_FMT = "jsonb_array_elements(%s)";
  private static final String NATIVE_UNWIND_EXP_FMT = "unnest(%s)";
  private static final String UNWIND_EXP_ALIAS_FMT = "p%s(%s)";

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
    String orgFieldName = unnestExpression.getIdentifierExpression().getName();
    String pgColumnName = PostgresUtils.encodeAliasForNestedField(orgFieldName);

    // Check if this is a flat collection (native PostgreSQL columns) or nested (JSONB)
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    String transformedFieldName;
    String unnestFunction;

    if (isFlatCollection) {
      // For flat collections, assume all unnested fields are native PostgreSQL arrays
      // Use the transformer to get the proper column name (handles quotes and naming)
      transformedFieldName = postgresQueryParser.transformField(orgFieldName).getPgColumn();
      // Use native unnest() for PostgreSQL array columns
      unnestFunction = NATIVE_UNWIND_EXP_FMT;
      // Append "_unnested" suffix to avoid column name conflicts with the original array column
      // e.g., unnest("tags") p1(tags_unnested) instead of p1(tags)
      pgColumnName = pgColumnName + "_unnested";
    } else {
      // For nested collections, use JSONB path accessor
      transformedFieldName =
          unnestExpression
              .getIdentifierExpression()
              .accept(postgresFieldIdentifierExpressionVisitor);
      // Use jsonb_array_elements() for JSONB arrays
      unnestFunction = JSONB_UNWIND_EXP_FMT;
    }

    postgresQueryParser.getPgColumnNames().put(orgFieldName, pgColumnName);
    int nextIndex = postgresQueryParser.getPgColumnNames().size();
    int preIndex = nextIndex - 1;

    String preTable = "table" + preIndex;
    String newTable = "table" + nextIndex;
    String tableAlias = "t" + preIndex;
    String unwindExpr = String.format(unnestFunction, transformedFieldName);
    String unwindExprAlias = String.format(UNWIND_EXP_ALIAS_FMT, nextIndex, pgColumnName);

    String fmt =
        unnestExpression.isPreserveNullAndEmptyArrays()
            ? PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT
            : WITHOUT_PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT;

    return String.format(fmt, newTable, preTable, tableAlias, unwindExpr, unwindExprAlias);
  }

  @Override
  public String visit(SubQueryJoinExpression subQueryJoinExpression) {
    throw new UnsupportedOperationException("This operation is not supported");
  }

  public static Optional<String> getFromClause(PostgresQueryParser postgresQueryParser) {

    PostgresFromTypeExpressionVisitor postgresFromTypeExpressionVisitor =
        new PostgresFromTypeExpressionVisitor(postgresQueryParser);
    String childList =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(postgresFromTypeExpressionVisitor))
            .map(Object::toString)
            .collect(Collectors.joining(",\n"));

    if (StringUtils.isEmpty(childList)) {
      return Optional.empty();
    }

    String table0Query = prepareTable0Query(postgresQueryParser);

    postgresQueryParser.setFinalTableName("table" + postgresQueryParser.getPgColumnNames().size());
    return Optional.of(String.format(QUERY_FMT, table0Query, childList));
  }

  private static String prepareTable0Query(PostgresQueryParser postgresQueryParser) {
    Optional<String> whereFilter =
        PostgresFilterTypeExpressionVisitor.getFilterClause(postgresQueryParser);

    return whereFilter.isPresent()
        ? String.format(
            TABLE0_QUERY_FMT_WHERE, postgresQueryParser.getTableIdentifier(), whereFilter.get())
        : String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier());
  }
}
