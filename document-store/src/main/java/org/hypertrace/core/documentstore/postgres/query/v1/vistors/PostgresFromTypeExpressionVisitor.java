package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
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
  private static final String JSONB_UNWIND_EXP_FMT =
      "jsonb_array_elements(CASE WHEN jsonb_typeof(%s) = 'array' THEN %s ELSE '[]'::jsonb END)";
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

    boolean isJsonbArray =
        unnestExpression.getIdentifierExpression() instanceof JsonIdentifierExpression;

    String transformedFieldName;
    String unnestFunction;

    if (isFlatCollection && !isJsonbArray) {
      // For flat collections with native arrays (e.g., tags), use unnest()
      // Use the transformer to get the proper column name (handles quotes and naming)
      transformedFieldName = postgresQueryParser.transformField(orgFieldName).getPgColumn();
      // Use native unnest() for PostgreSQL array columns
      unnestFunction = NATIVE_UNWIND_EXP_FMT;
      // Append "_unnested" suffix to avoid column name conflicts with the original array column
      // e.g., unnest("tags") p1(tags_unnested) instead of p1(tags)
      pgColumnName = pgColumnName + "_unnested";
    } else {
      // For nested collections OR JSONB arrays in flat collections, use jsonb_array_elements()
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
    String unwindExpr =
        unnestFunction.equals(JSONB_UNWIND_EXP_FMT)
            ? String.format(unnestFunction, transformedFieldName, transformedFieldName)
            : String.format(unnestFunction, transformedFieldName);

    // we'll quote the col name to prevent folding to lower case for top-level array fields
    String unwindExprAlias =
        String.format(UNWIND_EXP_ALIAS_FMT, nextIndex, getQuotedColName(pgColumnName));

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

    // Check if there are any unnest operations
    if (postgresQueryParser.getQuery().getFromTypeExpressions().isEmpty()) {
      return Optional.empty();
    }

    // IMPORTANT: Build table0 query BEFORE processing unnest expressions
    // This ensures filters use original field names, not unnested aliases
    String table0Query = prepareTable0Query(postgresQueryParser);

    // Now process unnest expressions, which will populate pgColumnNames map
    PostgresFromTypeExpressionVisitor postgresFromTypeExpressionVisitor =
        new PostgresFromTypeExpressionVisitor(postgresQueryParser);
    String childList =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(postgresFromTypeExpressionVisitor))
            .map(Object::toString)
            .collect(Collectors.joining(",\n"));

    postgresQueryParser.setFinalTableName("table" + postgresQueryParser.getPgColumnNames().size());
    return Optional.of(String.format(QUERY_FMT, table0Query, childList));
  }

  private static String prepareTable0Query(PostgresQueryParser postgresQueryParser) {
    // For flat collections with unnest operations, we cannot apply filters in table0 because:
    // 1. Filters on unnested fields reference scalar values that don't exist yet in table0
    // 2. Filters on array fields might use operators that don't work on arrays (like LIKE)
    // For nested collections, filters work fine because they reference JSONB paths in 'document'
    boolean isFlatCollection =
        postgresQueryParser.getPgColTransformer().getDocumentType() == DocumentType.FLAT;

    if (isFlatCollection) {
      // For flat collections with unnest, skip filters in table0
      return String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier());
    } else {
      // For nested collections, apply filters in table0 as usual (preserves existing behavior)
      Optional<String> whereFilter =
          PostgresFilterTypeExpressionVisitor.getFilterClause(postgresQueryParser);

      return whereFilter.isPresent()
          ? String.format(
              TABLE0_QUERY_FMT_WHERE, postgresQueryParser.getTableIdentifier(), whereFilter.get())
          : String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier());
    }
  }

  /*
  Returns the column name with double quotes if the collection is flat to prevent folding to lower-case by PG
   */
  private String getQuotedColName(String pgColumnName) {
    return PostgresUtils.wrapFieldNamesWithDoubleQuotes(pgColumnName);
  }
}
