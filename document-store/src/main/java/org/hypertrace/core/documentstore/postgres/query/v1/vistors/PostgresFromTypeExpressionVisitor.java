package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.hypertrace.core.documentstore.DocumentType;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.JsonIdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.query.v1.parser.filter.nonjson.field.PostgresDataType;
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
      // For flat collections with unnest, scalar/array-column filters cannot be applied verbatim in
      // table0 (unnested scalar columns don't exist yet, and array columns reject scalar operators
      // like LIKE). However, an equality/IN filter on a native array column has an equivalent,
      // GIN-indexable array-overlap (&&) form that CAN be evaluated on the base array column before
      // the row-multiplying unnest. Emit that as a pruning pre-filter when available.
      Optional<String> arrayOverlapPrefilterMaybe =
          buildFlatArrayOverlapPrefilter(postgresQueryParser);
      return arrayOverlapPrefilterMaybe
          .map(
              prefilter ->
                  String.format(
                      TABLE0_QUERY_FMT_WHERE, postgresQueryParser.getTableIdentifier(), prefilter))
          .orElseGet(
              () -> String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier()));
    } else {
      // For nested collections, apply filters in table0 as usual
      Optional<String> whereFilter =
          PostgresFilterTypeExpressionVisitor.getFilterClause(postgresQueryParser);

      return whereFilter
          .map(
              s ->
                  String.format(
                      TABLE0_QUERY_FMT_WHERE, postgresQueryParser.getTableIdentifier(), s))
          .orElseGet(
              () -> String.format(TABLE0_QUERY_FMT, postgresQueryParser.getTableIdentifier()));
    }
  }

  /**
   * Builds an array-overlap ({@code &&}) pre-filter for flat collections so rows can be pruned at
   * the base array column before the row-multiplying unnest. Only equality/IN predicates on a
   * native (non-JSONB) array column are translated, since only those have a correct, GIN-indexable
   * array-membership equivalent. The generated predicate is necessary-but-not-sufficient: the
   * post-unnest element filter in the outer WHERE still selects the matching element and is left
   * unchanged.
   *
   * <p><b>Example.</b> For a query that unnests the native array column {@code
   * attributes.domainIds} with an equality filter on the unnested element:
   *
   * <pre>{@code
   * Without this pre-filter (filter only applied after unnest, so the GIN index on the array
   * column cannot be used and every candidate row is expanded first):
   *
   *   With
   *     table0 as (SELECT * FROM "entities_api"),
   *     table1 as (SELECT * FROM table0 t0
   *       LEFT JOIN LATERAL unnest("attributes.domainIds")
   *         p1("attributes_dot_domainIds_unnested") ON TRUE)
   *   SELECT ... FROM table1
   *   WHERE "attributes_dot_domainIds_unnested" = ANY(?)
   *
   * With this pre-filter (rows pruned at the base array column before unnest, via GIN):
   *
   *   With
   *     table0 as (SELECT * FROM "entities_api"
   *       WHERE "attributes.domainIds" && ?),          -- added by this method
   *     table1 as (SELECT * FROM table0 t0
   *       LEFT JOIN LATERAL unnest("attributes.domainIds")
   *         p1("attributes_dot_domainIds_unnested") ON TRUE)
   *   SELECT ... FROM table1
   *   WHERE "attributes_dot_domainIds_unnested" = ANY(?)   -- unchanged, still selects the element
   * }</pre>
   *
   * <p>Here the {@code &&} parameter is the same value list as the post-unnest {@code = ANY(?)}
   * filter, bound as a single array parameter. The outer filter is intentionally left in place to
   * pick the matching unnested element from the surviving rows.
   */
  private static Optional<String> buildFlatArrayOverlapPrefilter(
      PostgresQueryParser postgresQueryParser) {
    List<String> overlapClauses = new ArrayList<>();
    Set<String> seenClauses = new LinkedHashSet<>();

    for (FromTypeExpression fromTypeExpression :
        postgresQueryParser.getQuery().getFromTypeExpressions()) {
      // && applies to native array columns only (JSONB arrays are excluded). Eligible element
      // predicates are moved onto the unnest's own filter by PostgresUnnestQueryTransformer, so the
      // unnest filter is the only place we need to look.
      if (!(fromTypeExpression instanceof UnnestExpression)) {
        continue;
      }
      UnnestExpression unnest = (UnnestExpression) fromTypeExpression;
      if (unnest.getIdentifierExpression() instanceof JsonIdentifierExpression
          || unnest.getFilterTypeExpression() == null) {
        continue;
      }

      String fieldName = unnest.getIdentifierExpression().getName();
      for (RelationalExpression predicate :
          findConjunctiveEqualityPredicates(unnest.getFilterTypeExpression(), fieldName)) {
        addOverlapClause(predicate, fieldName, postgresQueryParser, overlapClauses, seenClauses);
      }
    }

    return overlapClauses.isEmpty()
        ? Optional.empty()
        : Optional.of(String.join(" AND ", overlapClauses));
  }

  /**
   * Returns the AND-connected equality/IN predicates on {@code fieldName} that have a constant RHS.
   *
   * <p>Only AND nodes are traversed: a predicate under OR/NOT is not implied by the overall filter,
   * so pre-filtering on it would be incorrect. Returning fewer predicates is always safe, since the
   * pre-filter is purely additive pruning.
   */
  private static List<RelationalExpression> findConjunctiveEqualityPredicates(
      FilterTypeExpression filter, String fieldName) {
    if (filter instanceof LogicalExpression) {
      LogicalExpression logicalExpression = (LogicalExpression) filter;
      if (logicalExpression.getOperator() != LogicalOperator.AND) {
        return List.of();
      }
      List<RelationalExpression> predicates = new ArrayList<>();
      for (FilterTypeExpression operand : logicalExpression.getOperands()) {
        predicates.addAll(findConjunctiveEqualityPredicates(operand, fieldName));
      }
      return predicates;
    }

    if (filter instanceof RelationalExpression) {
      RelationalExpression predicate = (RelationalExpression) filter;
      RelationalOperator operator = predicate.getOperator();
      boolean isEqualityOrIn =
          operator == RelationalOperator.EQ || operator == RelationalOperator.IN;
      boolean isOnField =
          predicate.getLhs() instanceof IdentifierExpression
              && ((IdentifierExpression) predicate.getLhs()).getName().equals(fieldName);
      if (isEqualityOrIn && isOnField && predicate.getRhs() instanceof ConstantExpression) {
        return List.of(predicate);
      }
    }

    return List.of();
  }

  /**
   * Renders a single equality/IN predicate into an array-overlap clause ({@code "col" && ?}), binds
   * its value(s) as one array parameter, and appends the clause. Equivalent predicates (a query can
   * carry the same condition more than once) are emitted at most once, since each clause consumes a
   * positional parameter.
   */
  private static void addOverlapClause(
      RelationalExpression predicate,
      String fieldName,
      PostgresQueryParser postgresQueryParser,
      List<String> overlapClauses,
      Set<String> seenClauses) {
    Object constantValue = ((ConstantExpression) predicate.getRhs()).getValue();
    List<Object> values =
        constantValue instanceof List
            ? new ArrayList<>((List<?>) constantValue)
            : Collections.singletonList(constantValue);
    if (values.isEmpty() || values.get(0) == null) {
      return;
    }

    PostgresDataType elementType = PostgresDataType.fromJavaValue(values.get(0));
    if (elementType == PostgresDataType.UNKNOWN || !seenClauses.add(fieldName + "::" + values)) {
      return;
    }

    String arrayColumn = postgresQueryParser.transformField(fieldName).getPgColumn();
    postgresQueryParser
        .getParamsBuilder()
        .addArrayParam(values.toArray(), elementType.getSqlType());
    overlapClauses.add(arrayColumn + " && ?");
  }

  /*
  Returns the column name with double quotes if the collection is flat to prevent folding to lower-case by PG
   */
  private String getQuotedColName(String pgColumnName) {
    return PostgresUtils.wrapFieldNamesWithDoubleQuotes(pgColumnName);
  }
}
