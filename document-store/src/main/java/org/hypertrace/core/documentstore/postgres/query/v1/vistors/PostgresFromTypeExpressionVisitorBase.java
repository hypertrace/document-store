package org.hypertrace.core.documentstore.postgres.query.v1.vistors;

import java.util.stream.Collectors;
import lombok.Getter;
import org.hypertrace.core.documentstore.expression.impl.SubQueryJoinExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.parser.FromTypeExpressionVisitor;
import org.hypertrace.core.documentstore.postgres.query.v1.PostgresQueryParser;
import org.hypertrace.core.documentstore.postgres.utils.PostgresUtils;

/**
 * Base class for Postgres FROM clause visitors containing common logic.
 *
 * <p>Subclasses implement collection-specific logic for unnest operations.
 */
public abstract class PostgresFromTypeExpressionVisitorBase implements FromTypeExpressionVisitor {

  protected static final String QUERY_FMT = "With \n%s\n%s\n";
  protected static final String TABLE0_QUERY_FMT = "table0 as (SELECT * from %s),";

  // <next-table-name> as (SELECT * from <pre-table-name> <table-name-alias>, <unwind-expr>
  // <unwind-exp-column-alias>)
  protected static final String WITHOUT_PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT =
      "%s as (SELECT * from %s %s, %s %s)";
  protected static final String PRESERVE_NULL_AND_EMPTY_TABLE_QUERY_FMT =
      "%s as (SELECT * from %s %s LEFT JOIN LATERAL %s %s on TRUE)";
  protected static final String UNWIND_EXP_ALIAS_FMT = "p%s(%s)";

  protected final PostgresQueryParser postgresQueryParser;
  @Getter protected Boolean preserveNullAndEmptyArrays;

  protected PostgresFromTypeExpressionVisitorBase(PostgresQueryParser postgresQueryParser) {
    this.postgresQueryParser = postgresQueryParser;
    this.preserveNullAndEmptyArrays = null;
  }

  @Override
  public String visit(UnnestExpression unnestExpression) {
    String orgFieldName = unnestExpression.getIdentifierExpression().getName();
    String pgColumnName = PostgresUtils.encodeAliasForNestedField(orgFieldName);

    // Get collection-specific field transformation and unnest function
    UnnestConfig config = buildUnnestConfig(unnestExpression, orgFieldName, pgColumnName);

    postgresQueryParser.getPgColumnNames().put(orgFieldName, config.pgColumnName);
    int nextIndex = postgresQueryParser.getPgColumnNames().size();
    int preIndex = nextIndex - 1;

    String preTable = "table" + preIndex;
    String newTable = "table" + nextIndex;
    String tableAlias = "t" + preIndex;
    String unwindExpr = config.unnestFunction;
    String unwindExprAlias = String.format(UNWIND_EXP_ALIAS_FMT, nextIndex, config.pgColumnName);

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

  /**
   * Builds the unnest configuration specific to the collection type.
   *
   * @param unnestExpression the unnest expression
   * @param orgFieldName original field name
   * @param pgColumnName initial column name
   * @return UnnestConfig with transformed field name, unnest function, and final column name
   */
  protected abstract UnnestConfig buildUnnestConfig(
      UnnestExpression unnestExpression, String orgFieldName, String pgColumnName);

  /**
   * Builds the table0 query with appropriate filters based on collection type.
   *
   * @param postgresQueryParser the query parser
   * @return table0 query string
   */
  protected abstract String buildTable0Query(PostgresQueryParser postgresQueryParser);

  /** Configuration for unnest operations, containing collection-specific values. */
  protected static class UnnestConfig {
    public final String transformedFieldName;
    public final String unnestFunction;
    public final String pgColumnName;

    public UnnestConfig(String transformedFieldName, String unnestFunction, String pgColumnName) {
      this.transformedFieldName = transformedFieldName;
      this.unnestFunction = unnestFunction;
      this.pgColumnName = pgColumnName;
    }
  }

  /**
   * Builds the complete FROM clause with CTEs.
   *
   * @param postgresQueryParser the query parser
   * @param visitor the visitor instance to use
   * @return complete FROM clause string
   */
  protected static String buildFromClause(
      PostgresQueryParser postgresQueryParser, PostgresFromTypeExpressionVisitorBase visitor) {

    // Build table0 query BEFORE processing unnest expressions
    String table0Query = visitor.buildTable0Query(postgresQueryParser);

    // Process unnest expressions
    String childList =
        postgresQueryParser.getQuery().getFromTypeExpressions().stream()
            .map(fromTypeExpression -> fromTypeExpression.accept(visitor))
            .map(Object::toString)
            .collect(Collectors.joining(",\n"));

    postgresQueryParser.setFinalTableName("table" + postgresQueryParser.getPgColumnNames().size());
    return String.format(QUERY_FMT, table0Query, childList);
  }
}
