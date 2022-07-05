package org.hypertrace.core.documentstore.postgres.query.v1;

import static org.hypertrace.core.documentstore.expression.operators.RelationalOperator.NEQ;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.postgres.Params;
import org.hypertrace.core.documentstore.query.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PostgresQueryParserTest {
  private static final String TEST_COLLECTION = "testCollection";

  @Test
  void testParseSimpleFilter() {
    Query query =
        Query.builder()
            .setFilter(
                RelationalExpression.of(
                    IdentifierExpression.of("quantity"), NEQ, ConstantExpression.of(10)))
            .build();
    PostgresQueryParser postgresQueryParser = new PostgresQueryParser(TEST_COLLECTION);
    String sql = postgresQueryParser.parse(query);
    Assertions.assertEquals(
        "SELECT * FROM testCollection "
            + "WHERE document->'quantity' IS NULL OR CAST (document->>'quantity' AS NUMERIC) != ?",
        sql);

    Params params = postgresQueryParser.getParamsBuilder().build();
    Assertions.assertEquals(10, params.getObjectParams().get(1));
  }
}
