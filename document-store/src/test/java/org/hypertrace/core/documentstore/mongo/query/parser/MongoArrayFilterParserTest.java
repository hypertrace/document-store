package org.hypertrace.core.documentstore.mongo.query.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.hypertrace.core.documentstore.expression.impl.ArrayRelationalFilterExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.ArrayOperator;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.junit.jupiter.api.Test;

class MongoArrayFilterParserTest {

  private final MongoFilterTypeExpressionParser parser = new MongoFilterTypeExpressionParser();

  @Test
  void testAllOperator() {
    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ALL)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.IN,
                    ConstantExpression.ofStrings(List.of("Blue", "Green"))))
            .build();

    final Map<String, Object> result = parser.visit(expression);

    // {"$expr": {"$setIsSubset": [["Blue", "Green"], {"$ifNull": ["$tags", []]}]}}
    final Map<String, Object> expr = getMap(result, "$expr");
    final List<Object> setIsSubset = getList(expr, "$setIsSubset");
    assertEquals(2, setIsSubset.size());
    assertEquals(List.of("Blue", "Green"), setIsSubset.get(0));

    final Map<String, Object> ifNull = castToMap(setIsSubset.get(1));
    final Object[] ifNullArgs = (Object[]) ifNull.get("$ifNull");
    assertEquals("$tags", ifNullArgs[0]);
    assertEquals(0, ((Object[]) ifNullArgs[1]).length);
  }

  @Test
  void testOneOperator() {
    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ONE)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.IN,
                    ConstantExpression.ofStrings(List.of("Blue", "Green"))))
            .build();

    final Map<String, Object> result = parser.visit(expression);

    /*
    {"$expr": {"$and": [
      {"$eq": [{"$size": {"$ifNull": ["$tags", []]}}, 1]},
      {"$in": [{"$arrayElemAt": [{"$ifNull": ["$tags", []]}, 0]}, ["Blue", "Green"]]}
    ]}}
     */
    final Map<String, Object> expr = getMap(result, "$expr");
    final List<Object> and = getList(expr, "$and");
    assertEquals(2, and.size());

    final List<Object> eq = getList(castToMap(and.get(0)), "$eq");
    final Map<String, Object> size = castToMap(eq.get(0));
    final Map<String, Object> sizeIfNull = castToMap(size.get("$size"));
    final Object[] sizeIfNullArgs = (Object[]) sizeIfNull.get("$ifNull");
    assertEquals("$tags", sizeIfNullArgs[0]);
    assertEquals(1, eq.get(1));

    final List<Object> in = getList(castToMap(and.get(1)), "$in");
    final Map<String, Object> arrayElemAt = castToMap(in.get(0));
    final List<Object> arrayElemAtArgs = getList(arrayElemAt, "$arrayElemAt");
    final Map<String, Object> elemIfNull = castToMap(arrayElemAtArgs.get(0));
    final Object[] elemIfNullArgs = (Object[]) elemIfNull.get("$ifNull");
    assertEquals("$tags", elemIfNullArgs[0]);
    assertEquals(0, arrayElemAtArgs.get(1));
    assertEquals(List.of("Blue", "Green"), in.get(1));
  }

  @Test
  void testAllOperatorWithSingleValue() {
    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ALL)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.EQ,
                    ConstantExpression.of("Blue")))
            .build();

    final Map<String, Object> result = parser.visit(expression);

    final Map<String, Object> expr = getMap(result, "$expr");
    final List<Object> setIsSubset = getList(expr, "$setIsSubset");
    assertEquals(List.of("Blue"), setIsSubset.get(0));
  }

  @Test
  void testAnyOperatorStillSupported() {
    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ANY)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.IN,
                    ConstantExpression.ofStrings(List.of("Blue", "Green"))))
            .build();

    // ANY remains supported
    final Map<String, Object> result = parser.visit(expression);
    assertTrue(result.containsKey("$expr"));
  }

  @Test
  void testAllOperatorInsideExprLocation() {
    final MongoFilterTypeExpressionParser insideExprParser =
        new MongoFilterTypeExpressionParser(
            org.hypertrace.core.documentstore.mongo.query.parser.filter
                .MongoRelationalFilterParserFactory.MongoRelationalFilterContext.builder()
                .location(
                    org.hypertrace.core.documentstore.mongo.query.parser.filter
                        .MongoRelationalFilterParserFactory.FilterLocation.INSIDE_EXPR)
                .build());

    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ALL)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.IN,
                    ConstantExpression.ofStrings(List.of("Blue"))))
            .build();

    final Map<String, Object> result = insideExprParser.visit(expression);

    // already inside $expr, so no additional wrapping
    assertTrue(result.containsKey("$setIsSubset"));
  }

  @Test
  void testOneOperatorRejectsNonConstantRhs() {
    final ArrayRelationalFilterExpression expression =
        ArrayRelationalFilterExpression.builder()
            .operator(ArrayOperator.ONE)
            .filter(
                RelationalExpression.of(
                    IdentifierExpression.of("tags"),
                    RelationalOperator.EQ,
                    IdentifierExpression.of("otherField")))
            .build();

    assertThrows(UnsupportedOperationException.class, () -> parser.visit(expression));
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> castToMap(final Object object) {
    return (Map<String, Object>) object;
  }

  private Map<String, Object> getMap(final Map<String, Object> map, final String key) {
    return castToMap(map.get(key));
  }

  @SuppressWarnings("unchecked")
  private List<Object> getList(final Map<String, Object> map, final String key) {
    return (List<Object>) map.get(key);
  }
}
