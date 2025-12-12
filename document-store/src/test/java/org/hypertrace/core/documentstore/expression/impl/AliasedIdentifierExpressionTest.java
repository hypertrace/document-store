package org.hypertrace.core.documentstore.expression.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class AliasedIdentifierExpressionTest {

  @Test
  void testBuilderCreatesInstance() {
    AliasedIdentifierExpression expression =
        AliasedIdentifierExpression.builder().name("column1").contextAlias("alias1").build();

    assertEquals("column1", expression.getName());
    assertEquals("alias1", expression.getContextAlias());
  }

  @Test
  void testBuilderThrowsExceptionForNullName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AliasedIdentifierExpression.builder().contextAlias("alias1").build());
  }

  @Test
  void testBuilderThrowsExceptionForBlankName() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AliasedIdentifierExpression.builder().name("").contextAlias("alias1").build());
  }

  @Test
  void testBuilderThrowsExceptionForNullContextAlias() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AliasedIdentifierExpression.builder().name("column1").build());
  }

  @Test
  void testBuilderThrowsExceptionForBlankContextAlias() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AliasedIdentifierExpression.builder().name("column1").contextAlias("").build());
  }

  @Test
  void testEqualsWithSameNameAndContextAlias() {
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();

    assertEquals(expr1, expr2);
    assertEquals(expr1.hashCode(), expr2.hashCode());
  }

  @Test
  void testNotEqualsWithDifferentContextAlias() {
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("oldest").build();

    assertNotEquals(expr1, expr2);
  }

  @Test
  void testNotEqualsWithDifferentName() {
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("product").contextAlias("latest").build();

    assertNotEquals(expr1, expr2);
  }

  @Test
  void testEqualsAndHashCodeVerifiesCallSuperTrue() {
    // This tests that @EqualsAndHashCode(callSuper = true) is working correctly
    // by ensuring that the superclass field 'name' is considered in equality
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("column1").contextAlias("alias1").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("column1").contextAlias("alias1").build();
    AliasedIdentifierExpression expr3 =
        AliasedIdentifierExpression.builder().name("column2").contextAlias("alias1").build();

    // Same name and alias - should be equal
    assertEquals(expr1, expr2);
    assertEquals(expr1.hashCode(), expr2.hashCode());

    // Different name but same alias - should NOT be equal (proves callSuper = true)
    assertNotEquals(expr1, expr3);
  }

  @Test
  void testNotEqualsWithIdentifierExpression() {
    // Even if name is the same, AliasedIdentifierExpression should not equal IdentifierExpression
    AliasedIdentifierExpression aliasedExpr =
        AliasedIdentifierExpression.builder().name("column").contextAlias("alias1").build();
    IdentifierExpression identExpr = IdentifierExpression.of("column");

    assertNotEquals(aliasedExpr, identExpr);
    assertNotEquals(identExpr, aliasedExpr);
  }

  @Test
  void testHashCodeConsistencyAcrossMultipleInstances() {
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();
    AliasedIdentifierExpression expr3 =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();

    // Verify transitivity
    assertEquals(expr1, expr2);
    assertEquals(expr2, expr3);
    assertEquals(expr1, expr3);

    // Verify hashCode consistency
    assertEquals(expr1.hashCode(), expr2.hashCode());
    assertEquals(expr2.hashCode(), expr3.hashCode());
    assertEquals(expr1.hashCode(), expr3.hashCode());
  }

  @Test
  void testEqualsReflexive() {
    AliasedIdentifierExpression expr =
        AliasedIdentifierExpression.builder().name("column").contextAlias("alias").build();

    assertEquals(expr, expr);
  }

  @Test
  void testEqualsSymmetric() {
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("column").contextAlias("alias").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("column").contextAlias("alias").build();

    assertEquals(expr1, expr2);
    assertEquals(expr2, expr1);
  }

  @Test
  void testToStringFormat() {
    AliasedIdentifierExpression expression =
        AliasedIdentifierExpression.builder().name("item").contextAlias("latest").build();

    assertEquals("`latest.item`", expression.toString());
  }

  @Test
  void testCallSuperTrueConsidersSuperclassFields() {
    // Create two instances with:
    // - Same contextAlias (own field)
    // - Different name (superclass field)
    AliasedIdentifierExpression expr1 =
        AliasedIdentifierExpression.builder().name("field1").contextAlias("ctx").build();
    AliasedIdentifierExpression expr2 =
        AliasedIdentifierExpression.builder().name("field2").contextAlias("ctx").build();

    // They should NOT be equal because the superclass field 'name' is different
    // This proves @EqualsAndHashCode(callSuper = true) is working
    assertNotEquals(
        expr1,
        expr2,
        "Instances with same contextAlias but different superclass 'name' should not be equal");

    // HashCodes should also be different (though not guaranteed by contract, usually are)
    assertNotEquals(
        expr1.hashCode(),
        expr2.hashCode(),
        "Instances with different superclass fields should have different hashCodes");
  }
}
