package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Expression for referencing an identifier/column name within a context having an alias.
 *
 * <p>Example: In this query: <code>
 * SELECT item, quantity, date FROM <implicit_collection> JOIN ( SELECT item, MAX(date) AS
 * latest_date FROM <implicit_collection> GROUP BY item ) AS latest ON item = latest.item ORDER BY
 * `item` ASC;
 * </code> the rhs of the join condition "latest.item" can be expressed as: <code>
 * AliasedIdentifierExpression.builder().name("item").alias("alias1").build() </code>
 */
@EqualsAndHashCode(callSuper = true)
@Value
public class AliasedIdentifierExpression extends IdentifierExpression {

  String contextAlias;

  private AliasedIdentifierExpression(final String name, final String contextAlias) {
    super(name, null);
    this.contextAlias = contextAlias;
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "`" + getContextAlias() + "." + getName() + "`";
  }

  public static AliasedIdentifierExpressionBuilder builder() {
    return new AliasedIdentifierExpressionBuilder();
  }

  public static class AliasedIdentifierExpressionBuilder {

    private String name;
    private String contextAlias;

    public AliasedIdentifierExpressionBuilder name(final String name) {
      this.name = name;
      return this;
    }

    public AliasedIdentifierExpressionBuilder contextAlias(final String contextAlias) {
      this.contextAlias = contextAlias;
      return this;
    }

    public AliasedIdentifierExpression build() {
      Preconditions.checkArgument(
          this.name != null && !this.name.isBlank(), "name is null or blank");
      Preconditions.checkArgument(
          this.contextAlias != null && !this.contextAlias.isBlank(),
          "contextAlias is null or blank");
      return new AliasedIdentifierExpression(this.name, this.contextAlias);
    }
  }
}
