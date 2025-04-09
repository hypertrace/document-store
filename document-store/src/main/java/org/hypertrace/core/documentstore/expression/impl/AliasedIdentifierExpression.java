package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.Value;
import org.hypertrace.core.documentstore.parser.SelectTypeExpressionVisitor;

/**
 * Expression representing an identifier/column name with an alias
 *
 * <p>Example: AliasedIdentifierExpression.of("col1", "col1_alias");
 */
@Value
public class AliasedIdentifierExpression extends IdentifierExpression {
  String alias;

  private AliasedIdentifierExpression(final String name, final String alias) {
    super(name);
    this.alias = alias;
  }

  @Override
  public <T> T accept(final SelectTypeExpressionVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public String toString() {
    return "`" + getAlias() + "." + getName() + "`";
  }

  public static AliasedIdentifierExpressionBuilder builder() {
    return new AliasedIdentifierExpressionBuilder();
  }

  public static class AliasedIdentifierExpressionBuilder {
    private String name;
    private String alias;

    public AliasedIdentifierExpressionBuilder name(final String name) {
      this.name = name;
      return this;
    }

    public AliasedIdentifierExpressionBuilder alias(final String alias) {
      this.alias = alias;
      return this;
    }

    public AliasedIdentifierExpression build() {
      Preconditions.checkArgument(
          this.name != null && !this.name.isBlank(), "name is null or blank");
      Preconditions.checkArgument(
          this.alias != null && !this.alias.isBlank(), "alias is null or blank");
      return new AliasedIdentifierExpression(this.name, this.alias);
    }
  }
}
