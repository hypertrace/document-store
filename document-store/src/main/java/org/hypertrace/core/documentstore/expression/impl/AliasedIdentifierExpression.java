package org.hypertrace.core.documentstore.expression.impl;

import com.google.common.base.Preconditions;
import lombok.Value;

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

  public static AliasedIdentifierExpression of(final String name, final String alias) {
    Preconditions.checkArgument(name != null && !name.isBlank(), "name is null or blank");
    Preconditions.checkArgument(alias != null && !alias.isBlank(), "alias is null or blank");
    return new AliasedIdentifierExpression(name, alias);
  }

  @Override
  public String toString() {
    return "`" + getName() + "` AS `" + getAlias() + "`";
  }
}
